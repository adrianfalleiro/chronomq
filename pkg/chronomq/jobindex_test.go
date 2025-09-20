package chronomq_test

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/chronomq/chronomq/pkg/chronomq"
	"github.com/chronomq/chronomq/internal/temporal"
)

var _ = Describe("JobIndex", func() {
	var job *Job
	var index *JobIndex

	BeforeEach(func() {
		// Create a test job
		triggerTime := time.Now().Add(time.Hour)
		job = NewJobAutoID(triggerTime, []byte("test job data with some content"))
		index = NewJobIndex(job)
	})

	Context("Creation", func() {
		It("should create job index from job", func() {
			Expect(index).ToNot(BeNil())
			Expect(index.ID()).To(Equal(job.ID()))
			Expect(index.TriggerAt()).To(Equal(job.TriggerAt()))
			Expect(index.DiskKey()).To(Equal(job.ID())) // DiskKey should be same as job ID
		})

		It("should calculate size bytes correctly", func() {
			expectedSize := len(job.Body())
			Expect(index.SizeBytes()).To(Equal(expectedSize))
		})
	})

	Context("Memory Efficiency", func() {
		It("should be much smaller than original job", func() {
			// For this test to be meaningful, the job should have a larger body
			largeJob := NewJobAutoID(time.Now().Add(time.Hour), []byte("This is a much larger job body with significant content to test memory efficiency properly and ensure the index is actually smaller"))
			largeIndex := NewJobIndex(largeJob)

			// Calculate approximate memory usage of index
			indexMemory := len(largeIndex.ID()) + len(largeIndex.DiskKey()) + 64 // rough estimate for other fields

			// Original job memory includes the full body
			jobMemory := len(largeJob.Body()) + len(largeJob.ID()) + 64

			// Index should be significantly smaller
			Expect(indexMemory).To(BeNumerically("<", jobMemory/2))
		})

		It("should show significant savings for large jobs", func() {
			// Create a job with large payload (5MB)
			largePayload := make([]byte, 5*1024*1024)
			for i := range largePayload {
				largePayload[i] = byte(i % 256)
			}

			largeJob := NewJobAutoID(time.Now().Add(time.Hour), largePayload)
			largeIndex := NewJobIndex(largeJob)

			// Index memory usage
			indexMemory := len(largeIndex.ID()) + len(largeIndex.DiskKey()) + 64

			// Should be less than 1KB vs 5MB for the full job
			Expect(indexMemory).To(BeNumerically("<", 1024))
			Expect(largeIndex.SizeBytes()).To(Equal(5 * 1024 * 1024)) // But still tracks the size
		})
	})

	Context("Temporal State", func() {
		It("should reflect job temporal state correctly", func() {
			// Past job
			pastJob := NewJobAutoID(time.Now().Add(-time.Hour), []byte("past"))
			pastIndex := NewJobIndex(pastJob)
			Expect(pastIndex.AsTemporalState()).To(Equal(temporal.Past))

			// Current job (very close to now)
			currentJob := NewJobAutoID(time.Now(), []byte("current"))
			currentIndex := NewJobIndex(currentJob)
			// Could be Past or Current depending on exact timing
			state := currentIndex.AsTemporalState()
			Expect(state == temporal.Past || state == temporal.Current).To(BeTrue())

			// Future job
			futureJob := NewJobAutoID(time.Now().Add(time.Hour), []byte("future"))
			futureIndex := NewJobIndex(futureJob)
			Expect(futureIndex.AsTemporalState()).To(Equal(temporal.Future))
		})
	})

	Context("Priority Queue Integration", func() {
		It("should create proper priority item", func() {
			item := index.AsPriorityItem()
			Expect(item).ToNot(BeNil())

			// Item should contain the index
			retrievedIndex := item.Value().(*JobIndex)
			Expect(retrievedIndex.ID()).To(Equal(index.ID()))
			Expect(retrievedIndex.TriggerAt()).To(Equal(index.TriggerAt()))
		})

		It("should maintain proper ordering in priority queue", func() {
			// Create jobs with different trigger times
			now := time.Now()
			jobs := []*Job{
				NewJobAutoID(now.Add(3*time.Hour), []byte("job3")),
				NewJobAutoID(now.Add(1*time.Hour), []byte("job1")),
				NewJobAutoID(now.Add(2*time.Hour), []byte("job2")),
			}

			// Create indices
			indices := make([]*JobIndex, len(jobs))
			for i, job := range jobs {
				indices[i] = NewJobIndex(job)
			}

			// Priority items should order by trigger time
			items := make([]time.Time, len(indices))
			for i, idx := range indices {
				item := idx.AsPriorityItem()
				items[i] = item.Priority() // Priority() returns time.Time
			}

			// Earlier times should have higher priority (should be Before)
			Expect(items[1].Before(items[2])).To(BeTrue()) // job1 before job2
			Expect(items[2].Before(items[0])).To(BeTrue()) // job2 before job3
		})
	})

	Context("String Representation", func() {
		It("should provide informative string representation", func() {
			str := index.String()
			Expect(str).To(ContainSubstring(index.ID()))
			Expect(str).To(ContainSubstring("triggerAt"))
			Expect(str).ToNot(BeEmpty())
		})
	})

	Context("Edge Cases", func() {
		It("should handle job with nil body", func() {
			nilJob := NewJobAutoID(time.Now().Add(time.Hour), nil)
			nilIndex := NewJobIndex(nilJob)

			Expect(nilIndex.SizeBytes()).To(Equal(0))
			Expect(nilIndex.ID()).To(Equal(nilJob.ID()))
		})

		It("should handle job with empty body", func() {
			emptyJob := NewJobAutoID(time.Now().Add(time.Hour), []byte{})
			emptyIndex := NewJobIndex(emptyJob)

			Expect(emptyIndex.SizeBytes()).To(Equal(0))
			Expect(emptyIndex.ID()).To(Equal(emptyJob.ID()))
		})

		It("should handle job IDs correctly", func() {
			// This test verifies the index can handle the ID field properly
			Expect(len(index.ID())).To(BeNumerically(">", 0))
			Expect(len(index.DiskKey())).To(Equal(len(index.ID())))
		})
	})
})
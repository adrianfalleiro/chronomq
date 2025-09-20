package chronomq_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/chronomq/chronomq/pkg/chronomq"
	"github.com/chronomq/chronomq/pkg/persistence"
)

var _ = Describe("Indexed Storage", func() {
	var tempDir string
	var diskStore *persistence.DiskJobStore
	var hub *Hub

	BeforeEach(func() {
		// Create temporary directory for each test
		var err error
		tempDir, err = ioutil.TempDir("", "indexed-storage-test-")
		Expect(err).To(BeNil())
		diskStore = persistence.NewDiskJobStore(tempDir)

		// Create hub with indexed storage
		opts := &HubOpts{
			SpokeSpan:      time.Second * 10,
			DiskStore:      diskStore,
			AttemptRestore: false,
		}
		hub = NewHub(opts)
	})

	AfterEach(func() {
		if hub != nil {
			hub.Stop(false)
		}
		if diskStore != nil {
			diskStore.Close()
		}
		if tempDir != "" {
			os.RemoveAll(tempDir)
		}
	})

	Context("Memory Efficiency", func() {
		It("should store only lightweight indices in memory", func() {
			// Create large jobs (5MB each)
			largePayload := make([]byte, 5*1024*1024) // 5MB
			for i := range largePayload {
				largePayload[i] = byte(i % 256)
			}

			// Add 10 jobs with large payloads
			jobs := make([]*Job, 10)
			for i := 0; i < 10; i++ {
				jobs[i] = NewJobAutoID(time.Now().Add(time.Hour*time.Duration(i+1)), largePayload)
				err := hub.AddJobLocked(jobs[i])
				Expect(err).To(BeNil())
			}

			// Check that jobs are added
			Expect(hub.Stats().CurrentJobs).To(Equal(int64(10)))

			// Get memory footprint - should be much smaller than 50MB (10 * 5MB)
			totalMemory := 0
			for idx := range hub.GetNJobIndices(10) {
				// Each index should be small
				indexMemory := len(idx.ID()) + len(idx.DiskKey()) + 64 // rough estimate
				totalMemory += indexMemory
			}

			// Memory should be less than 10KB vs 50MB for full jobs
			Expect(totalMemory).To(BeNumerically("<", 10000))

			// Verify jobs are stored on disk
			var jobFileCount int
			err := filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if filepath.Ext(path) == ".job" {
					jobFileCount++
				}
				return nil
			})
			Expect(err).To(BeNil())
			Expect(jobFileCount).To(Equal(10))
		})
	})

	Context("Lazy Loading", func() {
		It("should load jobs from disk only when needed", func() {
			// Create job with specific payload
			testPayload := []byte("test job payload data")
			job := NewJobAutoID(time.Now().Add(-time.Second), testPayload) // Past job
			err := hub.AddJobLocked(job)
			Expect(err).To(BeNil())

			// Job should be immediately available since it's in the past
			retrievedJob, err := hub.NextLocked()
			Expect(err).To(BeNil())
			Expect(retrievedJob).ToNot(BeNil())
			Expect(retrievedJob.ID()).To(Equal(job.ID()))
			Expect(retrievedJob.Body()).To(Equal(testPayload))

			// Job should be removed from hub after retrieval
			Expect(hub.Stats().CurrentJobs).To(Equal(int64(0)))
		})

		It("should handle disk I/O errors gracefully", func() {
			// Create job
			job := NewJobAutoID(time.Now().Add(-time.Second), []byte("test"))
			err := hub.AddJobLocked(job)
			Expect(err).To(BeNil())

			// Instead of trying to break the disk store, let's test that
			// the system handles missing files gracefully by manually deleting the job file
			var jobFileCount int
			var jobFilePath string
			err = filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if filepath.Ext(path) == ".job" {
					jobFileCount++
					jobFilePath = path
				}
				return nil
			})
			Expect(err).To(BeNil())
			Expect(jobFileCount).To(Equal(1))

			// Remove the job file to simulate disk corruption/deletion
			err = os.Remove(jobFilePath)
			Expect(err).To(BeNil())

			// NextLocked should return an error when it can't load from disk
			retrievedJob, err := hub.NextLocked()
			Expect(err).To(HaveOccurred())
			Expect(retrievedJob).To(BeNil())
		})
	})

	Context("Index Rebuilding", func() {
		It("should rebuild indices from disk on startup", func() {
			// Add jobs to the first hub
			jobs := make([]*Job, 5)
			for i := 0; i < 5; i++ {
				jobs[i] = NewJobAutoID(time.Now().Add(time.Hour*time.Duration(i+1)), []byte("test data"))
				err := hub.AddJobLocked(jobs[i])
				Expect(err).To(BeNil())
			}

			Expect(hub.Stats().CurrentJobs).To(Equal(int64(5)))

			// Stop the first hub
			hub.Stop(false)
			hub = nil

			// Create new hub with restore flag
			opts := &HubOpts{
				SpokeSpan:      time.Second * 10,
				DiskStore:      diskStore,
				AttemptRestore: true,
			}
			hub = NewHub(opts)

			// Give time for async rebuild
			time.Sleep(200 * time.Millisecond)

			// Should have rebuilt all indices
			Expect(hub.Stats().CurrentJobs).To(Equal(int64(5)))

			// Verify we can get job indices
			indexCount := 0
			for range hub.GetNJobIndices(10) {
				indexCount++
			}
			Expect(indexCount).To(Equal(5))
		})
	})

	Context("Date-based Directory Structure", func() {
		It("should organize job files by date hierarchy", func() {
			// Create jobs with specific trigger times
			baseTime := time.Date(2025, 1, 15, 14, 30, 0, 0, time.UTC)

			// Jobs on different dates
			jobs := []*Job{
				NewJobAutoID(baseTime, []byte("job1")),                           // 2025/01/15/14/
				NewJobAutoID(baseTime.Add(time.Hour*25), []byte("job2")),         // 2025/01/16/15/
				NewJobAutoID(baseTime.Add(time.Hour*24*32), []byte("job3")),      // 2025/02/16/14/
			}

			for _, job := range jobs {
				err := hub.AddJobLocked(job)
				Expect(err).To(BeNil())
			}

			// Check directory structure
			expectedPaths := []string{
				filepath.Join(tempDir, "2025", "01", "15", "14"),
				filepath.Join(tempDir, "2025", "01", "16", "15"),
				filepath.Join(tempDir, "2025", "02", "16", "14"),
			}

			for _, expectedPath := range expectedPaths {
				_, err := os.Stat(expectedPath)
				Expect(err).To(BeNil(), "Expected directory %s to exist", expectedPath)
			}
		})
	})

	Context("Job Cancellation", func() {
		It("should remove job from both memory index and disk", func() {
			// Add job
			job := NewJobAutoID(time.Now().Add(time.Hour), []byte("test data"))
			err := hub.AddJobLocked(job)
			Expect(err).To(BeNil())

			// Verify job exists
			Expect(hub.Stats().CurrentJobs).To(Equal(int64(1)))

			// Cancel job (with body to verify the job data)
			canceledJob, err := hub.CancelJobWithBodyLocked(job.ID())
			Expect(err).To(BeNil())
			Expect(canceledJob).ToNot(BeNil())
			Expect(canceledJob.ID()).To(Equal(job.ID()))

			// Job should be removed from hub
			Expect(hub.Stats().CurrentJobs).To(Equal(int64(0)))

			// Job file should be deleted from disk
			var jobFileCount int
			err = filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if filepath.Ext(path) == ".job" {
					jobFileCount++
				}
				return nil
			})
			Expect(err).To(BeNil())
			Expect(jobFileCount).To(Equal(0))
		})
	})

	Context("Performance", func() {
		It("should handle many jobs efficiently", func() {
			// Add 1000 jobs
			start := time.Now()
			for i := 0; i < 1000; i++ {
				job := NewJobAutoID(time.Now().Add(time.Duration(i)*time.Second), []byte("test"))
				err := hub.AddJobLocked(job)
				Expect(err).To(BeNil())
			}
			addDuration := time.Since(start)

			Expect(hub.Stats().CurrentJobs).To(Equal(int64(1000)))

			// Adding 1000 jobs should be reasonably fast (< 1 second)
			Expect(addDuration).To(BeNumerically("<", time.Second))

			// Memory footprint should be small relative to job count
			// (Much less than if we stored full jobs in memory)
			totalIndexMemory := 0
			for idx := range hub.GetNJobIndices(1000) {
				totalIndexMemory += len(idx.ID()) + len(idx.DiskKey()) + 64
			}

			// Should be less than 1MB for 1000 job indices
			Expect(totalIndexMemory).To(BeNumerically("<", 1024*1024))
		})
	})
})
package persistence_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/chronomq/chronomq/pkg/chronomq"
	"github.com/chronomq/chronomq/pkg/persistence"
)

var _ = Describe("SimpleDiskJobStore", func() {
	var tempDir string
	var store persistence.DiskJobStore

	BeforeEach(func() {
		var err error
		tempDir, err = ioutil.TempDir("", "diskstore-test-")
		Expect(err).To(BeNil())
		store = persistence.NewSimpleDiskJobStore(tempDir)
	})

	AfterEach(func() {
		if store != nil {
			store.Close()
		}
		if tempDir != "" {
			os.RemoveAll(tempDir)
		}
	})

	Context("Basic Operations", func() {
		It("should store and retrieve jobs", func() {
			// Create a job
			job := NewJobAutoID(time.Now().Add(time.Hour), []byte("test job data"))

			// Store the job
			key, err := store.StoreJob(job)
			Expect(err).To(BeNil())
			Expect(key).To(Equal(job.ID()))

			// Retrieve the job
			data, err := store.RetrieveJob(key)
			Expect(err).To(BeNil())
			Expect(data).ToNot(BeNil())

			// Decode and verify
			jobData, ok := data.([]byte)
			Expect(ok).To(BeTrue())

			retrievedJob := &Job{}
			err = retrievedJob.GobDecode(jobData)
			Expect(err).To(BeNil())
			Expect(retrievedJob.ID()).To(Equal(job.ID()))
			Expect(retrievedJob.Body()).To(Equal([]byte("test job data")))
		})

		It("should delete jobs", func() {
			// Store a job
			job := NewJobAutoID(time.Now().Add(time.Hour), []byte("test job data"))
			key, err := store.StoreJob(job)
			Expect(err).To(BeNil())

			// Verify it exists
			_, err = store.RetrieveJob(key)
			Expect(err).To(BeNil())

			// Delete it
			err = store.DeleteJob(key)
			Expect(err).To(BeNil())

			// Should not be retrievable
			_, err = store.RetrieveJob(key)
			Expect(err).To(HaveOccurred())
		})

		It("should create date-based directory structure", func() {
			// Create job with specific trigger time
			triggerTime := time.Date(2025, 1, 15, 14, 30, 0, 0, time.UTC)
			job := NewJobAutoID(triggerTime, []byte("test"))

			// Store the job
			_, err := store.StoreJob(job)
			Expect(err).To(BeNil())

			// Check that directory structure was created
			expectedDir := filepath.Join(tempDir, "2025", "01", "15", "14")
			_, err = os.Stat(expectedDir)
			Expect(err).To(BeNil())

			// Check that job file exists
			jobFile := filepath.Join(expectedDir, job.ID()+".job")
			_, err = os.Stat(jobFile)
			Expect(err).To(BeNil())
		})
	})

	Context("Walking Job Files", func() {
		It("should walk through all job files", func() {
			// Store multiple jobs
			jobs := make([]*Job, 5)
			for i := 0; i < 5; i++ {
				jobs[i] = NewJobAutoID(time.Now().Add(time.Duration(i)*time.Hour), []byte("test"))
				_, err := store.StoreJob(jobs[i])
				Expect(err).To(BeNil())
			}

			// Walk through job files
			foundKeys := make([]string, 0)
			walkStore, ok := store.(interface {
				WalkJobFiles(func(string) error) error
			})
			Expect(ok).To(BeTrue())

			err := walkStore.WalkJobFiles(func(key string) error {
				foundKeys = append(foundKeys, key)
				return nil
			})
			Expect(err).To(BeNil())

			// Should have found all job keys
			Expect(len(foundKeys)).To(Equal(5))

			// Verify all job IDs are present
			for _, job := range jobs {
				Expect(foundKeys).To(ContainElement(job.ID()))
			}
		})
	})

	Context("Error Handling", func() {
		It("should handle non-existent jobs gracefully", func() {
			// Try to retrieve non-existent job
			_, err := store.RetrieveJob("non-existent-job-id")
			Expect(err).To(HaveOccurred())
		})

		It("should handle deletion of non-existent jobs gracefully", func() {
			// Try to delete non-existent job
			err := store.DeleteJob("non-existent-job-id")
			Expect(err).To(BeNil()) // Should not error - it's idempotent (deleting non-existent is fine)
		})

		It("should handle invalid job data gracefully", func() {
			// This test verifies the store can handle edge cases
			// Create an invalid job (nil body should still work)
			job := NewJobAutoID(time.Now().Add(time.Hour), nil)

			_, err := store.StoreJob(job)
			Expect(err).To(BeNil()) // Should not error even with nil body
		})
	})

	Context("Concurrency", func() {
		It("should handle concurrent operations safely", func() {
			// Store and retrieve jobs concurrently
			done := make(chan bool, 10)

			// Start 10 goroutines storing jobs
			for i := 0; i < 10; i++ {
				go func(id int) {
					defer func() { done <- true }()

					job := NewJobAutoID(time.Now().Add(time.Duration(id)*time.Hour), []byte("concurrent test"))
					key, err := store.StoreJob(job)
					Expect(err).To(BeNil())

					// Immediately try to retrieve it
					_, err = store.RetrieveJob(key)
					Expect(err).To(BeNil())
				}(i)
			}

			// Wait for all goroutines to complete
			for i := 0; i < 10; i++ {
				<-done
			}
		})
	})
})

func TestSimpleDiskStore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SimpleDiskStore Suite")
}
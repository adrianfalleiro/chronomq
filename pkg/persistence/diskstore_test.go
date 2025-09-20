package persistence_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/chronomq/chronomq/pkg/chronomq"
	"github.com/chronomq/chronomq/pkg/persistence"
)

var _ = Describe("DiskJobStore", func() {
	var tempDir string
	var diskStore persistence.DiskJobStoreInterface

	BeforeEach(func() {
		var err error
		tempDir, err = ioutil.TempDir("", "diskstore-test-")
		Expect(err).To(BeNil())
		diskStore = persistence.NewDiskJobStore(tempDir)
	})

	AfterEach(func() {
		if diskStore != nil {
			diskStore.Close()
		}
		if tempDir != "" {
			os.RemoveAll(tempDir)
		}
	})

	Context("Basic Operations", func() {
		It("should diskStore and retrieve jobs", func() {
			// Create a job
			job := NewJobAutoID(time.Now().Add(time.Hour), []byte("test job data"))

			// Store the job
			key, err := diskStore.StoreJob(job)
			Expect(err).To(BeNil())
			// Key should be a file path, not just the job ID
			Expect(key).To(ContainSubstring(job.ID()))
			Expect(key).To(HaveSuffix(".job"))

			// Retrieve the job
			data, err := diskStore.RetrieveJob(key)
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
			key, err := diskStore.StoreJob(job)
			Expect(err).To(BeNil())

			// Verify it exists
			_, err = diskStore.RetrieveJob(key)
			Expect(err).To(BeNil())

			// Delete it
			err = diskStore.DeleteJob(key)
			Expect(err).To(BeNil())

			// Should not be retrievable
			_, err = diskStore.RetrieveJob(key)
			Expect(err).To(HaveOccurred())
		})

		It("should create date-based directory structure", func() {
			// Create job with specific trigger time
			triggerTime := time.Date(2025, 1, 15, 14, 30, 0, 0, time.UTC)
			job := NewJobAutoID(triggerTime, []byte("test"))

			// Store the job
			_, err := diskStore.StoreJob(job)
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
				_, err := diskStore.StoreJob(jobs[i])
				Expect(err).To(BeNil())
			}

			// Walk through job files
			foundKeys := make([]string, 0)
			walkStore, ok := diskStore.(interface {
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

			// Verify all job IDs are present (extract from file paths)
			for _, job := range jobs {
				found := false
				for _, key := range foundKeys {
					// Extract job ID from file path (filename without .job extension)
					filename := filepath.Base(key)
					if strings.HasSuffix(filename, ".job") {
						jobID := filename[:len(filename)-4]
						if jobID == job.ID() {
							found = true
							break
						}
					}
				}
				Expect(found).To(BeTrue(), "Job ID %s should be found in walked files", job.ID())
			}
		})
	})

	Context("Error Handling", func() {
		It("should handle non-existent jobs gracefully", func() {
			// Try to retrieve non-existent job
			_, err := diskStore.RetrieveJob("non-existent-job-id")
			Expect(err).To(HaveOccurred())
		})

		It("should handle deletion of non-existent jobs gracefully", func() {
			// Try to delete non-existent job
			err := diskStore.DeleteJob("non-existent-job-id")
			Expect(err).To(BeNil()) // Should not error - it's idempotent (deleting non-existent is fine)
		})

		It("should handle invalid job data gracefully", func() {
			// This test verifies the diskStore can handle edge cases
			// Create an invalid job (nil body should still work)
			job := NewJobAutoID(time.Now().Add(time.Hour), nil)

			_, err := diskStore.StoreJob(job)
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
					key, err := diskStore.StoreJob(job)
					Expect(err).To(BeNil())

					// Immediately try to retrieve it
					_, err = diskStore.RetrieveJob(key)
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
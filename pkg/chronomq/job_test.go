package chronomq_test

import (
	"container/heap"
	"os"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"

	. "github.com/chronomq/chronomq/internal/queue"
	. "github.com/chronomq/chronomq/pkg/chronomq"
	"github.com/chronomq/chronomq/pkg/persistence"
)

var _ = Describe("Test jobs", func() {
	Context("Basic job tests", func() {
		It("can create a job", func() {
			b := []byte("foo")
			j := NewJob(uuid.NewV4().String(), time.Now(), b)
			Expect(j.IsReady()).To(BeTrue())
		})

		It("can create spoke bounds from job trigger time", func() {
			t := time.Unix(0, 0)
			j := NewJobAutoID(t.Add(time.Second*15), nil)

			sb := j.AsBound(time.Second)

			Expect(sb.Start().Before(j.TriggerAt()))
			Expect(sb.End().After(j.TriggerAt()))
		})
	})

	Context("Job Ordering", func() {
		It("orders jobs correctly", func() {
			t := time.Now()
			jone := NewJobAutoID(t.Add(1), nil)
			jtwo := NewJobAutoID(t.Add(20), nil)
			jthree := NewJobAutoID(t.Add(50), nil)
			ordList := []*Job{jone, jtwo, jthree}

			jobs := &PriorityQueue{jtwo.AsPriorityItem(), jone.AsPriorityItem(), jthree.AsPriorityItem()}
			heap.Init(jobs)

			for _, job := range ordList {
				j := heap.Pop(jobs).(*Item).Value().(*Job)
				Expect(j.ID()).To(Equal(job.ID()))
			}
		})
	})

	Context("Job serialization", func() {
		It("serde as gob", func() {
			j := NewJobAutoID(time.Now(), []byte("This is a test job"))
			encoded, err := j.GobEncode()
			Expect(err).To(BeNil())

			jj := &Job{}
			err = jj.GobDecode(encoded)
			Expect(err).To(BeNil())

			Expect(j.ID()).To(Equal(jj.ID()))
			Expect(j.Body()).To(Equal(jj.Body()))
			Expect(j.TriggerAt().Unix()).To(Equal(jj.TriggerAt().Unix()))
		})

		It("use disk store to save a job", func() {
			j := NewJobAutoID(time.Now(), []byte("This is a test job"))
			tempDir := "/tmp/chronomq-job-test"
			os.RemoveAll(tempDir)
			os.MkdirAll(tempDir, 0755)
			defer os.RemoveAll(tempDir)

			diskStore := persistence.NewDiskJobStore(tempDir)
			defer diskStore.Close()

			key, err := diskStore.StoreJob(j)
			Expect(err).NotTo(HaveOccurred())
			Expect(key).NotTo(BeEmpty())

			// Verify we can retrieve the job
			retrievedData, err := diskStore.RetrieveJob(key)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedData).NotTo(BeNil())

			// Decode the job data
			data, ok := retrievedData.([]byte)
			Expect(ok).To(BeTrue())

			var job Job
			err = job.GobDecode(data)
			Expect(err).NotTo(HaveOccurred())
			Expect(job.ID()).To(Equal(j.ID()))
			Expect(job.Body()).To(Equal(j.Body()))
		})
	})
})

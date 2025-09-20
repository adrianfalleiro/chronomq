package chronomq_test

import (
	"container/heap"
	"io/ioutil"
	"math/rand"
	"os"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"

	. "github.com/chronomq/chronomq/internal/queue"
	. "github.com/chronomq/chronomq/pkg/chronomq"
	"github.com/chronomq/chronomq/pkg/persistence"
)

var _ = Describe("Test spokes", func() {
	var tempDir string
	var diskStore *persistence.DiskJobStore

	BeforeEach(func() {
		// Create temporary directory for each test
		var err error
		tempDir, err = ioutil.TempDir("", "spoke-test-")
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

	Context("Basic spoke tests", func() {
		It("can create a spoke", func() {
			t := time.Now()

			// Starts in an hour, ends in 2 hours
			s := NewSpoke(t.Add(time.Hour*1), t.Add(time.Hour*2), diskStore)
			Expect(s.IsStarted()).To(BeFalse())
			Expect(s.IsExpired()).To(BeFalse())

			// Starts now, ends in 10 hours
			s = NewSpoke(time.Now(), time.Now().Add(time.Hour*10), diskStore)
			Expect(s.IsStarted()).To(BeTrue())
			Expect(s.IsExpired()).To(BeFalse())

			// Ends in the past for the next tick
			s = NewSpoke(time.Now().Add(-time.Hour), time.Now(), diskStore)
			Expect(s.IsStarted()).To(BeTrue())
			Expect(s.IsExpired()).To(BeTrue())
		})

		It("accepts jobs into spoke", func() {
			s := NewSpoke(time.Now(), time.Now().Add(time.Hour), diskStore)
			j := NewJobAutoID(s.Start().Add(time.Second*20), nil)

			// Accepts the job and returns nil
			Expect(s.IsJobInBounds(j)).To(BeTrue())
			Expect(s.AddJobLocked(j)).To(BeNil())
			Expect(s.PendingJobsLen()).To(Equal(1))
			Expect(s.OwnsJobLocked(j.ID())).To(BeTrue())
		})

		It("rejects jobs from spoke that lie outside its time bounds", func() {
			now := time.Now()
			s := NewSpoke(now.Add(time.Hour*1), now.Add(time.Hour*2), diskStore)

			// Job triggers after spoke ends
			j := NewJobAutoID(s.End().Add(time.Hour*1), nil)

			// Rejects the job and returns it
			Expect(s.AddJobLocked(j)).To(Not(BeNil()))
			Expect(s.PendingJobsLen()).To(Equal(0))

			// Job triggers before spoke ends
			j = NewJobAutoID(s.Start().Add(-10*time.Minute), nil)

			// Rejects the job and returns it
			Expect(s.AddJobLocked(j)).To(Not(BeNil()))
			Expect(s.PendingJobsLen()).To(Equal(0))
		})

		It("walks spoke with jobs", func() {
			s := NewSpoke(time.Now(), time.Now().Add(time.Hour), diskStore)

			for i := 0; i < 10; i++ {
				j := NewJobAutoID(s.Start().Add(time.Nanosecond*time.Duration(rand.Intn(900))), nil)
				Expect(s.AddJobLocked(j)).To(BeNil())
			}
			Expect(s.PendingJobsLen()).To(Equal(10))

			// Wait for all jobs to be ready
			time.Sleep(time.Second * 1)

			jobs := []*Job{}
			for s.PendingJobsLen() > 0 {
				job, err := s.NextLocked()
				Expect(err).To(BeNil())
				if job != nil {
					jobs = append(jobs, job)
				} else {
					break
				}
			}
			Expect(len(jobs)).To(Equal(10))
			prev := jobs[0]
			for i := 1; i < len(jobs); i++ {
				// Walk returns jobs in order
				Expect(prev.TriggerAt().Sub(jobs[i].TriggerAt()) <= 0).To(BeTrue())
			}
		})

		It("repeated walks spoke with jobs as they expire", func() {
			s := NewSpoke(time.Now(), time.Now().Add(time.Hour), diskStore)

			// Add some jobs < 1 sec triggerAt
			for i := 0; i < 10; i++ {
				j := NewJobAutoID(s.Start().Add(time.Duration(rand.Intn(900))), nil)
				Expect(s.AddJobLocked(j)).To(BeNil())
			}
			Expect(s.PendingJobsLen()).To(Equal(10))
			// Add some jobs > 1 sec triggerAt
			for i := 0; i < 10; i++ {
				j := NewJobAutoID(s.Start().Add(time.Second*20+time.Duration(10+rand.Intn(40))), nil)
				Expect(s.AddJobLocked(j)).To(BeNil())
			}
			Expect(s.PendingJobsLen()).To(Equal(20))

			// Wait for all jobs to be ready
			time.Sleep(time.Second)

			jobs := []*Job{}
			for s.PendingJobsLen() > 10 {
				j, err := s.NextLocked()
				Expect(err).To(BeNil())
				if j != nil {
					jobs = append(jobs, j)
				} else {
					break
				}
			}
			Expect(len(jobs)).To(Equal(10))
			prev := jobs[0]
			for i := 1; i < len(jobs); i++ {
				// Walk returns jobs in order
				Expect(prev.TriggerAt().Sub(jobs[i].TriggerAt()) <= 0).To(BeTrue())
			}

			// 10 jobs should remain
			Expect(s.PendingJobsLen()).To(Equal(10))
		})

		It("cancels job from spoke", func() {
			s := NewSpoke(time.Now(), time.Now().Add(time.Hour), diskStore)
			Expect(s.PendingJobsLen()).To(Equal(0))

			j := NewJobAutoID(s.Start().Add(time.Minute*10), nil)
			Expect(s.AddJobLocked(j)).To(BeNil())
			Expect(s.PendingJobsLen()).To(Equal(1))

			s.CancelJobLocked(j.ID())
			Expect(s.PendingJobsLen()).To(Equal(0))
		})

		It("cancels job from spoke that doesnt exist does nothing", func() {
			s := NewSpoke(time.Now(), time.Now().Add(time.Hour), diskStore)
			Expect(s.PendingJobsLen()).To(Equal(0))
			s.CancelJobLocked(uuid.NewV4().String())
			s.CancelJobLocked(uuid.NewV4().String())
		})
	})

	Context("Spoke Ordering", func() {
		It("orders spokes correctly", func() {
			t := time.Now()
			sone := NewSpoke(t.Add(1), t.Add(10), diskStore)
			stwo := NewSpoke(t.Add(20), t.Add(30), diskStore)
			sthree := NewSpoke(t.Add(50), t.Add(55), diskStore)
			ordList := []*Spoke{sone, stwo, sthree}

			spokes := &PriorityQueue{stwo.AsPriorityItem(), sone.AsPriorityItem(), sthree.AsPriorityItem()}
			heap.Init(spokes)

			// Expected order pop
			for _, spoke := range ordList {
				s := heap.Pop(spokes)
				item := s.(*Item)
				Expect(item.Value().(*Spoke).ID()).To(Equal(spoke.ID()))
			}
		})
	})
	Context("Spoke disk storage", func() {
		It("stores and retrieves jobs from disk", func() {
			s := NewSpoke(time.Now(), time.Now().Add(time.Minute*100), diskStore)

			// Add a job
			j := NewJobAutoID(time.Now().Add(time.Minute*10), []byte("test data"))
			err := s.AddJobLocked(j)
			Expect(err).To(BeNil())
			Expect(s.PendingJobsLen()).To(Equal(1))

			// Job should be stored on disk
			Expect(s.OwnsJobLocked(j.ID())).To(BeTrue())

			// Memory footprint should be small (just indices)
			footprint := s.MemoryFootprint()
			Expect(footprint).To(BeNumerically("<", 1000)) // Much smaller than full job
		})
	})
})
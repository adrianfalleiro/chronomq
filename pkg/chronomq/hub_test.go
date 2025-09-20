package chronomq_test

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/chronomq/chronomq/pkg/chronomq"
	"github.com/chronomq/chronomq/pkg/persistence"
)

var diskStore *persistence.DiskJobStore
var tempDir string

var _ = Describe("Test hub", func() {
	defer GinkgoRecover()

	BeforeEach(func() {
		// Create temporary directory for each test
		var err error
		tempDir, err = ioutil.TempDir("", "chronomq-test-")
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

	It("can create a hub", func() {
		// A hub with 10ms spokes
		h := NewHub(&HubOpts{SpokeSpan: time.Millisecond * 10, DiskStore: diskStore, AttemptRestore: false})
		Expect(h.Stats().CurrentJobs).To(Equal(int64(0)))
	})

	It("accepts jobs with random times and random spoke durations into a hub", func() {
		for i := 0; i < 50; i++ {
			// Create unique temp directory for this iteration
			iterTempDir, err := ioutil.TempDir("", "chronomq-iter-test-")
			Expect(err).To(BeNil())
			iterDiskStore := persistence.NewDiskJobStore(iterTempDir)

			h := NewHub(&HubOpts{
				SpokeSpan:      time.Second * time.Duration(rand.Intn(2999)+1),
				DiskStore:      iterDiskStore,
				AttemptRestore: false})

			j := NewJobAutoID(time.Now().Add(time.Millisecond*time.Duration(rand.Intn(999999))), nil)
			h.AddJobLocked(j)
			Expect(h.Stats().CurrentJobs).To(Equal(int64(1)))

			// Clean up
			h.Stop(false)
			os.RemoveAll(iterTempDir)
		}
	})

	It("walks job from a hub in proper order - with timeout", func(done Done) {
		defer close(done)

		// hub with spokes spanning  3000 nanosec (Faster for testing)
		opts := &HubOpts{
			SpokeSpan:      time.Nanosecond * 3000,
			DiskStore:      diskStore,
			AttemptRestore: false}
		h := NewHub(opts)

		// Add a jobs with a random trigger time in the future - max 9999 nanosec
		jobs := [1000]*Job{}
		for i := 0; i < len(jobs); i++ {
			// Some jobs could already be in the past
			triggerAt := time.Now().Add(time.Nanosecond * time.Duration(rand.Intn(9999)))
			if rand.Float32() <= 0.2 {
				triggerAt = time.Now().Add(time.Nanosecond * time.Duration(-1*rand.Intn(9999)))
			}

			j := NewJobAutoID(triggerAt, nil)

			jobs[i] = j
		}

		// Shuffle jobs
		rand.Shuffle(len(jobs), func(i, j int) {
			jobs[i], jobs[j] = jobs[j], jobs[i]
		})

		// Add all of them
		for i, j := range jobs {
			h.AddJobLocked(j)
			Expect(h.Stats().CurrentJobs).To(Equal(int64(i + 1)))
		}

		// Walk should return all jobs in global order
		walked := []*Job{}
		for h.Stats().CurrentJobs > 0 {
			job, err := h.NextLocked()
			Expect(err).To(BeNil())
			if job != nil {
				walked = append(walked, job)
			} else {
				break // No more ready jobs
			}
		}

		// Expect correct order
		var prev *Job = nil
		for _, j := range walked {
			if prev != nil {
				Expect(prev.TriggerAt().Before(j.TriggerAt()))
			}
			prev = j
		}

		Expect(h.Stats().CurrentJobs).To(Equal(int64(0)))

	}, 1.500)

	It("Persists and recovers from disk", func(done Done) {
		defer close(done)

		opts := &HubOpts{
			SpokeSpan:      time.Nanosecond * 3000,
			DiskStore:      diskStore,
			AttemptRestore: false}
		h := NewHub(opts)

		// Add a jobs with a random trigger time in the future - max 9999 nanosec
		jobs := [1000]*Job{}
		for i := 0; i < len(jobs); i++ {
			// Some jobs could already be in the past
			triggerAt := time.Now().Add(time.Nanosecond * time.Duration(rand.Intn(9999)))
			if rand.Float32() <= 0.2 {
				triggerAt = time.Now().Add(time.Nanosecond * time.Duration(-1*rand.Intn(9999)))
			}

			j := NewJobAutoID(triggerAt, nil)

			jobs[i] = j
		}

		jobMap := make(map[string]*Job, len(jobs))
		// Add all of them
		for i, j := range jobs {
			h.AddJobLocked(j)
			jobMap[j.ID()] = j
			Expect(h.Stats().CurrentJobs).To(Equal(int64(i + 1)))
		}

		// Reserve some jobs
		job1, err := h.NextLocked()
		Expect(err).To(BeNil())
		Expect(job1).ToNot(BeNil())
		job2, err := h.NextLocked()
		Expect(err).To(BeNil())
		Expect(job2).ToNot(BeNil())
		job3, err := h.NextLocked()
		Expect(err).To(BeNil())
		Expect(job3).ToNot(BeNil())

		// Check that jobs are persisted to disk
		// Count files in tempDir to verify disk persistence
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
		// Should have remaining jobs on disk (1000 - 3 consumed = 997)
		Expect(jobFileCount).To(BeNumerically(">", 990))
	}, 15)

	It("rebuilds indices from disk on startup", func(done Done) {
		defer close(done)

		// Create a hub and add some jobs
		opts := &HubOpts{
			SpokeSpan:      time.Nanosecond * 3000,
			DiskStore:      diskStore,
			AttemptRestore: false}
		h1 := NewHub(opts)

		// Add jobs
		jobs := make([]*Job, 10)
		for i := 0; i < 10; i++ {
			triggerAt := time.Now().Add(time.Hour * time.Duration(i+1))
			jobs[i] = NewJobAutoID(triggerAt, []byte("test job data"))
			err := h1.AddJobLocked(jobs[i])
			Expect(err).To(BeNil())
		}

		Expect(h1.Stats().CurrentJobs).To(Equal(int64(10)))

		// Stop the first hub
		h1.Stop(false)

		// Create new hub with restore flag - should rebuild indices
		opts2 := &HubOpts{
			SpokeSpan:      time.Nanosecond * 3000,
			DiskStore:      diskStore,
			AttemptRestore: true}
		h2 := NewHub(opts2)
		defer h2.Stop(false)

		// Give time for async rebuild
		time.Sleep(100 * time.Millisecond)

		// Should have rebuilt the indices
		Expect(h2.Stats().CurrentJobs).To(Equal(int64(10)))
	})
})

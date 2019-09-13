package goyaad

import (
	"container/heap"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/urjitbhatia/goyaad/pkg/metrics"
	"github.com/urjitbhatia/goyaad/pkg/persistence"
)

const (
	hundredYears = time.Hour * 24 * 365 * 100
)

// HubOpts define customizations for Hub initialization
type HubOpts struct {
	Persister      persistence.Persister // persister to store/restore from disk
	AttemptRestore bool                  // If true, hub will try to restore from disk on start
	SpokeSpan      time.Duration         // How wide should the spokes be
}

// Hub is a time ordered collection of spokes
type Hub struct {
	spokeSpan time.Duration
	spokeMap  *sync.Map // quick lookup map
	spokes    *PriorityQueue

	pastSpoke    *Spoke // Permanently pinned to the past
	currentSpoke *Spoke // The current spoke

	removedJobsCount uint64
	lock             *sync.Mutex

	persister persistence.Persister
}

// NewHub creates a new hub where adjacent spokes lie at the given
// spokeSpan duration boundary.
func NewHub(opts *HubOpts) *Hub {
	h := &Hub{
		spokeSpan:        opts.SpokeSpan,
		spokeMap:         &sync.Map{},
		spokes:           &PriorityQueue{},
		pastSpoke:        NewSpoke(time.Now().Add(-1*hundredYears), time.Now().Add(hundredYears)),
		currentSpoke:     nil,
		removedJobsCount: 0,
		lock:             &sync.Mutex{},
		persister:        opts.Persister,
	}
	heap.Init(h.spokes)

	logrus.WithFields(logrus.Fields{
		"spokeSpan":      opts.SpokeSpan,
		"attemptRestore": opts.AttemptRestore,
	}).Info("Created hub")

	go func() {
		if opts.AttemptRestore {
			logrus.Info("Hub: Entering restore mode")
			err := h.Restore()
			if err != nil {
				logrus.Error("Hub: Restore error", err)
			}

			logrus.Info("Hub: Initial restore finished. Resuming")
		}
	}()
	go h.StatusPrinter()

	return h
}

// Stop the hub gracefully and if persist is true, then persist all jobs to disk for later recovery
func (h *Hub) Stop(persist bool) {
	if persist {
		logrus.Infof("Hub:Stop Starting persistence for pid: %d", os.Getpid())
		errC := h.Persist()
		errCount := 0
		for range errC {
			errCount++
		}
		logrus.Infof("Hub:Stop Finished persistence with %d errors", errCount)
	}
	logrus.Infof("Hub:Stop stopped")
}

// PendingJobsCount return the number of jobs currently pending
func (h *Hub) PendingJobsCount() int {
	count := h.pastSpoke.PendingJobsLen()
	h.spokeMap.Range(func(k, v interface{}) bool {
		count += v.(*Spoke).PendingJobsLen()
		return true
	})
	return count
}

// CancelJob cancels a job if found. Calls are noop for unknown jobs
func (h *Hub) CancelJob(jobID string) error {
	go metrics.Incr("hub.cancel.req")
	h.lock.Lock()
	defer h.lock.Unlock()

	logrus.Debug("cancel: ", jobID)

	s, err := h.FindOwnerSpoke(jobID)
	if err != nil {
		logrus.Debug("cancel found no owner spoke: ", jobID)
		// return nil - cancel if job not found is idempotent
		return nil
	}
	logrus.Debug("cancel found owner spoke: ", jobID)
	err = s.CancelJob(jobID)
	h.removedJobsCount++
	go metrics.Incr("hub.cancel.ok")
	return err
}

// FindOwnerSpoke returns the spoke that owns this job
func (h *Hub) FindOwnerSpoke(jobID string) (*Spoke, error) {

	if h.pastSpoke.OwnsJob(jobID) {
		return h.pastSpoke, nil
	}

	// Checking the current spoke - lock the hub
	if h.currentSpoke != nil && h.currentSpoke.OwnsJob(jobID) {
		return h.currentSpoke, nil
	}

	// Find the owner in the spoke map
	var owner *Spoke
	h.spokeMap.Range(func(k, v interface{}) bool {
		s := v.(*Spoke)
		if s.OwnsJob(jobID) {
			owner = s
			return false
		}
		return true
	})
	if owner != nil {
		return owner, nil
	}
	return nil, errors.New("Cannot find job owner spoke")
}

// addSpoke adds spoke s to this hub
func (h *Hub) addSpoke(s *Spoke) {
	h.spokeMap.Store(s.SpokeBound, s)
	heap.Push(h.spokes, s.AsPriorityItem())
}

// Next returns the next job that is ready now or returns nil.
func (h *Hub) Next() *Job {
	defer metrics.Time("hub.next.search.duration", time.Now())

	h.lock.Lock()
	defer h.lock.Unlock()

	// since we have the lock, send some metrics
	go metrics.GaugeInt("hub.job.count", h.PendingJobsCount())
	go metrics.GaugeInt("hub.spoke.count", h.spokes.Len())

	// Lock Past spoke lock in func scope
	if j := func() *Job {
		pastLocker := h.pastSpoke.GetLocker()
		pastLocker.Lock()
		defer pastLocker.Unlock()
		go metrics.GaugeInt("hub.job.pastspoke.count", h.pastSpoke.PendingJobsLen())

		// Find a job in past spoke
		j := h.pastSpoke.Next()
		if j != nil {
			logrus.Debug("Got job from past spoke")
		}
		return j
	}(); j != nil {
		h.removedJobsCount++
		return j
	}

	// Checked past spoke

	// Find a job in current spoke
	// If current is empty and now expired, prune it...
	if h.currentSpoke != nil {
		h.currentSpoke = func() *Spoke {
			h.currentSpoke.Lock()
			defer h.currentSpoke.Unlock()
			if h.currentSpoke.PendingJobsLen() == 0 && h.currentSpoke.AsTemporalState() == Past {
				logrus.Info("pruning the current spoke")
				// This routine could be unfortunate - it found a currentspoke that was expired
				// so it has the pay the price finding the next candidate
				h.spokeMap.Delete(h.currentSpoke.SpokeBound)
				return nil
			}
			return h.currentSpoke
		}()
	}

	// No currently assigned spoke
	if h.currentSpoke == nil {
		// Fix the heap
		heap.Init(h.spokes)

		if h.spokes.Len() == 0 {
			// No spokes - can't do anything. Return
			return nil
		}

		// New current candidate
		item := h.spokes.AtIdx(0)
		current := item.value.(*Spoke)
		switch current.AsTemporalState() {
		case Future:
			// Next in time is still not current. Can't do anything. Return
			return nil
		case Past, Current:
			// We have found a new current spoke
			h.currentSpoke = current
			// Pop it from the queue - this is now a current spoke
			heap.Pop(h.spokes)
			logrus.Infof("Hub spoke pop cap: %d", h.spokes.Cap())
		}
	}

	// Read from current spoke

	// Assert - At this point, hub should have a current spoke
	if h.currentSpoke == nil {
		logrus.Panic("Unreachable state :: hub has a nil spoke after candidate search")
	}

	currentLocker := h.currentSpoke.GetLocker()
	currentLocker.Lock()
	defer currentLocker.Unlock()
	go metrics.GaugeInt("hub.job.currentspoke.count", h.currentSpoke.PendingJobsLen())

	j := h.currentSpoke.Next()
	if j == nil {
		// no job - return
		logrus.Debug("No job in current spoke")
		return nil
	}

	logrus.Debug("returning job: ", j.id)
	h.removedJobsCount++
	return j
}

func (h *Hub) mergeQueues(pq *PriorityQueue) {
	for pq.Len() > 0 {
		i := heap.Pop(pq)
		h.spokes.Push(i)
	}
}

// Prune clears spokes which are expired and have no jobs
// returns the number of spokes pruned
func (h *Hub) Prune() int {
	pruned := 0
	h.spokeMap.Range(func(k, v interface{}) bool {
		s := v.(*Spoke)
		if s.IsExpired() && s.PendingJobsLen() == 0 {
			h.spokeMap.Delete(k)
		}
		pruned++
		return true
	})

	return pruned
}

// AddJob to this hub. Hub should never reject a job - this method will panic if that happens
func (h *Hub) AddJob(j *Job) error {
	defer metrics.Time("hub.job.add.duration", time.Now())
	go metrics.GaugeInt("hub.job.size", len(j.body))

	h.lock.Lock()
	defer h.lock.Unlock()

	switch j.AsTemporalState() {
	case Past:
		logrus.Tracef("Adding job: %s to past spoke", j.id)
		pastLocker := h.pastSpoke.GetLocker()
		pastLocker.Lock()
		defer pastLocker.Unlock()

		logrus.WithField("JobID", j.ID()).Trace("Adding job to past spoke")
		err := h.pastSpoke.AddJob(j)
		if err != nil {
			logrus.WithError(err).Error("Past spoke rejected job. This should never happen")
			return err
		}
		go metrics.Incr("hub.addjob.past")
	case Future:
		logrus.Tracef("Adding job: %s to future spoke", j.id)
		// Lock current spoke so that add fixes the PQ as it adds
		if h.currentSpoke != nil {
			currLocker := h.currentSpoke.GetLocker()
			currLocker.Lock()
			defer currLocker.Unlock()

			if h.currentSpoke.ContainsJob(j) {
				err := h.currentSpoke.AddJob(j)
				if err != nil {
					logrus.WithError(err).Error("Current spoke rejected job. This should never happen")
					return err
				}
				return nil
			}
		}

		// Search for a spoke that can take ownership of this job
		// Reads are still going to be ordered anyways
		jobBound := j.AsBound(h.spokeSpan)
		candidate, ok := h.spokeMap.Load(jobBound)
		if ok {
			candidateSpoke := candidate.(*Spoke)
			// Found a candidate that can take this job
			logrus.Debugf("Adding job: %s to candidate spoke", j.id)
			err := candidateSpoke.AddJob(j)
			if err != nil {
				logrus.WithError(err).Error("Hub should always accept a job. No spoke accepted")
				return err
			}
			// Accepted, all done...
			return nil
		}

		// Time to create a new spoke for this job
		logrus.Debugf("Adding job: %s to a new spoke", j.id)
		s := NewSpoke(jobBound.start, jobBound.end)
		err := s.AddJob(j)
		if err != nil {
			logrus.WithError(err).Error("Hub should always accept a job. No spoke accepted")
			return err
		}

		// h is still locked here so it's ok
		h.addSpoke(s)
	}
	go metrics.Incr("hub.addjob")
	return nil
}

// Status prints the state of the spokes of this hub
func (h *Hub) Status() {
	logrus.Info("-------------------------------------------------------------")
	h.lock.Lock()
	defer h.lock.Unlock()

	spokesCount := h.spokes.Len()
	logrus.Infof("Hub has %d spokes", spokesCount)
	go metrics.GaugeInt("hub.spoke.count", spokesCount)

	pendingJobCount := h.PendingJobsCount()
	logrus.Infof("Hub has %d total jobs", pendingJobCount)
	go metrics.GaugeInt("hub.job.count", pendingJobCount)

	logrus.Infof("Hub has %d removed jobs", h.removedJobsCount)
	go metrics.Gauge("hub.job.removed.count", float64(h.removedJobsCount))

	logrus.Infof("Past spoke has %d jobs", h.pastSpoke.PendingJobsLen())
	go metrics.GaugeInt("hub.job.pastspoke.count", h.pastSpoke.PendingJobsLen())

	if h.currentSpoke != nil {
		logrus.Infof("Current spoke has %d jobs", h.currentSpoke.PendingJobsLen())
		go metrics.GaugeInt("hub.job.currentspoke.count", h.currentSpoke.PendingJobsLen())
	}

	logrus.Infof("Assigned current spoke: %v", h.currentSpoke == nil)
	logrus.Info("-------------------------------------------------------------")
}

// StatusPrinter starts a status printer that prints hub stats over some time interval
func (h *Hub) StatusPrinter() {
	t := time.NewTicker(time.Second * 10)
	for range t.C {
		h.Status()
	}
}

// Persist locks the hub and starts persisting data to disk
func (h *Hub) Persist() chan error {
	logrus.Warn("Starting disk offload")
	wg := &sync.WaitGroup{}
	ec := make(chan error)

	go func() {
		h.lock.Lock()
		defer h.lock.Unlock()

		logrus.Warnf("Total spokes: %d Total jobs: %d", h.spokes.Len(), h.PendingJobsCount())
		wg.Add(h.spokes.Len())
		defer close(ec)

		for i := 0; i < h.spokes.Len(); i++ {
			s := h.spokes.AtIdx(i).value.(*Spoke)
			func() {
				defer wg.Done()
				errC := s.Persist(h.persister)
				for e := range errC {
					ec <- e
				}
			}()
		}

		// Save past spoke
		errC := h.pastSpoke.Persist(h.persister)
		for e := range errC {
			ec <- e
		}

		// Save current spoke
		if h.currentSpoke != nil {
			errC := h.currentSpoke.Persist(h.persister)
			for e := range errC {
				ec <- e
			}
		}

		wg.Wait()
		h.persister.Finalize()
	}()

	return ec
}

// Restore loads any jobs saved to disk at the given path
func (h *Hub) Restore() error {
	jobs, err := h.persister.Recover()
	if err != nil {
		return err
	}

	errDecodeCount := 0
	errAddCount := 0
	recoverCount := 0
	for e := range jobs {
		j := new(Job)
		err := j.GobDecode(e)
		if err != nil {
			errDecodeCount++
			logrus.Error(err)
			continue
		}
		if err = h.AddJob(j); err != nil {
			errAddCount++
			logrus.Error(err)
			continue
		}
		recoverCount++
	}
	logrus.Infof("Hub:Restore recovered %d entries", recoverCount)

	if errAddCount == 0 && errDecodeCount == 0 {
		return nil
	}

	var retErr = errors.New("Hub:Restore failed")
	retErr = errors.Wrapf(retErr, "Hub:Restore encountered %d errors decoding persisted jobs", errDecodeCount)
	retErr = errors.Wrapf(retErr, "Hub:Restore encountered %d errors adding persisted jobs", errAddCount)
	return retErr
}

// GetNJobs returns upto N jobs (or less if there are less jobs in available)
// It does not return a consistent snapshot of jobs but provides a best effort view
func (h *Hub) GetNJobs(n int) chan *Job {
	jobChan := make(chan *Job)
	go func() {
		defer close(jobChan)
		func() {
			// Iterate over jobs from the past spoke first
			h.pastSpoke.Lock()
			defer h.pastSpoke.Unlock()

			pastJobsLen := h.pastSpoke.jobQueue.Len()
			for i := 0; i < pastJobsLen; i++ {
				j := h.pastSpoke.jobQueue.AtIdx(i)
				jobChan <- j.Value().(*Job)
				n--
				if n <= 0 {
					return
				}
			}
		}()
		if n <= 0 {
			// Found all requested jobs
			return
		}

		// Iterate over the future spokes from the map (We dont care about the order in this case)
		h.spokeMap.Range(func(sk, sv interface{}) bool {
			s := sv.(*Spoke)
			return func() bool {
				s.Lock()
				defer s.Unlock()

				// Iterate over the jobs in this spoke
				jobsLen := s.jobQueue.Len()
				for i := 0; i < jobsLen; i++ {
					j := s.jobQueue.AtIdx(i)
					jobChan <- j.Value().(*Job)
					n--
					if n <= 0 {
						return false
					}
				}
				// Continue if n is not 0 yet (return true to Range fn)
				return n > 0
			}()
		})
	}()

	return jobChan
}

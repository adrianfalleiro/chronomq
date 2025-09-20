package chronomq

import (
	"container/heap"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	cuckoo "github.com/seiflotfy/cuckoofilter"

	"github.com/chronomq/chronomq/internal/queue"
	"github.com/chronomq/chronomq/internal/stats"
	"github.com/chronomq/chronomq/internal/temporal"
	"github.com/chronomq/chronomq/pkg/metrics"
	"github.com/chronomq/chronomq/pkg/persistence"
)

// IndexedHubOpts define customizations for IndexedHub initialization
type IndexedHubOpts struct {
	Persister      persistence.Persister    // persister to store/restore from disk
	DiskStore      persistence.DiskJobStore // disk storage for job data
	AttemptRestore bool                     // If true, hub will try to restore from disk on start
	SpokeSpan      time.Duration            // How wide should the spokes be
	MaxCFSize      uint                     // Max size of the Cuckoo Filter
}

// IndexedHub is a memory-efficient hub that stores only job indices in memory
type IndexedHub struct {
	jobFilter *cuckoo.Filter
	spokeSpan time.Duration                        // How much time does a spoke span
	spokeMap  map[temporal.Bound]*IndexedSpoke     // Quick lookup map for indexed spokes
	spokes    *queue.PriorityQueue                 // Actual indexed spokes sorted by time
	diskStore persistence.DiskJobStore             // Disk storage for full job data

	pastSpoke    *IndexedSpoke // Permanently pinned to the past
	currentSpoke *IndexedSpoke // The current spoke - started in the past or now, ends in the future or now

	stats *stats.Counters
	lock  *sync.Mutex

	persister persistence.Persister
}

// NewIndexedHub creates a new indexed hub where adjacent spokes lie at the given
// spokeSpan duration boundary and jobs are stored on disk with indices in memory
func NewIndexedHub(opts *IndexedHubOpts) *IndexedHub {
	maxCFSize := TestMaxCFSize // keep test mem requirements low by default
	if opts.MaxCFSize != 0 {
		maxCFSize = opts.MaxCFSize
	}

	h := &IndexedHub{
		jobFilter:    cuckoo.NewFilter(maxCFSize),
		spokeSpan:    opts.SpokeSpan,
		spokeMap:     make(map[temporal.Bound]*IndexedSpoke),
		spokes:       &queue.PriorityQueue{},
		diskStore:    opts.DiskStore,
		pastSpoke:    NewIndexedSpoke(time.Now().Add(-1*hundredYears), time.Now().Add(hundredYears), opts.DiskStore),
		currentSpoke: nil,
		stats:        &stats.Counters{},
		lock:         &sync.Mutex{},
		persister:    opts.Persister,
	}
	heap.Init(h.spokes)

	log.Info().Dur("spokeSpan", opts.SpokeSpan).
		Bool("attemptRestore", opts.AttemptRestore).
		Uint("maxCFSize", maxCFSize).
		Msg("Created indexed hub")

	go func() {
		if opts.AttemptRestore {
			log.Info().Msg("IndexedHub: Entering restore mode")
			// Use new rebuild method instead of old Restore() method
			err := h.RebuildIndicesFromDisk()
			if err != nil {
				log.Error().Err(err).Msg("IndexedHub: Restore error")
			}
			log.Info().Msg("IndexedHub: Initial restore finished. Resuming")
		}
	}()
	go h.StatusPrinter()

	return h
}

// Stop the hub gracefully and if persist is true, then persist all jobs to disk for later recovery
func (h *IndexedHub) Stop(persist bool) {
	if persist {
		log.Info().Int("PID", os.Getpid()).Msg("IndexedHub:Stop Starting persistence")
		errC := h.PersistLocked()
		errCount := 0
		for range errC {
			errCount++
		}
		log.Info().Int("errorCount", errCount).Msg("IndexedHub:Stop Finished persistence with errors")
	}

	// Close disk store
	if err := h.diskStore.Close(); err != nil {
		log.Error().Err(err).Msg("IndexedHub:Stop Error closing disk store")
	}

	log.Info().Msg("IndexedHub:Stop stopped")
}

// Stats returns a snapshot of the current hub's stats
func (h *IndexedHub) Stats() stats.Snapshot {
	return h.stats.Read()
}

// CancelJobLocked cancels a job if found. Calls are noop for unknown jobs
func (h *IndexedHub) CancelJobLocked(jobID string) (*Job, error) {
	go metrics.Incr("indexedhub.cancel.req")
	id := []byte(jobID)

	h.lock.Lock()
	defer h.lock.Unlock()

	if !h.jobFilter.Lookup(id) {
		// no such job (filter can have false positives but not false negatives)
		go metrics.Incr("indexedhub.cancel.ok")
		go metrics.Incr("indexedhub.cancel.notfound")
		return nil, nil
	}

	j, err := h.cancelJob(jobID)
	if err == nil {
		// delete from hub was successful
		// remove from cuckoo filter (might not be present but we don't care either way)
		if !h.jobFilter.Delete(id) {
			go metrics.Incr("indexedhub.cancel.should_be_unreachable")
		}
	} else {
		// job not found
		go metrics.Incr("indexedhub.cancel.notfound")
	}

	return j, err
}

func (h *IndexedHub) cancelJob(jobID string) (*Job, error) {
	log.Debug().Str("jobID", jobID).Msg("canceling job in indexed hub")

	s, err := h.findOwnerSpoke(jobID)
	if err != nil {
		log.Debug().Str("jobID", jobID).Msg("cancel found no owner spoke")
		return nil, nil
	}

	log.Debug().Str("jobID", jobID).Msg("cancel found owner spoke")
	j, err := s.CancelJobLocked(jobID)
	if err == nil {
		h.stats.DecrJob()
		go metrics.Incr("indexedhub.cancel.ok")
	}
	return j, err
}

// findOwnerSpoke returns the spoke that owns this job
func (h *IndexedHub) findOwnerSpoke(jobID string) (*IndexedSpoke, error) {
	if h.pastSpoke.OwnsJobLocked(jobID) {
		return h.pastSpoke, nil
	}

	// Checking the current spoke
	if h.currentSpoke != nil && h.currentSpoke.OwnsJobLocked(jobID) {
		return h.currentSpoke, nil
	}

	// Find the owner in the spoke map
	for _, s := range h.spokeMap {
		if s.OwnsJobLocked(jobID) {
			return s, nil
		}
	}
	return nil, errors.New("Cannot find job owner spoke")
}

// addSpoke adds spoke s to this hub
func (h *IndexedHub) addSpoke(s *IndexedSpoke) {
	defer h.stats.IncrSpoke()
	h.spokeMap[s.Bound] = s
	heap.Push(h.spokes, s.AsPriorityItem())
}

// deleteSpokeFromMap removes a spoke from the map
func (h *IndexedHub) deleteSpokeFromMap(s *IndexedSpoke) {
	defer h.stats.DecrSpoke()
	delete(h.spokeMap, s.Bound)
}

// NextLocked returns the next job that is ready now or returns nil.
// This will load the job from disk when needed.
func (h *IndexedHub) NextLocked() (*Job, error) {
	defer metrics.Time("indexedhub.next.search.duration", time.Now())

	h.lock.Lock()
	defer h.lock.Unlock()

	j, err := h.next()
	if err != nil {
		return nil, err
	}

	if j != nil {
		h.jobFilter.Delete([]byte(j.ID()))
	}

	return j, nil
}

func (h *IndexedHub) next() (*Job, error) {
	// since we have the lock, send some metrics
	go metrics.GaugeInt("indexedhub.job.count", int(h.stats.Read().CurrentJobs))
	go metrics.GaugeInt("indexedhub.spoke.count", h.spokes.Len())

	// Check past spoke first
	if j, err := func() (*Job, error) {
		go metrics.GaugeInt("indexedhub.job.past.count", h.pastSpoke.PendingJobsLen())

		// Find a job in past spoke
		j, err := h.pastSpoke.NextLocked()
		if err != nil {
			return nil, fmt.Errorf("error retrieving job from past spoke: %w", err)
		}
		if j != nil {
			log.Debug().Msg("Got job from past spoke")
		}
		return j, nil
	}(); j != nil || err != nil {
		if err != nil {
			return nil, err
		}
		h.stats.DecrJob()
		return j, nil
	}

	// Handle current spoke expiration
	if h.currentSpoke != nil {
		h.currentSpoke = func() *IndexedSpoke {
			if h.currentSpoke.PendingJobsLen() == 0 && h.currentSpoke.AsTemporalState() == temporal.Past {
				h.deleteSpokeFromMap(h.currentSpoke)
				return nil
			}
			return h.currentSpoke
		}()
	}

	// Find new current spoke if needed
	if h.currentSpoke == nil {
		// Fix the heap
		heap.Init(h.spokes)

		if h.spokes.Len() == 0 {
			// No spokes - can't do anything. Return
			return nil, nil
		}

		// New current candidate
		item := h.spokes.AtIdx(0)
		current := item.Value().(*IndexedSpoke)
		switch current.AsTemporalState() {
		case temporal.Future:
			// Next in time is still not current. Can't do anything. Return
			return nil, nil
		case temporal.Past, temporal.Current:
			// We have found a new current spoke
			h.currentSpoke = current
			// Pop it from the queue - this is now a current spoke
			heap.Pop(h.spokes)
		}
	}

	// Assert - At this point, hub should have a current spoke
	if h.currentSpoke == nil {
		log.Panic().Msg("Unreachable state :: indexed hub has a nil spoke after candidate search")
	}

	go metrics.GaugeInt("indexedhub.job.current.count", h.currentSpoke.PendingJobsLen())

	j, err := h.currentSpoke.NextLocked()
	if err != nil {
		return nil, fmt.Errorf("error retrieving job from current spoke: %w", err)
	}

	if j == nil {
		log.Debug().Msg("No job in current spoke")
		return nil, nil
	}

	log.Debug().Str("jobID", j.ID()).Msg("returning next job from indexed hub")
	h.stats.DecrJob()
	return j, nil
}

// AddJobLocked to this hub. Hub should never reject a job - this method will panic if that happens
func (h *IndexedHub) AddJobLocked(j *Job) error {
	defer metrics.Time("indexedhub.job.add.duration", time.Now())
	go metrics.GaugeInt("indexedhub.job.size", len(j.Body()))

	h.lock.Lock()
	defer h.lock.Unlock()

	// Check if job already exists in the system
	id := []byte(j.ID())
	if h.jobFilter.Lookup(id) {
		// filter can give us false positives, do a full scan
		if spoke, _ := h.findOwnerSpoke(j.ID()); spoke != nil {
			return fmt.Errorf("Rejecting new job. Job with ID: %s already exists", j.ID())
		}
	}

	err := h.addJob(j)
	if err == nil {
		if !h.jobFilter.Insert(id) {
			log.Error().Msgf("Could not insert into the filter. ID: %s", id)
		}
		h.stats.IncrJob()
		go metrics.Incr("indexedhub.addjob")
	}
	return err
}

func (h *IndexedHub) addJob(j *Job) error {
	switch j.AsTemporalState() {
	case temporal.Past:
		log.Debug().Str("jobID", j.ID()).Msg("Adding job to past spoke")
		err := h.pastSpoke.AddJobLocked(j)
		if err != nil {
			log.Error().Err(err).Msg("Past spoke rejected job. This should never happen")
			return err
		}
		go metrics.Incr("indexedhub.addjob.past")
		return nil

	case temporal.Future:
		log.Debug().Str("jobID", j.ID()).Msg("Adding job to future spoke")

		// Check current spoke first
		if h.currentSpoke != nil {
			if h.currentSpoke.IsJobInBounds(j) {
				err := h.currentSpoke.AddJobLocked(j)
				if err != nil {
					log.Error().Err(err).Msg("Current spoke rejected job. This should never happen")
					return err
				}
				return nil
			}
		}

		// Search for a spoke that can take ownership of this job
		jobBound := j.AsBound(h.spokeSpan)
		if candidateSpoke, ok := h.spokeMap[jobBound]; ok {
			log.Debug().Str("jobID", j.ID()).Msg("Adding job to candidate spoke")
			err := candidateSpoke.AddJobLocked(j)
			if err != nil {
				log.Error().Err(err).Msg("Hub should always accept a job. No spoke accepted. This should never happen")
				return err
			}
			return nil
		}

		// Time to create a new spoke for this job
		log.Debug().Str("jobID", j.ID()).Msg("Adding job to a new indexed spoke")
		s := NewIndexedSpoke(jobBound.Start(), jobBound.End(), h.diskStore)
		err := s.AddJobLocked(j)
		if err != nil {
			log.Error().Err(err).Msg("Hub should always accept a job. No spoke accepted. This should never happen")
			return err
		}
		h.addSpoke(s)
		return nil
	}

	err := errors.Errorf("Unable to find a spoke for job. This should never happen")
	log.Error().Err(err).Msg("Can't add job to indexed hub")
	return err
}

// StatusLocked prints the state of the spokes of this hub
func (h *IndexedHub) StatusLocked() {
	log.Info().Msg("----------------------IndexedHub Stats------------------------")

	hubStats := h.stats.Read()
	log.Info().Int64("spokesCount", hubStats.CurrentSpokes).Send()
	go metrics.GaugeInt("indexedhub.spoke.count", int(hubStats.CurrentSpokes))

	log.Info().Int64("pendingJobsCount", hubStats.CurrentJobs).Send()
	go metrics.GaugeInt("indexedhub.job.count", int(hubStats.CurrentJobs))

	log.Info().Int64("removedJobsCount", hubStats.RemovedJobs).Send()
	go metrics.GaugeInt("indexedhub.job.removed.count", int(hubStats.RemovedJobs))

	// Calculate memory footprint
	memoryFootprint := 0
	h.lock.Lock()
	memoryFootprint += h.pastSpoke.MemoryFootprint()
	if h.currentSpoke != nil {
		memoryFootprint += h.currentSpoke.MemoryFootprint()
	}
	for _, s := range h.spokeMap {
		memoryFootprint += s.MemoryFootprint()
	}
	h.lock.Unlock()

	log.Info().Int("memoryFootprintBytes", memoryFootprint).Send()
	go metrics.GaugeInt("indexedhub.memory.footprint", memoryFootprint)

	// lock only for this bit - current spoke can be replaced while running...
	h.lock.Lock()
	defer h.lock.Unlock()
	log.Info().Int("pastSpokePendingJobsCount", h.pastSpoke.PendingJobsLen()).Send()
	log.Info().Uint("jobFilterCount", h.jobFilter.Count()).Send()
	go metrics.GaugeInt("indexedhub.job.past.count", h.pastSpoke.PendingJobsLen())

	if h.currentSpoke != nil {
		log.Info().Int("currentSpokePendingJobsCount", h.currentSpoke.PendingJobsLen()).Send()
		go metrics.GaugeInt("indexedhub.job.current.count", h.currentSpoke.PendingJobsLen())
	}
	log.Info().Msg("-------------------------------------------------------------")
}

// StatusPrinter starts a status printer that prints hub stats over some time interval
func (h *IndexedHub) StatusPrinter() {
	t := time.NewTicker(time.Second * 10)
	for range t.C {
		h.StatusLocked()
	}
}

// PersistLocked locks the hub and starts persisting data to disk
func (h *IndexedHub) PersistLocked() chan error {
	log.Warn().Msg("Starting indexed hub disk offload")
	ec := make(chan error)

	h.lock.Lock()
	go func() {
		defer h.lock.Unlock()
		defer close(ec)

		log.Warn().
			Int("totalSpokes", h.spokes.Len()).
			Int64("pendingJobsCount", h.stats.Read().CurrentJobs).
			Msg("About to persist indexed hub")

		for i := 0; i < h.spokes.Len(); i++ {
			s := h.spokes.AtIdx(i).Value().(*IndexedSpoke)
			errC := s.PersistIndices(h.persister)
			for e := range errC {
				ec <- e
			}
		}

		// Save past spoke indices
		errC := h.pastSpoke.PersistIndices(h.persister)
		for e := range errC {
			ec <- e
		}

		// Save current spoke indices
		if h.currentSpoke != nil {
			errC := h.currentSpoke.PersistIndices(h.persister)
			for e := range errC {
				ec <- e
			}
		}

		h.persister.Finalize()
	}()

	return ec
}

// Restore loads any jobs saved to disk at the given path
func (h *IndexedHub) Restore() error {
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
			log.Error().Err(err).Send()
			continue
		}
		if err = h.AddJobLocked(j); err != nil {
			errAddCount++
			log.Error().Err(err).Send()
			continue
		}
		recoverCount++
	}
	log.Info().Int("recoverCount", recoverCount).Msg("IndexedHub:Restore recovered entries")

	if errAddCount == 0 && errDecodeCount == 0 {
		return nil
	}

	var retErr = errors.New("IndexedHub:Restore failed")
	retErr = errors.Wrapf(retErr, "IndexedHub:Restore encountered %d errors decoding persisted jobs", errDecodeCount)
	retErr = errors.Wrapf(retErr, "IndexedHub:Restore encountered %d errors adding persisted jobs", errAddCount)
	return retErr
}

// GetNJobs returns up to N job indices (not full jobs) for inspection
func (h *IndexedHub) GetNJobIndices(n int) chan *JobIndex {
	indexChan := make(chan *JobIndex)
	go func() {
		defer close(indexChan)

		// Iterate over job indices from the past spoke first
		func() {
			h.pastSpoke.Lock()
			defer h.pastSpoke.Unlock()

			pastJobsLen := h.pastSpoke.PendingJobsLen()
			for i := 0; i < pastJobsLen; i++ {
				indexChan <- h.pastSpoke.JobIndexAtIdx(i)
				n--
				if n <= 0 {
					return
				}
			}
		}()

		if n <= 0 {
			return
		}

		// Iterate over the future spokes from the map
		for _, s := range h.spokeMap {
			s.Lock()
			defer s.Unlock()
			jobsLen := s.PendingJobsLen()
			for i := 0; i < jobsLen; i++ {
				indexChan <- s.JobIndexAtIdx(i)
				n--
				if n <= 0 {
					break
				}
			}
		}
	}()

	return indexChan
}

// RebuildIndicesFromDisk scans the disk storage and rebuilds all job indices in memory
// This method streams jobs from disk to avoid loading millions of job keys into memory at once
func (h *IndexedHub) RebuildIndicesFromDisk() error {
	h.lock.Lock()
	defer h.lock.Unlock()

	log.Info().Msg("Starting index rebuild from disk...")

	// Check if disk store supports streaming walkthrough
	diskStore, ok := h.diskStore.(interface {
		WalkJobFiles(func(string) error) error
	})
	if !ok {
		return fmt.Errorf("disk store does not support streaming job file iteration")
	}

	rebuiltCount := 0
	totalCount := 0

	// Stream process jobs from disk without loading all keys into memory
	err := diskStore.WalkJobFiles(func(jobKey string) error {
		totalCount++

		// Load job from disk to create index
		jobData, err := h.diskStore.RetrieveJob(jobKey)
		if err != nil {
			log.Warn().Err(err).Str("jobKey", jobKey).Msg("Failed to retrieve job during index rebuild")
			return nil // Continue with next job
		}

		// Decode job data
		data, ok := jobData.([]byte)
		if !ok {
			log.Warn().Str("jobKey", jobKey).Msg("Job data is not byte array")
			return nil // Continue with next job
		}

		job := &Job{}
		if err := job.GobDecode(data); err != nil {
			log.Warn().Err(err).Str("jobKey", jobKey).Msg("Failed to decode job during index rebuild")
			return nil // Continue with next job
		}

		// Create job index and add to appropriate spoke
		if err := h.addJobToSpoke(job, false); err != nil {
			log.Warn().Err(err).Str("jobID", job.ID()).Msg("Failed to add job to spoke during index rebuild")
			return nil // Continue with next job
		}

		rebuiltCount++

		// Log progress every 10,000 jobs for large datasets
		if rebuiltCount%10000 == 0 {
			log.Info().Int("rebuiltIndices", rebuiltCount).Msg("Index rebuild progress")
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to walk job files during rebuild: %w", err)
	}

	log.Info().
		Int("totalDiskJobs", totalCount).
		Int("rebuiltIndices", rebuiltCount).
		Msg("Index rebuild completed")

	return nil
}

// addJobToSpoke adds a job to the appropriate spoke (used during rebuild)
func (h *IndexedHub) addJobToSpoke(job *Job, persistToDisk bool) error {
	spoke := h.getSpokeForJob(job)
	if spoke == nil {
		spoke = h.createSpokeForJob(job)
	}

	if persistToDisk {
		// Normal path - store job to disk and add index
		return spoke.AddJobLocked(job)
	} else {
		// Rebuild path - job is already on disk, just add the index
		idx := NewJobIndex(job)
		if err := spoke.AddJobIndexLocked(idx); err != nil {
			return err
		}
		h.jobFilter.Insert([]byte(job.ID()))
		h.stats.IncrJob()
		return nil
	}
}

// getSpokeForJob finds the appropriate spoke for a job
func (h *IndexedHub) getSpokeForJob(job *Job) *IndexedSpoke {
	bound := temporal.NewBound(
		job.TriggerAt().Truncate(h.spokeSpan),
		job.TriggerAt().Truncate(h.spokeSpan).Add(h.spokeSpan),
	)

	return h.spokeMap[bound]
}

// createSpokeForJob creates a new spoke for the job's time range
func (h *IndexedHub) createSpokeForJob(job *Job) *IndexedSpoke {
	start := job.TriggerAt().Truncate(h.spokeSpan)
	end := start.Add(h.spokeSpan)

	spoke := NewIndexedSpoke(start, end, h.diskStore)
	h.spokeMap[spoke.Bound] = spoke
	heap.Push(h.spokes, spoke.AsPriorityItem())

	return spoke
}
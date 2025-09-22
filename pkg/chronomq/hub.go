package chronomq

import (
	"container/heap"
	"context"
	"fmt"
	"runtime"
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

const (
	// DefaultMaxCFSize is the default size for the cuckoo filter
	DefaultMaxCFSize = 1000000
	// TestMaxCFSize is a smaller size for testing
	TestMaxCFSize = 10000
)

var (
	// hundredYears represents a very long time duration
	hundredYears = 365 * 24 * time.Hour * 100
)

// HubOpts define customizations for Hub initialization
type HubOpts struct {
	AttemptRestore bool                      // If true, hub will try to restore from disk on start
	SpokeSpan      time.Duration             // How wide should the spokes be
	DiskStore      persistence.DiskJobStoreInterface  // Disk storage for jobs
	MaxCFSize      uint                      // Max size of the Cuckoo Filter
	SnapshotDir    string                    // Directory to store index snapshots
	SnapshotInterval time.Duration           // How often to create snapshots (0 = disabled)
	MaxSnapshots   int                       // Maximum number of snapshots to keep (0 = unlimited)
}

// Hub is a memory-efficient hub that stores only job indices in memory while persisting full jobs to disk
type Hub struct {
	jobFilter *cuckoo.Filter
	spokeSpan time.Duration                        // How much time does a spoke span
	spokeMap  map[temporal.Bound]*Spoke     // Quick lookup map for spokes
	spokes    *queue.PriorityQueue         // Actual spokes sorted by time
	diskStore persistence.DiskJobStoreInterface     // Disk storage for full job data

	// Centralized index for fast snapshots and lookups
	jobIndex map[string]*JobIndex            // Master index of all jobs

	pastSpoke    *Spoke // Permanently pinned to the past
	currentSpoke *Spoke // The current spoke - started in the past or now, ends in the future or now

	stats *stats.Counters
	lock  *sync.Mutex

	// Snapshotting configuration
	snapshotDir      string
	snapshotInterval time.Duration
	maxSnapshots     int

	ctx    context.Context
	cancel context.CancelFunc
}

// NewHub creates a new hub where adjacent spokes lie at the given
// spokeSpan duration boundary and jobs are stored on disk with indices in memory
func NewHub(opts *HubOpts) *Hub {
	maxCFSize := uint(TestMaxCFSize) // keep test mem requirements low by default
	if opts.MaxCFSize != 0 {
		maxCFSize = opts.MaxCFSize
	}

	ctx, cancel := context.WithCancel(context.Background())
	h := &Hub{
		jobFilter:        cuckoo.NewFilter(maxCFSize),
		spokeSpan:        opts.SpokeSpan,
		spokeMap:         make(map[temporal.Bound]*Spoke),
		spokes:           &queue.PriorityQueue{},
		diskStore:        opts.DiskStore,
		jobIndex:         make(map[string]*JobIndex), // Initialize centralized index
		pastSpoke:        NewSpoke(time.Now().Add(-1*hundredYears), time.Now().Add(hundredYears), opts.DiskStore),
		currentSpoke:     nil,
		stats:            &stats.Counters{},
		lock:             &sync.Mutex{},
		snapshotDir:      opts.SnapshotDir,
		snapshotInterval: opts.SnapshotInterval,
		maxSnapshots:     opts.MaxSnapshots,
		ctx:              ctx,
		cancel:           cancel,
	}

	// Set finalizer to ensure cleanup if Stop() is not called
	runtime.SetFinalizer(h, (*Hub).finalizeHub)
	heap.Init(h.spokes)

	log.Info().Dur("spokeSpan", opts.SpokeSpan).
		Bool("attemptRestore", opts.AttemptRestore).
		Uint("maxCFSize", maxCFSize).
		Msg("Created hub")

	go func() {
		if opts.AttemptRestore {
			log.Info().Msg("Hub: Entering restore mode")

			var err error
			// Use snapshot-based restore only
			if opts.SnapshotDir != "" {
				err = h.RebuildIndicesFromSnapshot(opts.SnapshotDir)
				if err != nil {
					log.Error().Err(err).Msg("Hub: Snapshot restore failed - no fallback available")
				}
			} else {
				log.Warn().Msg("Hub: Restore requested but no snapshot directory configured")
			}

			if err != nil {
				log.Error().Err(err).Msg("Hub: Restore error")
			}
			log.Info().Msg("Hub: Initial restore finished. Resuming")
		}
	}()
	go h.StatusPrinter()

	// Start snapshot routine if enabled
	if opts.SnapshotInterval > 0 && opts.SnapshotDir != "" {
		go h.SnapshotRoutine()
	}

	return h
}

// Stop the hub gracefully
func (h *Hub) Stop(persist bool) {
	// Create final snapshot before shutdown if requested
	if persist && h.snapshotDir != "" {
		log.Info().Msg("Creating final snapshot before shutdown...")
		start := time.Now()
		if err := h.SaveSnapshot(h.snapshotDir); err != nil {
			log.Error().Err(err).Msg("Failed to create final snapshot")
		} else {
			log.Info().
				Dur("duration", time.Since(start)).
				Msg("Final snapshot created successfully")

			// Clean up excess snapshots after creating final snapshot
			if err := h.CleanupExcessSnapshots(h.snapshotDir, h.maxSnapshots); err != nil {
				log.Warn().Err(err).Msg("Failed to cleanup excess snapshots after final snapshot")
			}
		}
	}

	// Cancel context to stop goroutines
	if h.cancel != nil {
		h.cancel()
	}

	// Close disk store
	if err := h.diskStore.Close(); err != nil {
		log.Error().Err(err).Msg("Hub:Stop Error closing disk store")
	}

	// Clear finalizer since we're cleaning up manually
	runtime.SetFinalizer(h, nil)

	log.Info().Msg("Hub:Stop stopped")
}

// finalizeHub is called by the garbage collector if Stop() was not called
func (h *Hub) finalizeHub() {
	if h.cancel != nil {
		h.cancel()
	}
}

// Stats returns a snapshot of the current hub's stats
func (h *Hub) Stats() stats.Snapshot {
	return h.stats.Read()
}

// DiskStore returns the hub's disk store for external access
func (h *Hub) DiskStore() persistence.DiskJobStoreInterface {
	return h.diskStore
}

// CancelJobLocked cancels a job if found. Calls are noop for unknown jobs
// Uses fast cancel by default (no job body loading) for better performance
func (h *Hub) CancelJobLocked(jobID string) (*Job, error) {
	// Fast cancel by default - if job body is needed, use CancelJobWithBodyLocked
	err := h.CancelJobFastLocked(jobID)
	return nil, err
}

// CancelJobWithBodyLocked cancels a job and returns the job body (slower due to disk I/O)
// Use this when you need access to the canceled job's data
func (h *Hub) CancelJobWithBodyLocked(jobID string) (*Job, error) {
	go metrics.Incr("hub.cancel.req")
	id := []byte(jobID)

	h.lock.Lock()
	defer h.lock.Unlock()

	if !h.jobFilter.Lookup(id) {
		// no such job (filter can have false positives but not false negatives)
		go func() {
			metrics.Incr("hub.cancel.ok")
			metrics.Incr("hub.cancel.notfound")
		}()
		return nil, nil
	}

	j, err := h.cancelJobWithBody(jobID)
	if err == nil {
		// delete from hub was successful
		// remove from cuckoo filter (might not be present but we don't care either way)
		filterDeleted := h.jobFilter.Delete(id)
		go func() {
			metrics.Incr("hub.cancel.ok")
			if !filterDeleted {
				metrics.Incr("hub.cancel.should_be_unreachable")
			}
		}()
	} else {
		// job not found
		go func() {
			metrics.Incr("hub.cancel.notfound")
		}()
	}

	return j, err
}

// CancelJobFastLocked cancels a job without loading its body (for better performance)
// Returns only an error indicating success/failure, not the canceled job
func (h *Hub) CancelJobFastLocked(jobID string) error {
	id := []byte(jobID)

	h.lock.Lock()
	defer h.lock.Unlock()

	// Batch metrics updates into a single goroutine
	defer func() {
		go func() {
			metrics.Incr("hub.cancel.req")
		}()
	}()

	if !h.jobFilter.Lookup(id) {
		// no such job (filter can have false positives but not false negatives)
		go func() {
			metrics.Incr("hub.cancel.ok")
			metrics.Incr("hub.cancel.notfound")
		}()
		return nil
	}

	err := h.cancelJobFast(jobID)
	if err == nil {
		// delete from hub was successful
		// remove from cuckoo filter (might not be present but we don't care either way)
		filterDeleted := h.jobFilter.Delete(id)
		go func() {
			metrics.Incr("hub.cancel.ok")
			if !filterDeleted {
				metrics.Incr("hub.cancel.should_be_unreachable")
			}
		}()
	} else {
		// job not found
		go func() {
			metrics.Incr("hub.cancel.notfound")
		}()
	}

	return err
}

func (h *Hub) cancelJob(jobID string) (*Job, error) {
	log.Debug().Str("jobID", jobID).Msg("canceling job in hub")

	s, err := h.findOwnerSpokeForCancel(jobID)
	if err != nil {
		log.Debug().Str("jobID", jobID).Msg("cancel found no owner spoke")
		return nil, nil
	}

	log.Debug().Str("jobID", jobID).Msg("cancel found owner spoke")
	j, err := s.CancelJobLocked(jobID)
	if err == nil {
		h.stats.DecrJob()
		go metrics.Incr("hub.cancel.ok")
	}
	return j, err
}

// cancelJobFast cancels a job without loading its body for better performance
func (h *Hub) cancelJobFast(jobID string) error {
	log.Debug().Str("jobID", jobID).Msg("fast canceling job in hub")

	s, err := h.findOwnerSpokeForCancel(jobID)
	if err != nil {
		log.Debug().Str("jobID", jobID).Msg("cancel found no owner spoke")
		return nil
	}

	log.Debug().Str("jobID", jobID).Msg("cancel found owner spoke")
	err = s.CancelJobFastLocked(jobID)
	if err == nil {
		h.stats.DecrJob()
		// Note: metrics handled by caller to avoid duplicate goroutine creation
	}
	return err
}

// cancelJobWithBody cancels a job and returns the job body (loads from disk)
func (h *Hub) cancelJobWithBody(jobID string) (*Job, error) {
	log.Debug().Str("jobID", jobID).Msg("canceling job with body in hub")

	s, err := h.findOwnerSpokeForCancel(jobID)
	if err != nil {
		log.Debug().Str("jobID", jobID).Msg("cancel found no owner spoke")
		return nil, nil
	}

	log.Debug().Str("jobID", jobID).Msg("cancel found owner spoke")
	j, err := s.CancelJobWithBodyLocked(jobID)
	if err == nil {
		h.stats.DecrJob()
		// Note: metrics handled by caller to avoid duplicate goroutine creation
	}
	return j, err
}

// findOwnerSpoke returns the spoke that owns this job
func (h *Hub) findOwnerSpoke(jobID string) (*Spoke, error) {
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

// findOwnerSpokeForCancel returns the spoke that owns this job using read locks for better performance
func (h *Hub) findOwnerSpokeForCancel(jobID string) (*Spoke, error) {
	// Check past spoke first (most common case for cancellations)
	if h.pastSpoke.OwnsJob(jobID) {
		return h.pastSpoke, nil
	}

	// Check current spoke
	if h.currentSpoke != nil && h.currentSpoke.OwnsJob(jobID) {
		return h.currentSpoke, nil
	}

	// Search through future spokes (using read locks for faster scanning)
	for _, s := range h.spokeMap {
		if s.OwnsJob(jobID) {
			return s, nil
		}
	}
	return nil, errors.New("Cannot find job owner spoke")
}

// addSpoke adds spoke s to this hub
func (h *Hub) addSpoke(s *Spoke) {
	defer h.stats.IncrSpoke()
	h.spokeMap[s.Bound] = s
	heap.Push(h.spokes, s.AsPriorityItem())
}

// deleteSpokeFromMap removes a spoke from the map
func (h *Hub) deleteSpokeFromMap(s *Spoke) {
	defer h.stats.DecrSpoke()
	delete(h.spokeMap, s.Bound)
}

// NextLocked returns the next job that is ready now or returns nil.
// This will load the job from disk when needed.
func (h *Hub) NextLocked() (*Job, error) {
	defer metrics.Time("hub.next.search.duration", time.Now())

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

func (h *Hub) next() (*Job, error) {
	// since we have the lock, send some metrics
	go metrics.GaugeInt("hub.job.count", int(h.stats.Read().CurrentJobs))
	go metrics.GaugeInt("hub.spoke.count", h.spokes.Len())

	// Check past spoke first
	if j, err := func() (*Job, error) {
		go metrics.GaugeInt("hub.job.past.count", h.pastSpoke.PendingJobsLen())

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
		h.currentSpoke = func() *Spoke {
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
		current := item.Value().(*Spoke)
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
		log.Panic().Msg("Unreachable state :: hub has a nil spoke after candidate search")
	}

	go metrics.GaugeInt("hub.job.current.count", h.currentSpoke.PendingJobsLen())

	j, err := h.currentSpoke.NextLocked()
	if err != nil {
		return nil, fmt.Errorf("error retrieving job from current spoke: %w", err)
	}

	if j == nil {
		log.Debug().Msg("No job in current spoke")
		return nil, nil
	}

	log.Debug().Str("jobID", j.ID()).Msg("returning next job from hub")
	h.stats.DecrJob()
	return j, nil
}

// AddJobLocked to this hub. Hub should never reject a job - this method will panic if that happens
func (h *Hub) AddJobLocked(j *Job) error {
	defer metrics.Time("hub.job.add.duration", time.Now())
	go metrics.GaugeInt("hub.job.size", len(j.Body()))

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
		go metrics.Incr("hub.addjob")

		// Add to centralized index after successful add
		h.updateCentralizedIndex(j)
	}
	return err
}

// updateCentralizedIndex adds or updates a job in the centralized index
func (h *Hub) updateCentralizedIndex(j *Job) {
	// Find the job index from the spokes
	spoke, err := h.findOwnerSpoke(j.ID())
	if err != nil || spoke == nil {
		return
	}

	// Get JobIndex from spoke
	if idx := spoke.GetJobIndex(j.ID()); idx != nil {
		h.jobIndex[j.ID()] = idx
	}
}

func (h *Hub) addJob(j *Job) error {
	switch j.AsTemporalState() {
	case temporal.Past:
		log.Debug().Str("jobID", j.ID()).Msg("Adding job to past spoke")
		err := h.pastSpoke.AddJobLocked(j)
		if err != nil {
			log.Error().Err(err).Msg("Past spoke rejected job. This should never happen")
			return err
		}
		go metrics.Incr("hub.addjob.past")
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
		log.Debug().Str("jobID", j.ID()).Msg("Adding job to a new spoke")
		s := NewSpoke(jobBound.Start(), jobBound.End(), h.diskStore)
		err := s.AddJobLocked(j)
		if err != nil {
			log.Error().Err(err).Msg("Hub should always accept a job. No spoke accepted. This should never happen")
			return err
		}
		h.addSpoke(s)
		return nil
	}

	err := errors.Errorf("Unable to find a spoke for job. This should never happen")
	log.Error().Err(err).Msg("Can't add job to hub")
	return err
}

// StatusLocked prints the state of the spokes of this hub
func (h *Hub) StatusLocked() {
	log.Info().Msg("----------------------Hub Stats------------------------")

	hubStats := h.stats.Read()
	log.Info().Int64("spokesCount", hubStats.CurrentSpokes).Send()
	go metrics.GaugeInt("hub.spoke.count", int(hubStats.CurrentSpokes))

	log.Info().Int64("pendingJobsCount", hubStats.CurrentJobs).Send()
	go metrics.GaugeInt("hub.job.count", int(hubStats.CurrentJobs))

	log.Info().Int64("removedJobsCount", hubStats.RemovedJobs).Send()
	go metrics.GaugeInt("hub.job.removed.count", int(hubStats.RemovedJobs))

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
	go metrics.GaugeInt("hub.memory.footprint", memoryFootprint)

	// lock only for this bit - current spoke can be replaced while running...
	h.lock.Lock()
	defer h.lock.Unlock()
	log.Info().Int("pastSpokePendingJobsCount", h.pastSpoke.PendingJobsLen()).Send()
	log.Info().Uint("jobFilterCount", h.jobFilter.Count()).Send()
	go metrics.GaugeInt("hub.job.past.count", h.pastSpoke.PendingJobsLen())

	if h.currentSpoke != nil {
		log.Info().Int("currentSpokePendingJobsCount", h.currentSpoke.PendingJobsLen()).Send()
		go metrics.GaugeInt("hub.job.current.count", h.currentSpoke.PendingJobsLen())
	}
	log.Info().Msg("-------------------------------------------------------------")
}

// StatusPrinter starts a status printer that prints hub stats over some time interval
func (h *Hub) StatusPrinter() {
	t := time.NewTicker(time.Second * 10)
	defer t.Stop()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-t.C:
			h.StatusLocked()
		}
	}
}

// SnapshotRoutine periodically creates index snapshots for fast restoration
func (h *Hub) SnapshotRoutine() {
	if h.snapshotInterval <= 0 || h.snapshotDir == "" {
		return
	}

	log.Info().
		Dur("interval", h.snapshotInterval).
		Str("snapshotDir", h.snapshotDir).
		Msg("Starting snapshot routine")

	ticker := time.NewTicker(h.snapshotInterval)
	defer ticker.Stop()

	// Cleanup timer for old snapshots (every hour)
	cleanupTicker := time.NewTicker(time.Hour)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			log.Info().Msg("Snapshot routine shutting down")
			return

		case <-ticker.C:
			start := time.Now()
			if err := h.SaveSnapshot(h.snapshotDir); err != nil {
				log.Error().Err(err).Msg("Failed to create snapshot")
			} else {
				log.Info().
					Dur("duration", time.Since(start)).
					Msg("Created index snapshot")

				// Clean up excess snapshots after creating a new one
				if err := h.CleanupExcessSnapshots(h.snapshotDir, h.maxSnapshots); err != nil {
					log.Warn().Err(err).Msg("Failed to cleanup excess snapshots after creation")
				}
			}

		case <-cleanupTicker.C:
			// Clean up excess snapshots (keep only maxSnapshots)
			if err := h.CleanupExcessSnapshots(h.snapshotDir, h.maxSnapshots); err != nil {
				log.Warn().Err(err).Msg("Failed to cleanup excess snapshots")
			}
		}
	}
}


// GetNJobs returns up to N job indices (not full jobs) for inspection
func (h *Hub) GetNJobIndices(n int) chan *JobIndex {
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


// RebuildIndicesFromSnapshot loads indices from a snapshot and replays delta changes
// This is much faster than full rebuild for large datasets
func (h *Hub) RebuildIndicesFromSnapshot(snapshotDir string) error {
	log.Info().Msg("Starting index rebuild from snapshot...")

	// Try to load the latest snapshot
	snapshot, err := h.LoadLatestSnapshot(snapshotDir)
	if err != nil {
		return fmt.Errorf("failed to load snapshot: %w", err)
	}

	// Restore indices from snapshot
	if err := h.RestoreFromSnapshot(snapshot); err != nil {
		return fmt.Errorf("failed to restore from snapshot: %w", err)
	}

	log.Info().
		Time("snapshotTime", snapshot.Timestamp).
		Int("restoredIndices", len(snapshot.JobIndices)).
		Msg("Restored indices from snapshot")

	// Replay delta changes since snapshot
	deltaCount, err := h.replayDeltaChanges(snapshot.Timestamp)
	if err != nil {
		log.Error().Err(err).Msg("Failed to replay delta changes")
		return err
	}

	log.Info().
		Int("deltaJobs", deltaCount).
		Dur("snapshotAge", time.Since(snapshot.Timestamp)).
		Msg("Index rebuild from snapshot completed")

	return nil
}

// replayDeltaChanges processes jobs that were added/modified since the snapshot time
func (h *Hub) replayDeltaChanges(since time.Time) (int, error) {
	// Check if disk store supports time-based walking
	diskStore, ok := h.diskStore.(interface {
		WalkJobFilesSince(time.Time, func(string) error) error
	})
	if !ok {
		log.Warn().Msg("Disk store does not support time-based iteration, skipping delta replay")
		return 0, nil
	}

	deltaCount := 0

	// Process only jobs modified since snapshot time
	err := diskStore.WalkJobFilesSince(since, func(jobKey string) error {
		// Load job from disk to create index
		jobData, err := h.diskStore.RetrieveJob(jobKey)
		if err != nil {
			log.Warn().Err(err).Str("jobKey", jobKey).Msg("Failed to retrieve job during delta replay")
			return nil // Continue with next job
		}

		// Decode job data
		data, ok := jobData.([]byte)
		if !ok {
			log.Warn().Str("jobKey", jobKey).Msg("Job data is not byte array during delta replay")
			return nil // Continue with next job
		}

		job := &Job{}
		if err := job.GobDecode(data); err != nil {
			log.Warn().Err(err).Str("jobKey", jobKey).Msg("Failed to decode job during delta replay")
			return nil // Continue with next job
		}

		h.lock.Lock()

		// Check if job already exists (might be in snapshot)
		id := []byte(job.ID())
		if h.jobFilter.Lookup(id) {
			// Job might already exist from snapshot, skip to avoid duplicates
			h.lock.Unlock()
			return nil
		}

		// Add new job index
		if err := h.addJobToSpoke(job, jobKey, false); err != nil {
			log.Warn().Err(err).Str("jobID", job.ID()).Msg("Failed to add job to spoke during delta replay")
		} else {
			deltaCount++
		}

		h.lock.Unlock()
		return nil
	})

	return deltaCount, err
}

// addJobToSpoke adds a job to the appropriate spoke (used during rebuild)
func (h *Hub) addJobToSpoke(job *Job, diskKey string, persistToDisk bool) error {
	spoke := h.getSpokeForJob(job)
	if spoke == nil {
		spoke = h.createSpokeForJob(job)
	}

	if persistToDisk {
		// Normal path - store job to disk and add index
		return spoke.AddJobLocked(job)
	} else {
		// Rebuild path - job is already on disk, just add the index
		idx := NewJobIndex(job, diskKey)
		if err := spoke.AddJobIndexLocked(idx); err != nil {
			return err
		}
		h.jobFilter.Insert([]byte(job.ID()))
		h.stats.IncrJob()
		return nil
	}
}

// getSpokeForJob finds the appropriate spoke for a job
func (h *Hub) getSpokeForJob(job *Job) *Spoke {
	bound := temporal.NewBound(
		job.TriggerAt().Truncate(h.spokeSpan),
		job.TriggerAt().Truncate(h.spokeSpan).Add(h.spokeSpan),
	)

	return h.spokeMap[bound]
}

// createSpokeForJob creates a new spoke for the job's time range
func (h *Hub) createSpokeForJob(job *Job) *Spoke {
	start := job.TriggerAt().Truncate(h.spokeSpan)
	end := start.Add(h.spokeSpan)

	spoke := NewSpoke(start, end, h.diskStore)
	h.spokeMap[spoke.Bound] = spoke
	heap.Push(h.spokes, spoke.AsPriorityItem())

	return spoke
}
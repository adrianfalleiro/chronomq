package chronomq

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	uuid "github.com/satori/go.uuid"

	"github.com/chronomq/chronomq/internal/queue"
	"github.com/chronomq/chronomq/internal/temporal"
	"github.com/chronomq/chronomq/pkg/persistence"
)

// Spoke is a memory-efficient spoke that stores only job indices in memory
// while keeping the full job data on disk
type Spoke struct {
	id uuid.UUID
	temporal.Bound
	indexMap   map[string]*queue.Item // Maps job ID to queue item containing JobIndex
	indexQueue queue.PriorityQueue    // Orders the job indices by trigger priority
	diskStore  persistence.DiskJobStoreInterface

	lock *sync.RWMutex
}

// ErrJobIndexOutOfSpokeBounds is returned when an attempt was made to add a job to a spoke that
// should not contain it - the job's trigger time is outside the spoke bounds
var ErrJobIndexOutOfSpokeBounds = errors.New("The offered job is outside the bounds of this spoke")

// NewSpoke creates a new spoke
func NewSpoke(start, end time.Time, diskStore persistence.DiskJobStoreInterface) *Spoke {
	iq := queue.PriorityQueue{}
	heap.Init(&iq)
	return &Spoke{
		id:         uuid.NewV4(),
		indexMap:   make(map[string]*queue.Item),
		indexQueue: iq,
		diskStore:  diskStore,
		Bound:      temporal.NewBound(start, end),
		lock:       &sync.RWMutex{},
	}
}

// IsJobInBounds returns true if this job's trigger time is temporally bounded by this spoke
func (s *Spoke) IsJobInBounds(j *Job) bool {
	return s.ContainsTime(j.TriggerAt())
}

// IsJobIndexInBounds returns true if this job index's trigger time is temporally bounded by this spoke
func (s *Spoke) IsJobIndexInBounds(idx *JobIndex) bool {
	return s.ContainsTime(idx.TriggerAt())
}

// JobIndexAtIdx returns the job index at position i
func (s *Spoke) JobIndexAtIdx(i int) *JobIndex {
	return s.indexQueue.AtIdx(i).Value().(*JobIndex)
}

// GetJobAtIdx retrieves the full job at position i from disk
func (s *Spoke) GetJobAtIdx(i int) (*Job, error) {
	idx := s.JobIndexAtIdx(i)
	return s.retrieveJobFromDisk(idx.DiskKey())
}

// GetLocker returns the spoke as a sync.Locker interface
func (s *Spoke) GetLocker() sync.Locker {
	return s
}

// Lock this spoke
func (s *Spoke) Lock() {
	s.lock.Lock()
}

// Unlock this spoke
func (s *Spoke) Unlock() {
	s.lock.Unlock()
}

// AsTemporalState returns the spoke's temporal classification at the point in time
func (s *Spoke) AsTemporalState() temporal.State {
	switch {
	case s.IsExpired():
		return temporal.Past
	case s.IsStarted():
		return temporal.Current
	default:
		return temporal.Future
	}
}

// AddJobLocked stores the job on disk and adds its index to the spoke
func (s *Spoke) AddJobLocked(j *Job) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.IsJobInBounds(j) {
		return ErrJobIndexOutOfSpokeBounds
	}

	// Store job on disk first (returns disk key)
	diskKey, err := s.diskStore.StoreJob(j)
	if err != nil {
		return fmt.Errorf("failed to store job on disk: %w", err)
	}

	// Create index with the disk key and add to memory structures
	idx := NewJobIndex(j, diskKey)
	item := idx.AsPriorityItem()
	s.indexMap[idx.ID()] = item
	heap.Push(&s.indexQueue, item)

	return nil
}

// AddJobIndexLocked adds a job index to the spoke without persisting to disk (used during rebuild)
func (s *Spoke) AddJobIndexLocked(idx *JobIndex) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.IsJobIndexInBounds(idx) {
		return ErrJobIndexOutOfSpokeBounds
	}

	// Add index to memory structures (job is already on disk)
	item := idx.AsPriorityItem()
	s.indexMap[idx.ID()] = item
	heap.Push(&s.indexQueue, item)

	return nil
}

// NextLocked returns the next ready job, loading it from disk
func (s *Spoke) NextLocked() (*Job, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.indexQueue.Len() == 0 {
		return nil, nil
	}

	// Peek at the first index
	item := s.indexQueue.AtIdx(0)
	if item == nil {
		return nil, nil
	}

	idx := item.Value().(*JobIndex)
	switch idx.AsTemporalState() {
	case temporal.Past, temporal.Current:
		// Remove index from memory structures
		delete(s.indexMap, idx.ID())
		heap.Pop(&s.indexQueue)

		// Load job from disk
		job, err := s.retrieveJobFromDisk(idx.DiskKey())
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve job from disk: %w", err)
		}

		// Clean up disk storage
		if err := s.diskStore.DeleteJob(idx.DiskKey()); err != nil {
			log.Warn().Err(err).Str("diskKey", idx.DiskKey()).Msg("Failed to delete job from disk store")
		}

		return job, nil
	default:
		return nil, nil
	}
}

// CancelJobLocked removes a job index and deletes the job from disk
// Uses fast cancel by default (no job body loading) for better performance
func (s *Spoke) CancelJobLocked(id string) (*Job, error) {
	// Fast cancel by default - if job body is needed, use CancelJobWithBodyLocked
	err := s.CancelJobFastLocked(id)
	return nil, err
}

// CancelJobFastLocked removes a job index and deletes from disk without loading job body
func (s *Spoke) CancelJobFastLocked(id string) error {
	_, err := s.cancelJobLocked(id, false)
	return err
}

// CancelJobWithBodyLocked removes a job index, deletes from disk, and returns the job body
// Use this when you need access to the canceled job's data (slower due to disk I/O)
func (s *Spoke) CancelJobWithBodyLocked(id string) (*Job, error) {
	return s.cancelJobLocked(id, true)
}

// cancelJobLocked is the internal implementation with optional job loading
func (s *Spoke) cancelJobLocked(id string, loadJob bool) (*Job, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if item, ok := s.indexMap[id]; ok {
		idx := item.Value().(*JobIndex)

		// Remove from memory structures
		delete(s.indexMap, id)
		heap.Remove(&s.indexQueue, item.Index())

		var job *Job
		if loadJob {
			// Load job from disk before deleting it
			var err error
			job, err = s.retrieveJobFromDisk(idx.DiskKey())
			if err != nil {
				log.Warn().Err(err).Str("diskKey", idx.DiskKey()).Msg("Failed to retrieve job during cancellation")
			}
		}

		// Delete from disk
		if err := s.diskStore.DeleteJob(idx.DiskKey()); err != nil {
			log.Warn().Err(err).Str("diskKey", idx.DiskKey()).Msg("Failed to delete job from disk store")
		}

		return job, nil
	}

	return nil, fmt.Errorf("Cannot find job")
}

// OwnsJobLocked returns true if a job by given id is owned by this spoke
func (s *Spoke) OwnsJobLocked(id string) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, ok := s.indexMap[id]
	return ok
}

// OwnsJob returns true if a job by given id is owned by this spoke (without locking)
// This is used for fast ownership checks during cancel operations
func (s *Spoke) OwnsJob(id string) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	_, ok := s.indexMap[id]
	return ok
}

// PendingJobsLen returns the number of job indices in this spoke
func (s *Spoke) PendingJobsLen() int {
	return s.indexQueue.Len()
}

// ID returns the id of this spoke
func (s *Spoke) ID() uuid.UUID {
	return s.id
}

// AsPriorityItem returns a spoke as a prioritizable Item
func (s *Spoke) AsPriorityItem() *queue.Item {
	return queue.NewItem(s, s.Start())
}

// retrieveJobFromDisk loads a job from disk storage
func (s *Spoke) retrieveJobFromDisk(diskKey string) (*Job, error) {
	dataInterface, err := s.diskStore.RetrieveJob(diskKey)
	if err != nil {
		return nil, err
	}

	// The disk store returns raw []byte data
	data, ok := dataInterface.([]byte)
	if !ok {
		return nil, fmt.Errorf("retrieved object is not byte data")
	}

	// Decode the data into a Job
	job := &Job{}
	if err := job.GobDecode(data); err != nil {
		return nil, fmt.Errorf("failed to decode job from disk data: %w", err)
	}

	return job, nil
}

// MemoryFootprint returns the estimated memory usage of this spoke
func (s *Spoke) MemoryFootprint() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Calculate memory used by indices (much smaller than full jobs)
	totalSize := 0
	for i := 0; i < s.indexQueue.Len(); i++ {
		idx := s.JobIndexAtIdx(i)
		// Approximate memory usage of JobIndex struct + strings
		totalSize += len(idx.ID()) + len(idx.DiskKey()) + 64 // 64 bytes for other fields
	}

	return totalSize
}


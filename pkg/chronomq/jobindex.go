package chronomq

import (
	"fmt"
	"time"

	"github.com/chronomq/chronomq/internal/queue"
	"github.com/chronomq/chronomq/internal/temporal"
)

// JobIndex represents a lightweight reference to a Job stored on disk
type JobIndex struct {
	id        string    // Job ID - also used as disk key
	triggerAt time.Time
	pri       int32
	sizeBytes int       // Size of the job body for memory tracking
}

// NewJobIndex creates a new job index from a full job
func NewJobIndex(j *Job) *JobIndex {
	return &JobIndex{
		id:        j.ID(),
		triggerAt: j.TriggerAt(),
		pri:       j.pri,
		sizeBytes: len(j.Body()),
	}
}

// ID returns the job ID
func (idx *JobIndex) ID() string {
	return idx.id
}

// TriggerAt returns the job's trigger time
func (idx *JobIndex) TriggerAt() time.Time {
	return idx.triggerAt
}

// Priority returns the job priority
func (idx *JobIndex) Priority() int32 {
	return idx.pri
}

// DiskKey returns the key to locate the job on disk (same as ID)
func (idx *JobIndex) DiskKey() string {
	return idx.id
}

// SizeBytes returns the size of the job body in bytes
func (idx *JobIndex) SizeBytes() int {
	return idx.sizeBytes
}

// AsTemporalState returns the job index's temporal classification
func (idx *JobIndex) AsTemporalState() temporal.State {
	now := time.Now()
	switch {
	case now.After(idx.triggerAt):
		return temporal.Past
	case idx.triggerAt.After(now):
		return temporal.Future
	default:
		return temporal.Past
	}
}

// AsBound returns temporal.Bound for a hypothetical spoke that should hold this job
func (idx *JobIndex) AsBound(spokeSpan time.Duration) temporal.Bound {
	start := idx.triggerAt.Truncate(spokeSpan)
	end := start.Add(spokeSpan)
	return temporal.NewBound(start, end)
}

// AsPriorityItem returns this job index as a prioritizable item
func (idx *JobIndex) AsPriorityItem() *queue.Item {
	return queue.NewItem(idx, idx.triggerAt)
}

// IsReady returns true if job is ready to be worked on
func (idx *JobIndex) IsReady() bool {
	return time.Now().After(idx.triggerAt)
}

// String returns a string representation of the job index
func (idx *JobIndex) String() string {
	return fmt.Sprintf("JobIndex{id: %s, triggerAt: %v, diskKey: %s}", idx.id, idx.triggerAt, idx.id)
}
package chronomq

import (
	"fmt"
	"time"

	"github.com/chronomq/chronomq/internal/queue"
	"github.com/chronomq/chronomq/internal/temporal"
)

// JobIndex represents a lightweight reference to a Job stored on disk
type JobIndex struct {
	JobID     string    `json:"id"`         // Job ID
	JobDiskKey   string    `json:"disk_key"`   // Key used to retrieve job from disk storage
	JobTriggerAt time.Time `json:"trigger_at"`
	JobPri       int32     `json:"priority"`
	JobSizeBytes int       `json:"size_bytes"` // Size of the job body for memory tracking
}

// NewJobIndex creates a new job index from a full job with the specified disk key
func NewJobIndex(j *Job, diskKey string) *JobIndex {
	return &JobIndex{
		JobID:        j.ID(),
		JobDiskKey:   diskKey,
		JobTriggerAt: j.TriggerAt(),
		JobPri:       j.pri,
		JobSizeBytes: len(j.Body()),
	}
}

// ID returns the job ID
func (idx *JobIndex) ID() string {
	return idx.JobID
}

// TriggerAt returns the job's trigger time
func (idx *JobIndex) TriggerAt() time.Time {
	return idx.JobTriggerAt
}

// Priority returns the job priority
func (idx *JobIndex) Priority() int32 {
	return idx.JobPri
}

// DiskKey returns the key to locate the job on disk
func (idx *JobIndex) DiskKey() string {
	return idx.JobDiskKey
}

// SizeBytes returns the size of the job body in bytes
func (idx *JobIndex) SizeBytes() int {
	return idx.JobSizeBytes
}

// AsTemporalState returns the job index's temporal classification
func (idx *JobIndex) AsTemporalState() temporal.State {
	now := time.Now()
	switch {
	case now.After(idx.JobTriggerAt):
		return temporal.Past
	case idx.JobTriggerAt.After(now):
		return temporal.Future
	default:
		return temporal.Current
	}
}

// AsBound returns temporal.Bound for a hypothetical spoke that should hold this job
func (idx *JobIndex) AsBound(spokeSpan time.Duration) temporal.Bound {
	start := idx.JobTriggerAt.Truncate(spokeSpan)
	end := start.Add(spokeSpan)
	return temporal.NewBound(start, end)
}

// AsPriorityItem returns this job index as a prioritizable item
func (idx *JobIndex) AsPriorityItem() *queue.Item {
	return queue.NewItem(idx, idx.JobTriggerAt)
}

// IsReady returns true if job is ready to be worked on
func (idx *JobIndex) IsReady() bool {
	return time.Now().After(idx.JobTriggerAt)
}

// String returns a string representation of the job index
func (idx *JobIndex) String() string {
	return fmt.Sprintf("JobIndex{id: %s, triggerAt: %v, diskKey: %s}", idx.JobID, idx.JobTriggerAt, idx.JobID)
}
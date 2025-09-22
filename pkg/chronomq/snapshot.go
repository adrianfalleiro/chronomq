package chronomq

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/chronomq/chronomq/internal/queue"
	"github.com/chronomq/chronomq/internal/stats"
	"github.com/chronomq/chronomq/internal/temporal"
)

// IndexSnapshot represents a point-in-time snapshot of all job indices
type IndexSnapshot struct {
	Timestamp     time.Time                      `json:"timestamp"`
	JobIndices    map[string]*JobIndex          `json:"job_indices"`
	SpokeState    map[string]*SpokeSnapshot     `json:"spoke_state"` // Using string keys for JSON compatibility
	FilterCount   uint                          `json:"filter_count"`  // Cuckoo filter count
	Stats         stats.Snapshot                `json:"stats"`
	Version       int                           `json:"version"`       // For future compatibility
}

// SpokeSnapshot represents the state of a spoke at snapshot time
type SpokeSnapshot struct {
	StartTime     time.Time `json:"start_time"`
	EndTime       time.Time `json:"end_time"`
	JobCount      int       `json:"job_count"`
	FirstJobTime  time.Time `json:"first_job_time,omitempty"`
	LastJobTime   time.Time `json:"last_job_time,omitempty"`
	MemoryFootprint int     `json:"memory_footprint"`
}

// NewIndexSnapshot creates a new index snapshot from the current hub state
func (h *Hub) NewIndexSnapshot() *IndexSnapshot {
	// Simple freeze of centralized index - just copy references while holding hub lock
	h.lock.Lock()

	// Copy centralized index
	jobIndices := make(map[string]*JobIndex, len(h.jobIndex))
	for k, v := range h.jobIndex {
		jobIndices[k] = v
	}

	filterCount := h.jobFilter.Count()
	stats := h.stats.Read()
	h.lock.Unlock() // Release hub lock immediately - very fast operation

	snapshot := &IndexSnapshot{
		Timestamp:   time.Now(),
		JobIndices:  jobIndices, // Use centralized index
		SpokeState:  make(map[string]*SpokeSnapshot), // Empty for now
		FilterCount: filterCount,
		Stats:       stats,
		Version:     1,
	}

	return snapshot
}

// spokeReference holds a spoke and its key for snapshot collection
type spokeReference struct {
	spoke *Spoke
	key   string
}

// getSpokeReferences quickly collects spoke references while hub is locked
func (h *Hub) getSpokeReferences() []spokeReference {
	refs := make([]spokeReference, 0, len(h.spokeMap)+2)

	// Collect past spoke
	if h.pastSpoke != nil {
		refs = append(refs, spokeReference{h.pastSpoke, "past"})
	}

	// Collect current spoke
	if h.currentSpoke != nil {
		refs = append(refs, spokeReference{h.currentSpoke, "current"})
	}

	// Collect future spokes
	for bound, spoke := range h.spokeMap {
		boundKey := fmt.Sprintf("future_%d_%d", bound.Start().Unix(), bound.End().Unix())
		refs = append(refs, spokeReference{spoke, boundKey})
	}

	return refs
}

// collectJobIndicesFromSpokeRefs collects from spoke references without hub lock
func (h *Hub) collectJobIndicesFromSpokeRefs(spokeRefs []spokeReference, snapshot *IndexSnapshot) {
	for _, ref := range spokeRefs {
		h.collectJobIndicesFromSpoke(ref.spoke, snapshot, ref.key)
	}
}

// collectJobIndicesFromSpokes gathers all job indices from hub spokes
func (h *Hub) collectJobIndicesFromSpokes(snapshot *IndexSnapshot) {
	// Collect from past spoke
	if h.pastSpoke != nil {
		h.collectJobIndicesFromSpoke(h.pastSpoke, snapshot, "past")
	}

	// Collect from current spoke
	if h.currentSpoke != nil {
		h.collectJobIndicesFromSpoke(h.currentSpoke, snapshot, "current")
	}

	// Collect from future spokes in spoke map
	for bound, spoke := range h.spokeMap {
		boundKey := fmt.Sprintf("future_%d_%d", bound.Start().Unix(), bound.End().Unix())
		h.collectJobIndicesFromSpoke(spoke, snapshot, boundKey)
	}
}

// collectJobIndicesFromSpoke collects job indices from a single spoke
func (h *Hub) collectJobIndicesFromSpoke(spoke *Spoke, snapshot *IndexSnapshot, spokeKey string) {
	// Try multiple times with increasing timeout to avoid skipping spokes
	maxRetries := 3
	baseTimeout := 50 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		timeout := baseTimeout * time.Duration(attempt+1) // 50ms, 100ms, 150ms

		done := make(chan bool, 1)
		var spokeData *SpokeSnapshot

		go func() {
			defer func() {
				if r := recover(); r != nil {
					done <- false
				}
			}()

			spoke.Lock()
			defer spoke.Unlock()

			spokeData = &SpokeSnapshot{
				StartTime:       spoke.Bound.Start(),
				EndTime:         spoke.Bound.End(),
				JobCount:        spoke.PendingJobsLen(),
				MemoryFootprint: spoke.MemoryFootprint(),
			}

			// Collect job indices
			jobCount := spoke.PendingJobsLen()
			for i := 0; i < jobCount; i++ {
				jobIndex := spoke.JobIndexAtIdx(i)
				if jobIndex != nil {
					snapshot.JobIndices[jobIndex.ID()] = jobIndex

					// Track first/last job times
					if spokeData.FirstJobTime.IsZero() || jobIndex.TriggerAt().Before(spokeData.FirstJobTime) {
						spokeData.FirstJobTime = jobIndex.TriggerAt()
					}
					if spokeData.LastJobTime.IsZero() || jobIndex.TriggerAt().After(spokeData.LastJobTime) {
						spokeData.LastJobTime = jobIndex.TriggerAt()
					}
				}
			}

			done <- true
		}()

		// Wait with timeout
		select {
		case success := <-done:
			if success && spokeData != nil {
				snapshot.SpokeState[spokeKey] = spokeData
				return // Successfully collected this spoke
			}
		case <-time.After(timeout):
			// Try again with longer timeout
			continue
		}
	}

	// If we get here, all retries failed - log but don't include this spoke
	// This is better than blocking indefinitely
}

// SaveSnapshot saves the index snapshot to disk
func (h *Hub) SaveSnapshot(snapshotDir string) error {
	snapshot := h.NewIndexSnapshot()

	// Ensure snapshot directory exists
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	// Create snapshot filename with timestamp
	filename := fmt.Sprintf("index-snapshot-%s.gob", snapshot.Timestamp.Format("2006-01-02-15-04-05"))
	filePath := filepath.Join(snapshotDir, filename)

	// Save as gob file for efficiency
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file: %w", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(snapshot); err != nil {
		return fmt.Errorf("failed to encode snapshot: %w", err)
	}

	// Create/update symlink to latest snapshot
	latestPath := filepath.Join(snapshotDir, "index-latest.gob")
	os.Remove(latestPath) // Remove existing symlink
	if err := os.Symlink(filename, latestPath); err != nil {
		// Fallback: copy file if symlink fails
		if copyErr := h.copyFile(filePath, latestPath); copyErr != nil {
			return fmt.Errorf("failed to create latest snapshot link: %w", err)
		}
	}

	return nil
}

// LoadLatestSnapshot loads the most recent snapshot from disk
func (h *Hub) LoadLatestSnapshot(snapshotDir string) (*IndexSnapshot, error) {
	latestPath := filepath.Join(snapshotDir, "index-latest.gob")

	file, err := os.Open(latestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open latest snapshot: %w", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	var snapshot IndexSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("failed to decode snapshot: %w", err)
	}

	return &snapshot, nil
}

// RestoreFromSnapshot restores hub state from a snapshot
func (h *Hub) RestoreFromSnapshot(snapshot *IndexSnapshot) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	// Clear current state
	h.spokeMap = make(map[temporal.Bound]*Spoke)
	h.spokes = &queue.PriorityQueue{}
	h.jobIndex = make(map[string]*JobIndex) // Clear centralized index

	// Restore job indices to appropriate spokes
	restoredCount := 0
	for _, jobIndex := range snapshot.JobIndices {
		if err := h.restoreJobIndex(jobIndex); err != nil {
			return fmt.Errorf("failed to restore job index %s: %w", jobIndex.ID(), err)
		}
		restoredCount++

		// Add to job filter
		h.jobFilter.Insert([]byte(jobIndex.ID()))
		// Add to centralized index
		h.jobIndex[jobIndex.ID()] = jobIndex
	}

	// Update stats to reflect restored state
	h.stats.SetJobs(int64(len(snapshot.JobIndices)))

	return nil
}

// restoreJobIndex adds a job index to the appropriate spoke
func (h *Hub) restoreJobIndex(jobIndex *JobIndex) error {
	spoke := h.getSpokeForJobIndex(jobIndex)
	if spoke == nil {
		spoke = h.createSpokeForJobIndex(jobIndex)
	}

	return spoke.AddJobIndexLocked(jobIndex)
}

// getSpokeForJobIndex finds the appropriate spoke for a job index
func (h *Hub) getSpokeForJobIndex(jobIndex *JobIndex) *Spoke {
	// Check if this is a past job (should go to pastSpoke)
	if jobIndex.AsTemporalState() == temporal.Past {
		return h.pastSpoke
	}

	// Find spoke by time bounds
	bound := jobIndex.AsBound(h.spokeSpan)
	return h.spokeMap[bound]
}

// createSpokeForJobIndex creates a new spoke for the job index's time range
func (h *Hub) createSpokeForJobIndex(jobIndex *JobIndex) *Spoke {
	// Past jobs go to pastSpoke
	if jobIndex.AsTemporalState() == temporal.Past {
		return h.pastSpoke
	}

	// Create new spoke for future jobs
	bound := jobIndex.AsBound(h.spokeSpan)
	spoke := NewSpoke(bound.Start(), bound.End(), h.diskStore)
	h.spokeMap[bound] = spoke

	// Don't add to priority queue yet - that will happen when hub processes it

	return spoke
}

// copyFile copies a file from src to dst
func (h *Hub) copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0644)
}

// CleanupOldSnapshots removes snapshots older than the specified duration
func (h *Hub) CleanupOldSnapshots(snapshotDir string, maxAge time.Duration) error {
	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		return err
	}

	cutoff := time.Now().Add(-maxAge)
	for _, entry := range entries {
		if entry.IsDir() || !isSnapshotFile(entry.Name()) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		if info.ModTime().Before(cutoff) {
			filePath := filepath.Join(snapshotDir, entry.Name())
			os.Remove(filePath) // Ignore errors for cleanup
		}
	}

	return nil
}

// isSnapshotFile checks if a filename is a snapshot file
func isSnapshotFile(filename string) bool {
	return filepath.Ext(filename) == ".gob" &&
		   (filename != "index-latest.gob") &&
		   len(filename) > len("index-snapshot-")
}
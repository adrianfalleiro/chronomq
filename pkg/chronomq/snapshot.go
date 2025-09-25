package chronomq

import (
    "encoding/gob"
    "fmt"
    "os"
    "path/filepath"
    "sort"
    "time"

    "github.com/rs/zerolog/log"

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

// (removed) spokeReference and spoke collection helpers

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

// CleanupExcessSnapshots keeps only the N most recent snapshots and removes the rest
func (h *Hub) CleanupExcessSnapshots(snapshotDir string, maxSnapshots int) error {
	if maxSnapshots <= 0 {
		return nil // Unlimited snapshots
	}

	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		return err
	}

	// Collect snapshot files with their modification times
	type snapshotInfo struct {
		name    string
		modTime time.Time
	}

	var snapshots []snapshotInfo
	for _, entry := range entries {
		if entry.IsDir() || !isSnapshotFile(entry.Name()) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		snapshots = append(snapshots, snapshotInfo{
			name:    entry.Name(),
			modTime: info.ModTime(),
		})
	}

	// If we have fewer snapshots than maxSnapshots, nothing to clean up
	if len(snapshots) <= maxSnapshots {
		return nil
	}

	// Sort by modification time (newest first)
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].modTime.After(snapshots[j].modTime)
	})

	// Remove excess snapshots (keep only the first maxSnapshots)
	excessCount := len(snapshots) - maxSnapshots
	for i := maxSnapshots; i < len(snapshots); i++ {
		filePath := filepath.Join(snapshotDir, snapshots[i].name)
		if err := os.Remove(filePath); err != nil {
			log.Warn().Err(err).Str("file", snapshots[i].name).Msg("Failed to remove old snapshot")
		}
	}

	if excessCount > 0 {
		log.Info().
			Int("removed", excessCount).
			Int("kept", maxSnapshots).
			Msg("Cleaned up excess snapshots")
	}

	return nil
}

// isSnapshotFile checks if a filename is a snapshot file
func isSnapshotFile(filename string) bool {
	return filepath.Ext(filename) == ".gob" &&
		   (filename != "index-latest.gob") &&
		   len(filename) > len("index-snapshot-")
}
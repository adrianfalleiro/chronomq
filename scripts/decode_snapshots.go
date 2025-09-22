package main

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/chronomq/chronomq/pkg/chronomq"
)

// SnapshotInfo represents decoded snapshot information for display
type SnapshotInfo struct {
	FilePath      string                     `json:"file_path"`
	FileSize      int64                      `json:"file_size"`
	Timestamp     time.Time                  `json:"timestamp"`
	Version       int                        `json:"version"`
	TotalJobs     int                        `json:"total_jobs"`
	FilterCount   uint                       `json:"filter_count"`
	SpokeCount    int                        `json:"spoke_count"`
	SpokeState    map[string]*SpokeInfo      `json:"spoke_state"`
	JobSummary    *JobSummary                `json:"job_summary"`
	Stats         interface{}                `json:"stats"`
}

type SpokeInfo struct {
	StartTime       time.Time `json:"start_time"`
	EndTime         time.Time `json:"end_time"`
	JobCount        int       `json:"job_count"`
	FirstJobTime    time.Time `json:"first_job_time,omitempty"`
	LastJobTime     time.Time `json:"last_job_time,omitempty"`
	MemoryFootprint int       `json:"memory_footprint"`
	Duration        string    `json:"duration"`
}

type JobSummary struct {
	ReadyJobs    int       `json:"ready_jobs"`
	FutureJobs   int       `json:"future_jobs"`
	EarliestJob  time.Time `json:"earliest_job,omitempty"`
	LatestJob    time.Time `json:"latest_job,omitempty"`
	JobsBySpoke  map[string]int `json:"jobs_by_spoke"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run decode_snapshots.go <snapshot_file_or_directory> [--json] [--details]")
		fmt.Println("  snapshot_file_or_directory: Path to snapshot file or snapshots directory")
		fmt.Println("  --json: Output in JSON format")
		fmt.Println("  --details: Include detailed job information")
		fmt.Println("")
		fmt.Println("Examples:")
		fmt.Println("  go run decode_snapshots.go ./snapshots/")
		fmt.Println("  go run decode_snapshots.go ./snapshots/index-latest.gob --json")
		fmt.Println("  go run decode_snapshots.go ./snapshots/index-snapshot-2025-01-15-10-30-00.gob --details")
		os.Exit(1)
	}

	path := os.Args[1]
	jsonOutput := false
	includeDetails := false

	// Parse flags
	for i := 2; i < len(os.Args); i++ {
		arg := os.Args[i]
		switch arg {
		case "--json":
			jsonOutput = true
		case "--details":
			includeDetails = true
		}
	}

	var snapshots []SnapshotInfo

	// Check if path is a file or directory
	fileInfo, err := os.Stat(path)
	if err != nil {
		fmt.Printf("Error accessing path %s: %v\n", path, err)
		os.Exit(1)
	}

	if fileInfo.IsDir() {
		// Scan directory for snapshot files
		fmt.Printf("Scanning for snapshot files in: %s\n", path)
		err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// Skip directories and non-gob files
			if info.IsDir() || !strings.HasSuffix(info.Name(), ".gob") {
				return nil
			}

			snapshotInfo, err := decodeSnapshotFile(filePath, info.Size(), includeDetails)
			if err != nil {
				fmt.Printf("Warning: Failed to decode %s: %v\n", filePath, err)
				return nil
			}

			snapshots = append(snapshots, *snapshotInfo)
			return nil
		})

		if err != nil {
			fmt.Printf("Error walking directory: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Single file
		snapshotInfo, err := decodeSnapshotFile(path, fileInfo.Size(), includeDetails)
		if err != nil {
			fmt.Printf("Error decoding snapshot file: %v\n", err)
			os.Exit(1)
		}
		snapshots = append(snapshots, *snapshotInfo)
	}

	if len(snapshots) == 0 {
		fmt.Println("No snapshot files found.")
		return
	}

	if jsonOutput {
		output := map[string]interface{}{
			"total_snapshots": len(snapshots),
			"snapshots":       snapshots,
		}
		jsonData, err := json.MarshalIndent(output, "", "  ")
		if err != nil {
			fmt.Printf("Error marshaling to JSON: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(jsonData))
	} else {
		// Text output
		for i, snapshot := range snapshots {
			fmt.Printf("\n=== Snapshot %d ===\n", i+1)
			fmt.Printf("File: %s\n", snapshot.FilePath)
			fmt.Printf("File Size: %d bytes\n", snapshot.FileSize)
			fmt.Printf("Snapshot Timestamp: %s\n", snapshot.Timestamp.Format(time.RFC3339))
			fmt.Printf("Age: %s\n", time.Since(snapshot.Timestamp).String())
			fmt.Printf("Version: %d\n", snapshot.Version)
			fmt.Printf("Total Jobs: %d\n", snapshot.TotalJobs)
			fmt.Printf("Filter Count: %d\n", snapshot.FilterCount)
			fmt.Printf("Spoke Count: %d\n", snapshot.SpokeCount)

			if snapshot.JobSummary != nil {
				fmt.Printf("Ready Jobs: %d\n", snapshot.JobSummary.ReadyJobs)
				fmt.Printf("Future Jobs: %d\n", snapshot.JobSummary.FutureJobs)
				if !snapshot.JobSummary.EarliestJob.IsZero() {
					fmt.Printf("Earliest Job: %s\n", snapshot.JobSummary.EarliestJob.Format(time.RFC3339))
				}
				if !snapshot.JobSummary.LatestJob.IsZero() {
					fmt.Printf("Latest Job: %s\n", snapshot.JobSummary.LatestJob.Format(time.RFC3339))
				}
			}

			if includeDetails && len(snapshot.SpokeState) > 0 {
				fmt.Printf("\n--- Spoke Details ---\n")
				for spokeName, spoke := range snapshot.SpokeState {
					fmt.Printf("Spoke: %s\n", spokeName)
					fmt.Printf("  Duration: %s\n", spoke.Duration)
					fmt.Printf("  Job Count: %d\n", spoke.JobCount)
					fmt.Printf("  Memory Footprint: %d bytes\n", spoke.MemoryFootprint)
					if !spoke.FirstJobTime.IsZero() {
						fmt.Printf("  First Job: %s\n", spoke.FirstJobTime.Format(time.RFC3339))
					}
					if !spoke.LastJobTime.IsZero() {
						fmt.Printf("  Last Job: %s\n", spoke.LastJobTime.Format(time.RFC3339))
					}
					fmt.Println()
				}
			}
		}

		// Summary
		if len(snapshots) > 1 {
			fmt.Printf("\n=== Summary ===\n")
			fmt.Printf("Total snapshots: %d\n", len(snapshots))

			totalJobs := 0
			var latestSnapshot *SnapshotInfo
			for _, snapshot := range snapshots {
				totalJobs += snapshot.TotalJobs
				if latestSnapshot == nil || snapshot.Timestamp.After(latestSnapshot.Timestamp) {
					latestSnapshot = &snapshot
				}
			}

			fmt.Printf("Total jobs across all snapshots: %d\n", totalJobs)
			if latestSnapshot != nil {
				fmt.Printf("Latest snapshot: %s (age: %s)\n",
					latestSnapshot.FilePath, time.Since(latestSnapshot.Timestamp).String())
			}
		}
	}
}

func decodeSnapshotFile(filePath string, fileSize int64, includeDetails bool) (*SnapshotInfo, error) {
	// Read the gob-encoded file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Decode the snapshot
	var snapshot chronomq.IndexSnapshot
	decoder := gob.NewDecoder(strings.NewReader(string(data)))
	if err := decoder.Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("failed to decode gob: %w", err)
	}

	// Convert spoke state for display
	spokeState := make(map[string]*SpokeInfo)
	for key, spoke := range snapshot.SpokeState {
		spokeInfo := &SpokeInfo{
			StartTime:       spoke.StartTime,
			EndTime:         spoke.EndTime,
			JobCount:        spoke.JobCount,
			FirstJobTime:    spoke.FirstJobTime,
			LastJobTime:     spoke.LastJobTime,
			MemoryFootprint: spoke.MemoryFootprint,
			Duration:        spoke.EndTime.Sub(spoke.StartTime).String(),
		}
		spokeState[key] = spokeInfo
	}

	// Analyze jobs if details requested
	var jobSummary *JobSummary
	if includeDetails {
		jobSummary = analyzeJobs(snapshot.JobIndices, spokeState)
	}

	snapshotInfo := &SnapshotInfo{
		FilePath:    filePath,
		FileSize:    fileSize,
		Timestamp:   snapshot.Timestamp,
		Version:     snapshot.Version,
		TotalJobs:   len(snapshot.JobIndices),
		FilterCount: snapshot.FilterCount,
		SpokeCount:  len(snapshot.SpokeState),
		SpokeState:  spokeState,
		JobSummary:  jobSummary,
		Stats:       snapshot.Stats,
	}

	return snapshotInfo, nil
}

func analyzeJobs(jobIndices map[string]*chronomq.JobIndex, spokeState map[string]*SpokeInfo) *JobSummary {
	if len(jobIndices) == 0 {
		return &JobSummary{}
	}

	now := time.Now()
	readyJobs := 0
	futureJobs := 0
	var earliestJob, latestJob time.Time
	jobsBySpoke := make(map[string]int)

	// Count jobs per spoke
	for _, spoke := range spokeState {
		jobsBySpoke[fmt.Sprintf("%s-%s", spoke.StartTime.Format("15:04"), spoke.EndTime.Format("15:04"))] = spoke.JobCount
	}

	for _, jobIndex := range jobIndices {
		triggerTime := jobIndex.TriggerAt()

		if triggerTime.Before(now) || triggerTime.Equal(now) {
			readyJobs++
		} else {
			futureJobs++
		}

		if earliestJob.IsZero() || triggerTime.Before(earliestJob) {
			earliestJob = triggerTime
		}
		if latestJob.IsZero() || triggerTime.After(latestJob) {
			latestJob = triggerTime
		}
	}

	return &JobSummary{
		ReadyJobs:   readyJobs,
		FutureJobs:  futureJobs,
		EarliestJob: earliestJob,
		LatestJob:   latestJob,
		JobsBySpoke: jobsBySpoke,
	}
}
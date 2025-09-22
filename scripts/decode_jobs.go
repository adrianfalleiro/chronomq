package scripts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/chronomq/chronomq/pkg/chronomq"
)

// JobInfo represents decoded job information for display
type JobInfo struct {
	ID        string    `json:"id"`
	TriggerAt time.Time `json:"trigger_at"`
	Body      string    `json:"body"`
	Priority  int32     `json:"priority"`
	TTR       string    `json:"ttr"`
	FilePath  string    `json:"file_path"`
	FileSize  int64     `json:"file_size"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run decode_jobs.go <jobs_directory> [--json] [--limit N]")
		fmt.Println("  jobs_directory: Path to jobs directory (e.g., ./jobs)")
		fmt.Println("  --json: Output in line-delimited JSON (JSONL) format")
		fmt.Println("  --limit N: Limit to first N jobs")
		os.Exit(1)
	}

	jobsDir := os.Args[1]
	jsonOutput := false
	limit := -1

	// Parse flags
	for i := 2; i < len(os.Args); i++ {
		arg := os.Args[i]
		switch arg {
		case "--json":
			jsonOutput = true
		case "--limit":
			if i+1 < len(os.Args) {
				fmt.Sscanf(os.Args[i+1], "%d", &limit)
				i++ // Skip the next argument
			}
		}
	}

	var jobs []JobInfo
	count := 0

	if !jsonOutput {
		fmt.Printf("Scanning for job files in: %s\n", jobsDir)
	}

	err := filepath.Walk(jobsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and non-job files
		if info.IsDir() || !strings.HasSuffix(info.Name(), ".job") {
			return nil
		}

		// Check limit
		if limit > 0 && count >= limit {
			return filepath.SkipDir
		}

		jobInfo, err := decodeJobFile(path, info.Size())
		if err != nil {
			if !jsonOutput {
				fmt.Printf("Warning: Failed to decode %s: %v\n", path, err)
			}
			return nil
		}

		count++

		if jsonOutput {
			// Output line-delimited JSON (JSONL format)
			jsonData, err := json.Marshal(*jobInfo)
			if err != nil {
				if !jsonOutput {
					fmt.Printf("Warning: Failed to marshal job %s: %v\n", jobInfo.ID, err)
				}
				return nil
			}
			fmt.Println(string(jsonData))
		} else {
			jobs = append(jobs, *jobInfo)
			fmt.Printf("\n=== Job %d ===\n", count)
			fmt.Printf("File: %s\n", jobInfo.FilePath)
			fmt.Printf("ID: %s\n", jobInfo.ID)
			fmt.Printf("Trigger At: %s\n", jobInfo.TriggerAt.Format(time.RFC3339))
			fmt.Printf("Time Until Trigger: %s\n", time.Until(jobInfo.TriggerAt).String())
			fmt.Printf("Priority: %d\n", jobInfo.Priority)
			fmt.Printf("TTR: %s\n", jobInfo.TTR)
			fmt.Printf("Body Size: %d bytes\n", len(jobInfo.Body))
			if len(jobInfo.Body) > 0 && len(jobInfo.Body) <= 200 {
				fmt.Printf("Body: %s\n", jobInfo.Body)
			} else if len(jobInfo.Body) > 200 {
				fmt.Printf("Body (first 200 chars): %s...\n", jobInfo.Body[:200])
			}
			fmt.Printf("File Size: %d bytes\n", jobInfo.FileSize)
		}

		return nil
	})

	if err != nil {
		fmt.Printf("Error walking directory: %v\n", err)
		os.Exit(1)
	}

	if !jsonOutput {
		fmt.Printf("\n=== Summary ===\n")
		fmt.Printf("Total jobs found: %d\n", count)

		if count > 0 {
			// Calculate some basic statistics
			var readyCount, futureCount int
			var earliestTrigger, latestTrigger time.Time
			now := time.Now()

			for i, job := range jobs {
				if job.TriggerAt.Before(now) || job.TriggerAt.Equal(now) {
					readyCount++
				} else {
					futureCount++
				}

				if i == 0 || job.TriggerAt.Before(earliestTrigger) {
					earliestTrigger = job.TriggerAt
				}
				if i == 0 || job.TriggerAt.After(latestTrigger) {
					latestTrigger = job.TriggerAt
				}
			}

			fmt.Printf("Jobs ready now: %d\n", readyCount)
			fmt.Printf("Jobs scheduled for future: %d\n", futureCount)
			fmt.Printf("Earliest trigger: %s\n", earliestTrigger.Format(time.RFC3339))
			fmt.Printf("Latest trigger: %s\n", latestTrigger.Format(time.RFC3339))
		}
	}
}

func decodeJobFile(filePath string, fileSize int64) (*JobInfo, error) {
	// Read the gob-encoded file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Create a job and decode the gob data
	var job chronomq.Job
	if err := job.GobDecode(data); err != nil {
		return nil, fmt.Errorf("failed to decode gob: %w", err)
	}

	// Extract readable information
	jobInfo := &JobInfo{
		ID:        job.ID(),
		TriggerAt: job.TriggerAt(),
		Body:      string(job.Body()),
		FilePath:  filePath,
		FileSize:  fileSize,
	}

	return jobInfo, nil
}
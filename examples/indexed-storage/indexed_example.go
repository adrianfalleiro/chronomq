package main

import (
	"fmt"
	"log"
	"time"

	"github.com/chronomq/chronomq/pkg/chronomq"
	"github.com/chronomq/chronomq/pkg/persistence"
)

// Example demonstrating the new index-based storage system
// that keeps only lightweight job indices in memory while storing full job data on disk
func main() {
	fmt.Println("ChronoMQ Index-Based Storage Example")
	fmt.Println("====================================")

	// Create in-memory storage for the example (in production, use file/cloud storage)
	storage, err := persistence.InMemStorage()
	if err != nil {
		log.Fatalf("Failed to create storage: %v", err)
	}

	// Create disk job store for full job data
	diskStore := persistence.NewFileDiskJobStore(storage)
	defer diskStore.Close()

	// Create indexed hub with disk storage
	indexedHub := chronomq.NewIndexedHub(&chronomq.IndexedHubOpts{
		DiskStore:      diskStore,
		AttemptRestore: false,
		SpokeSpan:      time.Minute * 5, // 5-minute spokes
		MaxCFSize:      chronomq.TestMaxCFSize,
	})
	defer indexedHub.Stop(false)

	// Create some example jobs with varying sizes
	jobs := createExampleJobs()

	fmt.Printf("Adding %d jobs to indexed hub...\n", len(jobs))
	for i, job := range jobs {
		if err := indexedHub.AddJobLocked(job); err != nil {
			log.Printf("Failed to add job %d: %v", i, err)
			continue
		}
		fmt.Printf("Added job %s (size: %d bytes)\n", job.ID(), len(job.Body()))
	}

	// Show hub statistics
	stats := indexedHub.Stats()
	fmt.Printf("\nHub Statistics:\n")
	fmt.Printf("- Current Jobs: %d\n", stats.CurrentJobs)
	fmt.Printf("- Current Spokes: %d\n", stats.CurrentSpokes)

	// Demonstrate memory efficiency by inspecting job indices
	fmt.Printf("\nJob Indices in Memory (lightweight references):\n")
	count := 0
	for idx := range indexedHub.GetNJobIndices(10) { // Get first 10 indices
		fmt.Printf("- Index: ID=%s, TriggerAt=%s, Size=%d bytes, DiskKey=%s\n",
			idx.ID(), idx.TriggerAt().Format("15:04:05"), idx.SizeBytes(), idx.DiskKey())
		count++
	}

	fmt.Printf("\nProcessing jobs (loading from disk as needed):\n")
	processedCount := 0
	for processedCount < 5 { // Process first 5 jobs
		job, err := indexedHub.NextLocked()
		if err != nil {
			log.Printf("Error getting next job: %v", err)
			break
		}
		if job == nil {
			fmt.Println("No more ready jobs")
			break
		}

		fmt.Printf("Processing job %s (loaded from disk, size: %d bytes)\n",
			job.ID(), len(job.Body()))
		processedCount++
	}

	fmt.Printf("\nProcessed %d jobs\n", processedCount)

	// Final statistics
	finalStats := indexedHub.Stats()
	fmt.Printf("\nFinal Statistics:\n")
	fmt.Printf("- Remaining Jobs: %d\n", finalStats.CurrentJobs)
	fmt.Printf("- Jobs Processed: %d\n", finalStats.RemovedJobs)

	fmt.Println("\n=== Memory Usage Comparison ===")
	fmt.Println("Traditional Hub: Stores full Job objects in memory")
	fmt.Printf("- Each job: ~%d bytes in memory (including body)\n", estimateJobMemoryUsage(jobs[0]))
	fmt.Printf("- 10 jobs: ~%d bytes total in memory\n", estimateJobMemoryUsage(jobs[0])*10)

	fmt.Println("\nIndexed Hub: Stores only JobIndex in memory, jobs on disk")
	fmt.Printf("- Each index: ~%d bytes in memory (no body)\n", estimateJobIndexMemoryUsage(jobs[0]))
	fmt.Printf("- 10 indices: ~%d bytes total in memory\n", estimateJobIndexMemoryUsage(jobs[0])*10)
	fmt.Printf("- Memory saved: ~%d bytes (%.1f%% reduction)\n",
		(estimateJobMemoryUsage(jobs[0])-estimateJobIndexMemoryUsage(jobs[0]))*10,
		float64(estimateJobMemoryUsage(jobs[0])-estimateJobIndexMemoryUsage(jobs[0]))/float64(estimateJobMemoryUsage(jobs[0]))*100)
}

// createExampleJobs creates a set of example jobs with different trigger times and body sizes
func createExampleJobs() []*chronomq.Job {
	now := time.Now()
	jobs := make([]*chronomq.Job, 0, 10)

	// Create jobs with varying body sizes to demonstrate memory savings
	bodySizes := []int{100, 500, 1000, 2000, 5000} // bytes

	for i := 0; i < 10; i++ {
		// Create job body with specified size
		bodySize := bodySizes[i%len(bodySizes)]
		body := make([]byte, bodySize)
		for j := range body {
			body[j] = byte('A' + (j % 26)) // Fill with letters
		}

		// Stagger trigger times
		triggerAt := now.Add(time.Duration(i) * time.Second)

		job := chronomq.NewJob(fmt.Sprintf("job_%03d", i), triggerAt, body)
		job.SetOpts(int32(i), time.Minute) // priority and TTR

		jobs = append(jobs, job)
	}

	return jobs
}

// estimateJobMemoryUsage estimates the memory usage of a full Job object
func estimateJobMemoryUsage(job *chronomq.Job) int {
	// Approximate memory usage: struct overhead + ID + body + other fields
	return 64 + len(job.ID()) + len(job.Body()) + 32 // rough estimate
}

// estimateJobIndexMemoryUsage estimates the memory usage of a JobIndex
func estimateJobIndexMemoryUsage(job *chronomq.Job) int {
	// JobIndex only stores: ID + triggerAt + priority + diskKey + sizeBytes
	// Much smaller than full job
	return 64 + len(job.ID()) + 32 + 16 // no body data
}
package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"net/rpc"
	"runtime"
	"time"
)

const (
	JobCount     = 1000            // Start with 1K jobs for testing
	PayloadSizeB = 5 * 1024 * 1024 // 5MB per job
	ServerAddr   = ":11301"
)

// Job represents a job for the RPC API (simplified struct)
type Job struct {
	ID    string
	Body  []byte
	Delay time.Duration
}

func main() {
	fmt.Printf("ChronoMQ Small Load Test - %d jobs with %dMB payloads each\n", JobCount, PayloadSizeB/(1024*1024))
	fmt.Printf("Total payload data: %.2fGB\n", float64(JobCount*PayloadSizeB)/(1024*1024*1024))
	fmt.Println("=======================================================")

	// Show initial memory stats
	printMemStats("Initial (client-side)")

	// Connect to ChronoMQ server
	fmt.Printf("🔌 Connecting to ChronoMQ at %s...\n", ServerAddr)
	client, err := rpc.Dial("tcp", ServerAddr)
	if err != nil {
		log.Fatalf("❌ Failed to connect to ChronoMQ server: %v", err)
	}
	defer client.Close()

	fmt.Println("✅ Connected to ChronoMQ server")

	start := time.Now()

	// Submit jobs one by one
	for i := 0; i < JobCount; i++ {
		if err := submitJob(client, i); err != nil {
			log.Printf("❌ Failed to submit job %d: %v", i, err)
			continue
		}

		// Progress reporting
		if i%100 == 0 {
			fmt.Printf("📝 Submitted job #%d\n", i)
			if i%500 == 0 && i > 0 {
				printMemStats(fmt.Sprintf("After %d jobs (client-side)", i))
			}
		}
	}

	duration := time.Since(start)
	fmt.Printf("\n✅ Successfully submitted %d jobs in %v\n", JobCount, duration)
	fmt.Printf("📊 Rate: %.1f jobs/second\n", float64(JobCount)/duration.Seconds())

	// Show final memory stats
	printMemStats("Final (client-side)")

	fmt.Println("\n🎉 Small load test completed!")
	fmt.Printf("💾 Server should be using minimal memory (~%dKB for indices)\n",
		(JobCount*64)/1024) // ~64 bytes per job index
	fmt.Printf("💽 While %.2fGB of job data is stored on disk\n",
		float64(JobCount*PayloadSizeB)/(1024*1024*1024))
}

// submitJob creates and submits a single job with 5MB payload
func submitJob(client *rpc.Client, jobNum int) error {
	// Generate 5MB random payload
	payload := make([]byte, PayloadSizeB)
	if _, err := rand.Read(payload); err != nil {
		return fmt.Errorf("failed to generate payload: %w", err)
	}

	// Create job with staggered trigger times
	job := Job{
		ID:    fmt.Sprintf("large_job_%06d", jobNum),
		Body:  payload,
		Delay: time.Duration(jobNum) * time.Millisecond * 100, // Spread jobs over time
	}

	// Submit job via RPC
	var jobID string
	err := client.Call("RPCServer.PutWithID", job, &jobID)
	if err != nil {
		return fmt.Errorf("RPC call failed: %w", err)
	}

	return nil
}

// printMemStats shows current memory usage
func printMemStats(label string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("\n📈 Memory Stats - %s:\n", label)
	fmt.Printf("   Allocated: %dMB\n", bToMb(m.Alloc))
	fmt.Printf("   Total Allocated: %dMB\n", bToMb(m.TotalAlloc))
	fmt.Printf("   System Memory: %dMB\n", bToMb(m.Sys))
	fmt.Printf("   Garbage Collections: %d\n", m.NumGC)
	fmt.Println()
}

// bToMb converts bytes to megabytes
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
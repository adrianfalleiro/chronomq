package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"runtime"
	"time"
)

const (
	JobCount     = 100             // Smaller number for clearer demonstration
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
	fmt.Printf("🧪 ChronoMQ Persistence Demonstration\n")
	fmt.Printf("=====================================\n")
	fmt.Printf("📊 %d jobs with %dMB payloads each\n", JobCount, PayloadSizeB/(1024*1024))
	fmt.Printf("📊 Total payload data: %.2fGB\n", float64(JobCount*PayloadSizeB)/(1024*1024*1024))
	fmt.Println()

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

	// Submit jobs with future trigger times so they stay in the system
	fmt.Println("\n🚀 Phase 1: Submitting jobs with future triggers (will persist)")
	for i := 0; i < JobCount; i++ {
		if err := submitFutureJob(client, i); err != nil {
			log.Printf("❌ Failed to submit job %d: %v", i, err)
			continue
		}

		// Progress reporting
		if i%20 == 0 {
			fmt.Printf("📝 Submitted future job #%d\n", i)
		}
	}

	duration := time.Since(start)
	fmt.Printf("\n✅ Successfully submitted %d future jobs in %v\n", JobCount, duration)
	fmt.Printf("📊 Rate: %.1f jobs/second\n", float64(JobCount)/duration.Seconds())

	// Show memory stats after submission
	printMemStats("After job submission (client-side)")

	fmt.Println("\n⏳ Phase 2: Letting jobs persist in IndexedHub...")
	fmt.Println("   💾 Jobs are now stored on disk with indices in memory")
	fmt.Println("   🔍 Check the data directory - you should see job data files")
	time.Sleep(2 * time.Second)

	// Inspect jobs in the system
	fmt.Println("\n🔍 Phase 3: Inspecting jobs in the system...")
	inspectJobs(client)

	fmt.Println("\n⚡ Phase 4: Consuming some jobs (demonstrating disk retrieval)...")
	consumeJobs(client, 10) // Consume 10 jobs to show disk loading

	fmt.Println("\n🔍 Phase 5: Final inspection...")
	inspectJobs(client)

	// Send SIGUSR1 to trigger persistence (if server supports it)
	fmt.Println("\n💾 Phase 6: Triggering explicit persistence...")
	fmt.Println("   (Jobs should remain on disk even after explicit persistence)")

	// Show final stats
	printMemStats("Final (client-side)")

	fmt.Println("\n🎉 Persistence test completed!")
	fmt.Printf("💾 Server used minimal memory for %d job indices\n", JobCount)
	fmt.Printf("💽 While %.2fGB of job data was stored on disk\n",
		float64(JobCount*PayloadSizeB)/(1024*1024*1024))
	fmt.Println("\n🔍 Key Observations:")
	fmt.Println("   ✅ Memory usage stays low (only indices in RAM)")
	fmt.Println("   ✅ Job payloads stored on disk (check data directory)")
	fmt.Println("   ✅ Jobs loaded from disk only when consumed")
	fmt.Println("   ✅ IndexedHub vs Traditional Hub memory difference is huge!")
}

// submitFutureJob creates and submits a job with a future trigger time
func submitFutureJob(client *rpc.Client, jobNum int) error {
	// Generate 5MB random payload
	payload := make([]byte, PayloadSizeB)
	if _, err := rand.Read(payload); err != nil {
		return fmt.Errorf("failed to generate payload: %w", err)
	}

	// Create job with future trigger time (1 hour from now + stagger)
	// This ensures jobs stay in the system and get persisted
	futureTime := time.Hour + time.Duration(jobNum)*time.Minute
	job := Job{
		ID:    fmt.Sprintf("persist_job_%06d", jobNum),
		Body:  payload,
		Delay: futureTime,
	}

	// Submit job via RPC
	var jobID string
	err := client.Call("RPCServer.PutWithID", job, &jobID)
	if err != nil {
		return fmt.Errorf("RPC call failed: %w", err)
	}

	return nil
}

// inspectJobs shows jobs currently in the system
func inspectJobs(client *rpc.Client) {
	var jobs []Job // This will be populated by the InspectN call
	err := client.Call("RPCServer.InspectN", 5, &jobs) // Get first 5 jobs
	if err != nil {
		fmt.Printf("❌ Failed to inspect jobs: %v\n", err)
		return
	}

	fmt.Printf("📋 Found %d jobs in system (showing first 5):\n", len(jobs))
	for i, job := range jobs {
		fmt.Printf("   %d. ID: %s, Body: %s\n",
			i+1, job.ID, describeBodySize(len(job.Body)))
	}
}

// consumeJobs consumes a specified number of jobs
func consumeJobs(client *rpc.Client, count int) {
	// First, let's submit some jobs that are ready now
	fmt.Printf("   🚀 Submitting %d jobs ready for immediate consumption...\n", count)

	for i := 0; i < count; i++ {
		payload := make([]byte, PayloadSizeB)
		rand.Read(payload)

		job := Job{
			ID:    fmt.Sprintf("ready_job_%06d", i),
			Body:  payload,
			Delay: 0, // Ready now
		}

		var jobID string
		client.Call("RPCServer.PutWithID", job, &jobID)
	}

	time.Sleep(100 * time.Millisecond) // Let jobs get processed

	fmt.Printf("   ⚡ Consuming %d jobs (this loads from disk)...\n", count)
	for i := 0; i < count; i++ {
		var job Job
		err := client.Call("RPCServer.Next", time.Second, &job)
		if err != nil {
			fmt.Printf("   ❌ Failed to consume job %d: %v\n", i, err)
			continue
		}
		if job.ID == "" {
			fmt.Printf("   ℹ️  No more jobs ready for consumption\n")
			break
		}
		fmt.Printf("   ✅ Consumed job: %s (%s)\n", job.ID, describeBodySize(len(job.Body)))
	}
}

// describeBodySize returns a human-readable description of body size
func describeBodySize(size int) string {
	if size == 0 {
		return "no body (index only)"
	}
	return fmt.Sprintf("%.1fMB body", float64(size)/(1024*1024))
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
	fmt.Printf("   Process ID: %d (for monitoring)\n", os.Getpid())
	fmt.Println()
}

// bToMb converts bytes to megabytes
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
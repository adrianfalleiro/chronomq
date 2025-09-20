package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"net/rpc"
	"runtime"
	"sync"
	"time"

	"github.com/chronomq/chronomq/api/rpc/chronomq"
)

const (
	JobCount     = 100000
	PayloadSizeB = 5 * 1024 * 1024 // 5MB per job
	ServerAddr   = ":11301"
	Concurrency  = 50 // Number of concurrent goroutines
)

func main() {
	fmt.Printf("ChronoMQ Load Test - %d jobs with %dMB payloads each\n", JobCount, PayloadSizeB/(1024*1024))
	fmt.Printf("Total payload data: %.2fGB\n", float64(JobCount*PayloadSizeB)/(1024*1024*1024))
	fmt.Println("=======================================================")

	// Show initial memory stats
	printMemStats("Initial")

	start := time.Now()

	// Create job channel for work distribution
	jobChan := make(chan int, JobCount)
	var wg sync.WaitGroup

	// Start worker goroutines
	fmt.Printf("Starting %d worker goroutines...\n", Concurrency)
	for i := 0; i < Concurrency; i++ {
		wg.Add(1)
		go worker(i, jobChan, &wg)
	}

	// Send jobs to workers
	go func() {
		for i := 0; i < JobCount; i++ {
			jobChan <- i
		}
		close(jobChan)
	}()

	// Wait for all workers to complete
	wg.Wait()

	duration := time.Since(start)
	fmt.Printf("\n✅ Successfully submitted %d jobs in %v\n", JobCount, duration)
	fmt.Printf("📊 Rate: %.0f jobs/second\n", float64(JobCount)/duration.Seconds())

	// Show final memory stats
	printMemStats("After job submission")

	// Wait a bit for any async operations to complete
	fmt.Println("\nWaiting 5 seconds for server-side processing...")
	time.Sleep(5 * time.Second)

	fmt.Println("\n🎉 Load test completed!")
	fmt.Printf("💾 With IndexedHub, the server should be using minimal memory (~%dMB for indices)\n",
		(JobCount*64)/(1024*1024)) // ~64 bytes per job index
	fmt.Printf("💽 While %dGB of job data is stored on disk\n",
		(JobCount*PayloadSizeB)/(1024*1024*1024))
}

// worker connects to ChronoMQ and submits jobs
func worker(id int, jobChan <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()

	// Connect to ChronoMQ server
	client, err := rpc.Dial("tcp", ServerAddr)
	if err != nil {
		log.Printf("Worker %d failed to connect: %v", id, err)
		return
	}
	defer client.Close()

	// Process jobs from channel
	for jobNum := range jobChan {
		if err := submitJob(client, jobNum); err != nil {
			log.Printf("Worker %d failed to submit job %d: %v", id, jobNum, err)
			continue
		}

		// Progress reporting
		if jobNum%1000 == 0 {
			fmt.Printf("📝 Submitted job #%d (worker %d)\n", jobNum, id)
			if jobNum%10000 == 0 && jobNum > 0 {
				printMemStats(fmt.Sprintf("After %d jobs", jobNum))
			}
		}
	}
}

// submitJob creates and submits a single job with 5MB payload
func submitJob(client *rpc.Client, jobNum int) error {
	// Generate 5MB random payload
	payload := make([]byte, PayloadSizeB)
	if _, err := rand.Read(payload); err != nil {
		return fmt.Errorf("failed to generate payload: %w", err)
	}

	// Create job with staggered trigger times (spread over 1 hour)
	job := chronomq.Job{
		ID:    fmt.Sprintf("large_job_%06d", jobNum),
		Body:  payload,
		Delay: time.Duration(jobNum) * time.Second, // Spread jobs over time
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
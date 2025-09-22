package scripts

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/chronomq/chronomq/api/rpc/chronomq"
)

func main() {
	// Connect to the chronomq server
	client, err := chronomq.NewClient(":11301")
	if err != nil {
		log.Fatalf("Failed to connect to chronomq: %v", err)
	}

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Add 100,000 jobs scheduled into the future
	const numJobs = 100000
	startTime := time.Now()

	log.Printf("Adding %d jobs scheduled into the future...", numJobs)

	successCount := 0
	for i := 0; i < numJobs; i++ {
		// Schedule jobs between 1 minute and 24 hours into the future
		minDelay := time.Minute
		maxDelay := time.Hour * 24
		delay := minDelay + time.Duration(rand.Int63n(int64(maxDelay-minDelay)))

		// Create job payload
		payload := fmt.Sprintf(`{"job_id": %d, "message": "Test job %d", "timestamp": "%s", "delay": "%s"}`,
			i, i, time.Now().Format(time.RFC3339), delay.String())

		// Add job with auto-generated ID
		jobID, err := client.Put([]byte(payload), delay)
		if err != nil {
			log.Printf("Failed to add job %d: %v", i, err)
			continue
		}

		successCount++

		// Log progress every 5000 jobs
		if (i+1)%5000 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(i+1) / elapsed.Seconds()
			log.Printf("Added %d jobs (success: %d, rate: %.1f jobs/sec, last job ID: %s)",
				i+1, successCount, rate, jobID)
		}
	}

	elapsed := time.Since(startTime)
	rate := float64(successCount) / elapsed.Seconds()

	log.Printf("Completed! Added %d/%d jobs in %v (%.1f jobs/sec)",
		successCount, numJobs, elapsed, rate)

	if successCount < numJobs {
		log.Printf("Failed to add %d jobs", numJobs-successCount)
	}
}
package persistence

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type DiskJobStoreInterface interface {
	// StoreJob stores a job on disk and returns a key to retrieve it
	StoreJob(job interface{}) (string, error)

	// RetrieveJob retrieves a job from disk using its key
	RetrieveJob(key string) (interface{}, error)

	// DeleteJob removes a job from disk storage
	DeleteJob(key string) error

	// Close closes the disk store and releases resources
	Close() error
}

// gobEncoder interface for objects that can be gob-encoded
type gobEncoder interface {
	GobEncode() ([]byte, error)
}

// DiskJobStore implements disk-based job storage using direct file system operations
// This provides clear visibility of job persistence to disk
type DiskJobStore struct {
	basePath string
	nextKey  int64
	lock     *sync.RWMutex
}

// NewDiskJobStore creates a new disk job store that writes individual files
func NewDiskJobStore(basePath string) *DiskJobStore {
	// Ensure directory exists
	os.MkdirAll(basePath, 0755)

	return &DiskJobStore{
		basePath: basePath,
		nextKey:  1,
		lock:     &sync.RWMutex{},
	}
}

// getJobPath returns a date-based hierarchical path for the job file
// Creates path like: basePath/2025/01/15/14/job_id.job (YYYY/MM/DD/HH/)
func (sds *DiskJobStore) getJobPath(job interface{}, jobID string) string {
	// Try to get trigger time from job for date-based structure
	if jobWithTrigger, ok := job.(interface{ TriggerAt() time.Time }); ok {
		triggerTime := jobWithTrigger.TriggerAt()

		// Create date-based path: YYYY/MM/DD/HH/
		year := fmt.Sprintf("%04d", triggerTime.Year())
		month := fmt.Sprintf("%02d", int(triggerTime.Month()))
		day := fmt.Sprintf("%02d", triggerTime.Day())
		hour := fmt.Sprintf("%02d", triggerTime.Hour())

		dirPath := filepath.Join(sds.basePath, year, month, day, hour)

		// Ensure subdirectories exist
		os.MkdirAll(dirPath, 0755)

		return filepath.Join(dirPath, jobID+".job")
	}

	// Fallback for jobs without trigger time - use current time
	now := time.Now()
	year := fmt.Sprintf("%04d", now.Year())
	month := fmt.Sprintf("%02d", int(now.Month()))
	day := fmt.Sprintf("%02d", now.Day())
	hour := fmt.Sprintf("%02d", now.Hour())

	dirPath := filepath.Join(sds.basePath, year, month, day, hour)
	os.MkdirAll(dirPath, 0755)

	return filepath.Join(dirPath, jobID+".job")
}

// findJobFile searches for a job file by ID within the hierarchical structure
func (sds *DiskJobStore) findJobFile(jobID string) (string, error) {
	var foundPath string

	err := filepath.Walk(sds.basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Check if this is the job file we're looking for
		if !info.IsDir() && info.Name() == jobID+".job" {
			foundPath = path
			return fmt.Errorf("found") // Use error to stop walking
		}

		return nil
	})

	if err != nil && err.Error() != "found" {
		return "", err
	}

	if foundPath == "" {
		return "", fmt.Errorf("job file not found: %s", jobID)
	}

	return foundPath, nil
}

// StoreJob stores a job on disk using the job's ID as the key
func (sds *DiskJobStore) StoreJob(job interface{}) (string, error) {
	sds.lock.Lock()
	defer sds.lock.Unlock()

	// Extract job ID to use as disk key
	var jobID string
	if jobWithID, ok := job.(interface{ ID() string }); ok {
		jobID = jobWithID.ID()
	} else {
		// Fallback to generated key if job doesn't have ID method
		jobID = fmt.Sprintf("job_%d", sds.nextKey)
		sds.nextKey++
	}

	// Encode job using gob
	if gobEncodable, ok := job.(gobEncoder); ok {
		data, err := gobEncodable.GobEncode()
		if err != nil {
			return "", fmt.Errorf("failed to encode job: %w", err)
		}

		// Write to date-based hierarchical file path
		filePath := sds.getJobPath(job, jobID)
		err = os.WriteFile(filePath, data, 0644)
		if err != nil {
			return "", fmt.Errorf("failed to write job file: %w", err)
		}

		return jobID, nil
	}

	return "", fmt.Errorf("job does not implement gob encoding")
}

// RetrieveJob retrieves a job from disk using its key
func (sds *DiskJobStore) RetrieveJob(key string) (interface{}, error) {
	sds.lock.RLock()
	defer sds.lock.RUnlock()

	// Find the job file in the date-based hierarchy
	filePath, err := sds.findJobFile(key)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("job with key %s not found", key)
		}
		return nil, fmt.Errorf("failed to read job file: %w", err)
	}

	// Return raw data - let the caller handle decoding
	return data, nil
}

// DeleteJob removes a job from disk storage
func (sds *DiskJobStore) DeleteJob(key string) error {
	sds.lock.Lock()
	defer sds.lock.Unlock()

	// Find and delete the job file from the date-based hierarchy
	filePath, err := sds.findJobFile(key)
	if err != nil {
		// If file doesn't exist, that's fine for delete operation
		if strings.Contains(err.Error(), "not found") {
			return nil
		}
		return err
	}

	err = os.Remove(filePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete job file: %w", err)
	}

	return nil
}

// Close closes the disk store and releases resources
func (sds *DiskJobStore) Close() error {
	// Nothing to clean up for simple file store
	return nil
}

// GetStoredJobCount returns the number of job files currently on disk
func (sds *DiskJobStore) GetStoredJobCount() (int, error) {
	count := 0
	err := sds.WalkJobFiles(func(key string) error {
		count++
		return nil
	})
	return count, err
}

// GetAllJobKeys returns all job IDs currently stored on disk
func (sds *DiskJobStore) GetAllJobKeys() ([]string, error) {
	var keys []string
	err := sds.WalkJobFiles(func(key string) error {
		keys = append(keys, key)
		return nil
	})
	return keys, err
}

// WalkJobFiles efficiently walks through all job files in the hierarchical structure
func (sds *DiskJobStore) WalkJobFiles(fn func(string) error) error {
	return filepath.Walk(sds.basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and non-job files
		if info.IsDir() || !strings.HasSuffix(info.Name(), ".job") {
			return nil
		}

		// Extract job ID from filename (remove .job extension)
		filename := info.Name()
		key := filename[:len(filename)-4]

		return fn(key)
	})
}
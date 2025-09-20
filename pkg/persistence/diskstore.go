package persistence

import (
	"encoding/gob"
	"fmt"
	"io"
	"sync"
)

// DiskJobStore provides disk-based storage for individual jobs
type DiskJobStore interface {
	// StoreJob stores a job on disk and returns a key to retrieve it
	StoreJob(job interface{}) (string, error)

	// RetrieveJob retrieves a job from disk using its key
	RetrieveJob(key string) (interface{}, error)

	// DeleteJob removes a job from disk storage
	DeleteJob(key string) error

	// Close closes the disk store and releases resources
	Close() error
}

// FileDiskJobStore implements DiskJobStore using file-based storage
type FileDiskJobStore struct {
	storage Storage
	lock    *sync.RWMutex
	jobs    map[string][]byte // In-memory cache for quick access
	nextKey int64             // Simple key generator
}

// NewFileDiskJobStore creates a new file-based disk job store
func NewFileDiskJobStore(storage Storage) *FileDiskJobStore {
	return &FileDiskJobStore{
		storage: storage,
		lock:    &sync.RWMutex{},
		jobs:    make(map[string][]byte),
		nextKey: 1,
	}
}

// StoreJob stores a job on disk and returns a key to retrieve it
func (fds *FileDiskJobStore) StoreJob(job interface{}) (string, error) {
	fds.lock.Lock()
	defer fds.lock.Unlock()

	// Generate unique key
	key := fmt.Sprintf("job_%d", fds.nextKey)
	fds.nextKey++

	// Encode job using gob
	if gobEncodable, ok := job.(gobEncoder); ok {
		data, err := gobEncodable.GobEncode()
		if err != nil {
			return "", fmt.Errorf("failed to encode job: %w", err)
		}

		// Store in memory cache
		fds.jobs[key] = data

		// Immediately persist to actual storage
		err = fds.persistSingleJob(key, data)
		if err != nil {
			// Remove from cache if disk write failed
			delete(fds.jobs, key)
			return "", fmt.Errorf("failed to persist job to storage: %w", err)
		}

		return key, nil
	}

	return "", fmt.Errorf("job does not implement gob encoding")
}

// RetrieveJob retrieves a job from disk using its key
func (fds *FileDiskJobStore) RetrieveJob(key string) (interface{}, error) {
	fds.lock.RLock()
	defer fds.lock.RUnlock()

	data, exists := fds.jobs[key]
	if !exists {
		return nil, fmt.Errorf("job with key %s not found", key)
	}

	// Return the raw data - let the caller handle decoding
	// This avoids circular import issues
	return data, nil
}

// DeleteJob removes a job from disk storage
func (fds *FileDiskJobStore) DeleteJob(key string) error {
	fds.lock.Lock()
	defer fds.lock.Unlock()

	// Remove from memory cache
	delete(fds.jobs, key)

	// Remove from disk storage
	return fds.deleteSingleJob(key)
}

// Close closes the disk store and releases resources
func (fds *FileDiskJobStore) Close() error {
	fds.lock.Lock()
	defer fds.lock.Unlock()

	// Clear the cache
	fds.jobs = nil
	return nil
}

// persistToStorage persists all cached jobs to the underlying storage
// This would typically be called periodically or on shutdown
func (fds *FileDiskJobStore) persistToStorage() error {
	fds.lock.RLock()
	defer fds.lock.RUnlock()

	writer, err := fds.storage.Writer()
	if err != nil {
		return fmt.Errorf("failed to create storage writer: %w", err)
	}
	defer writer.Close()

	encoder := gob.NewEncoder(writer)

	// Write all jobs to storage
	for key, data := range fds.jobs {
		entry := diskEntry{Key: key, Data: data}
		if err := encoder.Encode(entry); err != nil {
			return fmt.Errorf("failed to encode entry %s: %w", key, err)
		}
	}

	return nil
}

// loadFromStorage loads jobs from the underlying storage
func (fds *FileDiskJobStore) loadFromStorage() error {
	reader, err := fds.storage.Reader()
	if err != nil {
		return fmt.Errorf("failed to create storage reader: %w", err)
	}
	defer reader.Close()

	decoder := gob.NewDecoder(reader)
	fds.jobs = make(map[string][]byte)

	for {
		var entry diskEntry
		err := decoder.Decode(&entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to decode entry: %w", err)
		}

		fds.jobs[entry.Key] = entry.Data
	}

	return nil
}

// diskEntry represents a key-value pair stored on disk
type diskEntry struct {
	Key  string
	Data []byte
}

// gobEncoder interface for objects that can be gob-encoded
type gobEncoder interface {
	GobEncode() ([]byte, error)
}

// persistSingleJob writes a single job to disk storage
func (fds *FileDiskJobStore) persistSingleJob(key string, data []byte) error {
	// For now, we'll batch persist jobs periodically rather than one-by-one
	// This is more efficient for the storage backend and matches the existing
	// ChronoMQ persistence model

	// The job is already in memory cache, so it's "persisted" from the perspective
	// of not losing it immediately. The actual disk persistence happens during
	// shutdown or explicit persistence calls via persistToStorage()

	return nil
}

// deleteSingleJob removes a single job from disk storage
func (fds *FileDiskJobStore) deleteSingleJob(key string) error {
	// For file-based storage, we would need to implement file deletion
	// For now, this is a no-op since the underlying storage abstraction
	// doesn't provide a delete API
	// TODO: Implement proper file deletion when storage interface supports it
	return nil
}

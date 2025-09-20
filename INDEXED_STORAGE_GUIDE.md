# Index-Based Storage Guide

## Overview

ChronoMQ now supports an index-based storage system that significantly reduces memory usage by storing only lightweight job indices in memory while keeping the full job data on disk. This approach is particularly beneficial for applications handling large job payloads or high job volumes.

## Memory Usage Comparison

### Traditional Storage (Hub + Spoke)
- **Memory**: Stores complete Job objects in memory
- **Typical usage**: ~1KB+ per job (including payload)
- **Best for**: Small payloads, low to medium job volumes

### Index-Based Storage (IndexedHub + IndexedSpoke)
- **Memory**: Stores only JobIndex objects in memory (~64 bytes per job)
- **Disk**: Stores complete Job objects on disk
- **Memory savings**: 90%+ reduction for typical payloads
- **Best for**: Large payloads, high job volumes

## Architecture Changes

### New Components

1. **JobIndex**: Lightweight in-memory reference to a job
   - Job ID, trigger time, priority
   - Disk key for retrieval
   - Payload size (for monitoring)

2. **DiskJobStore**: Interface for disk-based job storage
   - `StoreJob()`: Saves job to disk, returns key
   - `RetrieveJob()`: Loads job from disk by key
   - `DeleteJob()`: Removes job from disk

3. **IndexedSpoke**: Memory-efficient spoke implementation
   - Stores JobIndex objects instead of full Jobs
   - Loads jobs from disk only when needed

4. **IndexedHub**: Memory-efficient hub implementation
   - Uses IndexedSpoke instances
   - Maintains same interface as traditional Hub

## Migration Guide

### Before (Traditional Hub)

```go
hub := chronomq.NewHub(&chronomq.HubOpts{
    Persister:      persister,
    AttemptRestore: true,
    SpokeSpan:      time.Minute * 5,
    MaxCFSize:      chronomq.DefaultMaxCFSize,
})
defer hub.Stop(true)

// Add jobs
job := chronomq.NewJob("job1", triggerAt, payload)
hub.AddJobLocked(job)

// Process jobs
nextJob := hub.NextLocked()
```

### After (IndexedHub)

```go
// Create disk storage
storage, err := persistence.NewBlobStore(storeConfig)
diskStore := persistence.NewFileDiskJobStore(storage)

// Create indexed hub
hub := chronomq.NewIndexedHub(&chronomq.IndexedHubOpts{
    Persister:      persister,
    DiskStore:      diskStore,        // New: disk storage
    AttemptRestore: true,
    SpokeSpan:      time.Minute * 5,
    MaxCFSize:      chronomq.DefaultMaxCFSize,
})
defer hub.Stop(true)

// Same interface for adding jobs
job := chronomq.NewJob("job1", triggerAt, payload)
hub.AddJobLocked(job) // Job stored on disk, index in memory

// Same interface for processing jobs
nextJob, err := hub.NextLocked() // Job loaded from disk when needed
if err != nil {
    // Handle disk I/O errors
}
```

### Key Differences

1. **Initialization**: IndexedHub requires a `DiskJobStore`
2. **Error Handling**: `NextLocked()` now returns an error for disk I/O failures
3. **Resource Management**: Must close `DiskStore` when done

## Performance Considerations

### Benefits
- **Memory Efficiency**: 90%+ reduction in memory usage for typical workloads
- **Scalability**: Can handle much larger job volumes within same memory constraints
- **Persistence**: Jobs are automatically stored on disk

### Trade-offs
- **Latency**: Small overhead when loading jobs from disk (~1-10ms depending on storage)
- **Disk I/O**: Requires reliable disk/storage access
- **Complexity**: Additional error handling for storage operations

### When to Use Each

**Use Traditional Hub when:**
- Job payloads are small (< 1KB)
- Job volumes are low-medium
- Lowest possible latency is critical
- Simplified error handling preferred

**Use IndexedHub when:**
- Job payloads are large (> 1KB)
- High job volumes (> 100K jobs)
- Memory usage is a concern
- Storage reliability is available

## Configuration Examples

### Local File Storage

```go
storeConfig := persistence.StoreConfig{
    Bucket: &url.URL{Scheme: "file", Path: "/path/to/storage"},
}
storage, _ := storeConfig.Storage()
diskStore := persistence.NewFileDiskJobStore(storage)
```

### Cloud Storage (S3)

```go
storeConfig := persistence.StoreConfig{
    Bucket: &url.URL{Scheme: "s3", Host: "mybucket", Path: "/chronomq/"},
}
storage, _ := storeConfig.Storage()
diskStore := persistence.NewFileDiskJobStore(storage)
```

### In-Memory (Testing)

```go
storage, _ := persistence.InMemStorage()
diskStore := persistence.NewFileDiskJobStore(storage)
```

## Monitoring and Observability

### New Metrics

The IndexedHub provides additional metrics for monitoring:

- `indexedhub.memory.footprint`: Actual memory usage in bytes
- `indexedhub.job.add.duration`: Time to add jobs (including disk storage)
- `indexedhub.next.search.duration`: Time to retrieve jobs (including disk loading)

### Memory Footprint Monitoring

```go
// Monitor memory usage
hubStats := indexedHub.Stats()
memoryFootprint := 0

for idx := range indexedHub.GetNJobIndices(1000) {
    memoryFootprint += estimateIndexSize(idx)
}

fmt.Printf("Memory footprint: %d bytes\n", memoryFootprint)
```

## Error Handling

### Disk Storage Errors

```go
job, err := indexedHub.NextLocked()
if err != nil {
    // Handle potential disk I/O errors
    log.Printf("Failed to load job from disk: %v", err)
    continue
}
```

### Storage Availability

```go
// Check storage health before creating hub
if err := storage.verifyAccess(); err != nil {
    log.Fatal("Storage not accessible: %v", err)
}
```

## Best Practices

1. **Storage Selection**: Choose reliable storage backend for production
2. **Error Handling**: Always handle errors from `NextLocked()`
3. **Resource Cleanup**: Always close `DiskStore` on shutdown
4. **Monitoring**: Monitor memory footprint and disk I/O metrics
5. **Testing**: Test with realistic job payload sizes

## Compatibility

The IndexedHub maintains the same core interface as the traditional Hub, making migration straightforward. The main changes are:

- Constructor parameters (requires `DiskJobStore`)
- Error return from `NextLocked()`
- Additional resource cleanup requirements

Existing client code that adds jobs and processes them should work with minimal changes.
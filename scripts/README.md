# ChronoMQ Scripts

This directory contains utility scripts for interacting with ChronoMQ data.

## Scripts

### 1. add_example_jobs.go

Adds 100,000 test jobs scheduled into the future to a running ChronoMQ instance.

```bash
go run add_example_jobs.go
```

**Features:**

- Connects to ChronoMQ on port 11301
- Creates jobs with random delays between 1 minute and 24 hours
- Shows progress every 5000 jobs
- Reports final performance metrics

### 2. decode_jobs.go

Decodes job files from ChronoMQ's disk storage into human-readable format.

```bash
# Basic usage
go run decode_jobs.go <jobs_directory> [--json] [--limit N]

# Examples
go run decode_jobs.go ../jobs --limit 10
go run decode_jobs.go ../jobs --json --limit 5 > jobs.jsonl
go run decode_jobs.go ../jobs --json | jq -r '.id'
```

**Options:**

- `--json`: Output in line-delimited JSON (JSONL) format
- `--limit N`: Limit output to first N jobs

**Features:**

- Supports hierarchical date-based job storage structure
- JSONL output perfect for processing with `jq`, `grep`, etc.
- Shows job metadata, trigger times, and payloads
- Provides statistics summary

### 3. decode_snapshots.go

Decodes ChronoMQ index snapshot files for analysis and debugging.

```bash
# Basic usage
go run decode_snapshots.go <snapshot_file_or_directory> [--json] [--details]

# Examples
go run decode_snapshots.go ../snapshots/
go run decode_snapshots.go ../snapshots/index-latest.gob --details
go run decode_snapshots.go ../snapshots/ --json > snapshot_analysis.json
```

**Options:**

- `--json`: Output in JSON format
- `--details`: Include detailed spoke information

**Features:**

- Analyzes index snapshots for system health monitoring
- Shows job counts, memory footprints, and spoke statistics
- Supports both single files and directory scanning
- Calculates snapshot age and job distribution

## Generated Files

- `jobs_report.json`: Sample output from decode_jobs.go
- `snapshot_report.json`: Sample output from decode_snapshots.go

## Usage Tips

### Job Analysis

```bash
# Find jobs ready to execute
go run decode_jobs.go ../jobs --json | jq 'select(.trigger_at < now)'

# Count jobs by hour
go run decode_jobs.go ../jobs --json | jq -r '.trigger_at' | cut -c12-13 | sort | uniq -c

# Find largest job payloads
go run decode_jobs.go ../jobs --json | jq -s 'sort_by(.file_size) | reverse | .[0:5]'
```

### Snapshot Analysis

```bash
# Compare snapshot sizes
go run decode_snapshots.go ../snapshots/ --json | jq '.snapshots[].total_jobs'

# Check latest snapshot health
go run decode_snapshots.go ../snapshots/index-latest.gob --details
```

### Performance Testing

```bash
# Load test ChronoMQ
go run add_jobs.go

# Monitor job processing
watch -n 5 'go run decode_jobs.go ../jobs --json --limit 100 | jq -s "length"'
```

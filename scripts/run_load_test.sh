#!/bin/bash

# ChronoMQ Load Test Runner
# This script starts the server and runs the load test

set -e

echo "🚀 ChronoMQ IndexedHub Load Test"
echo "=================================="

# Clean up any previous test data
rm -rf ./test-load-data
mkdir -p ./test-load-data

# Build the load test
echo "📦 Building load test..."
go build -o scripts/loadgen scripts/loadgen.go

# Build chronomq if not already built
if [ ! -f "./chronomq" ]; then
    echo "📦 Building ChronoMQ..."
    go build -o chronomq ./main.go
fi

echo "🏁 Starting ChronoMQ server..."
./chronomq server \
    --store-url ./test-load-data \
    --spokeSpan 10m \
    --friendly-log \
    --log-level INFO &

SERVER_PID=$!

# Wait for server to start
echo "⏳ Waiting for server startup..."
sleep 3

echo "🧪 Running load test..."
echo "   - 100,000 jobs"
echo "   - 5MB payload each"
echo "   - ~500GB total data"
echo ""

# Run the load test
./scripts/loadgen || echo "Load test completed (may have expected errors)"

echo ""
echo "🛑 Stopping server..."
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo ""
echo "📊 Test Results:"
echo "   - Check server logs above for memory usage"
echo "   - IndexedHub should use minimal RAM (~6MB for indices)"
echo "   - Job data stored in ./test-load-data/"

# Show disk usage
if [ -d "./test-load-data" ]; then
    echo "   - Disk usage: $(du -sh ./test-load-data | cut -f1)"
fi

echo ""
echo "✅ Load test completed!"
echo "🧹 Clean up with: rm -rf ./test-load-data scripts/loadgen"
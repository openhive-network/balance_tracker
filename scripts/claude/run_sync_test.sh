#!/bin/bash
# Helper script to run sync test and automatically stop at target block

set -e

# Configuration
TARGET_BLOCK=${1:-5000000}
HOST=${2:-172.17.0.2}
LOG_FILE=${3:-"scripts/btracker_sync_test.log"}
CHECK_INTERVAL=2  # seconds between log checks

echo "=== Sync Test Configuration ==="
echo "Target block: $TARGET_BLOCK"
echo "Host: $HOST"
echo "Log file: $LOG_FILE"
echo ""

# Initialize log file
echo "=== SYNC TEST START: $(date -Iseconds) ===" > "$LOG_FILE"

# Start the sync process in the background
./scripts/process_blocks.sh --host="$HOST" --stop-at-block="$TARGET_BLOCK" 2>&1 | tee -a "$LOG_FILE" &
SYNC_PID=$!

echo "Started sync process with PID: $SYNC_PID"
echo "Monitoring for block $TARGET_BLOCK..."

# Monitor the log file for target block completion
while true; do
    sleep $CHECK_INTERVAL

    # Check if process is still running
    if ! kill -0 $SYNC_PID 2>/dev/null; then
        echo "Sync process ended unexpectedly"
        break
    fi

    # Check if we've reached the target block
    if grep -q "processed block range: <[0-9]*, ${TARGET_BLOCK}>" "$LOG_FILE" 2>/dev/null; then
        echo ""
        echo "=== Target block $TARGET_BLOCK reached! ==="
        echo "Stopping sync process..."

        # Give it a moment to finish writing
        sleep 1

        # Kill the process
        kill $SYNC_PID 2>/dev/null || true

        # Wait for process to terminate
        wait $SYNC_PID 2>/dev/null || true

        echo "=== SYNC TEST END: $(date -Iseconds) ===" >> "$LOG_FILE"
        break
    fi

    # Show progress (last processed block range)
    LAST_BLOCK=$(grep "processed block range" "$LOG_FILE" 2>/dev/null | tail -1 | grep -oP '<\d+, \d+>' | tail -1)
    if [ -n "$LAST_BLOCK" ]; then
        printf "\rCurrent progress: %s" "$LAST_BLOCK"
    fi
done

echo ""
echo "=== Results ==="

# Calculate total time using awk (no bc dependency)
TOTAL_TIME=$(grep "processed block range" "$LOG_FILE" | awk -F'in ' '{sum += $2} END {printf "%.2f", sum}')
TOTAL_MINUTES=$(awk "BEGIN {printf \"%.2f\", $TOTAL_TIME / 60}")
BLOCK_COUNT=$(grep -c "processed block range" "$LOG_FILE")
AVG_TIME=$(awk "BEGIN {printf \"%.4f\", $TOTAL_TIME / $BLOCK_COUNT}")

echo "Total processing time: ${TOTAL_TIME}s (${TOTAL_MINUTES} minutes)"
echo "Block ranges processed: $BLOCK_COUNT"
echo "Blocks synced: $TARGET_BLOCK"
echo "Average time per 10k blocks: ${AVG_TIME}s"

# Show last few processed ranges
echo ""
echo "Last 5 block ranges:"
grep "processed block range" "$LOG_FILE" | tail -5

# Show comparison with baseline
echo ""
echo "=== Performance Comparison ==="
echo "  Baseline:       167.55s (2.79 minutes)"
echo "  Best (Phase 1): 158.82s (2.65 minutes) - 5.2% improvement"
echo "  Current:        ${TOTAL_TIME}s (${TOTAL_MINUTES} minutes)"

# Calculate improvement percentage
BASELINE=167.55
IMPROVEMENT=$(awk "BEGIN {printf \"%.1f\", (($BASELINE - $TOTAL_TIME) / $BASELINE) * 100}")
echo "  Improvement:    ${IMPROVEMENT}% from baseline"

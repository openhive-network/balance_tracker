#!/bin/bash
# Check Balance Tracker pipeline status efficiently for Claude
# Usage: check-balance-tracker-pipeline.sh [pipeline_id|branch]
# Output: Concise report on pipeline problems
#
# Key jobs to watch:
#   - prepare_haf_image, prepare_haf_data: HAF infrastructure
#   - docker-setup-docker-image-build: Main balance_tracker image
#   - sync: Runs HAF + balance_tracker to 5M blocks
#   - sync_with_mock_data: Sync with mock data for testing
#   - regression-test: Account balance dump tests
#   - performance-test: JMeter performance tests via PostgREST
#   - pattern-test: Tavern API pattern tests
#   - pattern-test-with-mock-data: Tavern tests with mock data

set -eo pipefail

PROJECT_ID=330
PROJECT_NAME="balance_tracker"
BRANCH="${1:-feature/nfs-cache-manager}"

# If arg looks like a number, treat as pipeline ID
if [[ "$BRANCH" =~ ^[0-9]+$ ]]; then
    PID="$BRANCH"
else
    # Get latest pipeline for branch
    PID=$(glab api "projects/$PROJECT_ID/pipelines?ref=$BRANCH&per_page=1" 2>/dev/null | jq -r '.[0].id')
fi

if [[ -z "$PID" || "$PID" == "null" ]]; then
    echo "ERROR: No pipeline found for $BRANCH"
    exit 1
fi

# Get pipeline info
PIPELINE=$(glab api "projects/$PROJECT_ID/pipelines/$PID" 2>/dev/null)
STATUS=$(echo "$PIPELINE" | jq -r '.status')
SHA=$(echo "$PIPELINE" | jq -r '.sha[:8]')
CREATED=$(echo "$PIPELINE" | jq -r '.created_at[:16]' | tr 'T' ' ')

echo "Pipeline $PID ($BRANCH) - $STATUS"
echo "SHA: $SHA | Created: $CREATED"
echo "URL: https://gitlab.syncad.com/hive/$PROJECT_NAME/-/pipelines/$PID"
echo ""

# Get all jobs
JOBS=$(glab api "projects/$PROJECT_ID/pipelines/$PID/jobs?per_page=100" 2>/dev/null)

# Job summary
echo "=== Summary ==="
echo "$JOBS" | jq -r 'group_by(.status) | .[] | "\(.[0].status): \(length)"' | sort
echo ""

# Key jobs status
echo "=== Key Jobs ==="
echo "$JOBS" | jq -r '
  .[] | select(.name == "prepare_haf_image" or .name == "prepare_haf_data" or
               .name == "docker-setup-docker-image-build" or .name == "sync" or
               .name == "sync_with_mock_data" or .name == "regression-test" or
               .name == "performance-test" or .name == "pattern-test" or
               .name == "pattern-test-with-mock-data") |
  "\(.status | if . == "success" then "OK" elif . == "failed" then "FAIL" elif . == "running" then "RUN" else . end) \(.name)"
'
echo ""

# Failed jobs with details
FAILED=$(echo "$JOBS" | jq -r '.[] | select(.status == "failed")')
if [[ -n "$FAILED" && "$FAILED" != "null" ]]; then
    echo "=== FAILED JOBS ==="
    echo "$JOBS" | jq -r '.[] | select(.status == "failed") | "[\(.id)] \(.name) (stage: \(.stage))"'
    echo ""

    # Get logs from each failed job
    for JOB_ID in $(echo "$JOBS" | jq -r '.[] | select(.status == "failed") | .id'); do
        JOB_NAME=$(echo "$JOBS" | jq -r ".[] | select(.id == $JOB_ID) | .name")
        echo "--- $JOB_NAME (job $JOB_ID) ---"

        LOG=$(glab api "projects/$PROJECT_ID/jobs/$JOB_ID/trace" 2>/dev/null | tail -150)

        case "$JOB_NAME" in
            sync|sync_with_mock_data)
                # Docker compose, HAF startup, balance_tracker sync errors
                echo "$LOG" | grep -E -A3 -B2 "Error|Exception|FATAL|could not|timeout|failed" | head -50
                ;;
            regression-test)
                # Account dump comparison failures
                echo "$LOG" | grep -E -A3 -B2 "FAILED|Error|mismatch|differ|AssertionError" | head -50
                ;;
            performance-test)
                # JMeter errors, parse-jmeter-output issues
                echo "$LOG" | grep -E -A3 -B2 "Error|Failed|Exception|timeout|0 passed" | head -50
                ;;
            pattern-test*)
                # Pytest/Tavern API test failures
                echo "$LOG" | grep -E -A3 -B2 "FAILED|ERROR|AssertionError|passed.*failed|tavern" | head -50
                ;;
            prepare_haf_*|docker-*)
                # Docker/registry/build errors
                echo "$LOG" | grep -E -A3 -B2 "error|failed|denied|Could not|timeout" | head -40
                ;;
            *)
                if echo "$LOG" | grep -qE "FAILED|Error|Exception|Traceback"; then
                    echo "$LOG" | grep -E -A5 -B2 "FAILED|Error:|Exception:|Traceback" | head -40
                else
                    echo "$LOG" | tail -30
                fi
                ;;
        esac
        echo ""
    done
fi

# Running jobs
RUNNING=$(echo "$JOBS" | jq -r '.[] | select(.status == "running")')
if [[ -n "$RUNNING" && "$RUNNING" != "null" ]]; then
    echo "=== RUNNING ==="
    echo "$JOBS" | jq -r '.[] | select(.status == "running") | "\(.name) on \(.runner.description // "pending")"'
    echo ""
fi

# Canceled jobs
CANCELED=$(echo "$JOBS" | jq -r '.[] | select(.status == "canceled")')
if [[ -n "$CANCELED" && "$CANCELED" != "null" ]]; then
    echo "=== CANCELED ==="
    echo "$JOBS" | jq -r '.[] | select(.status == "canceled") | .name'
    echo ""
fi

# Test details if tests completed
REGRESSION=$(echo "$JOBS" | jq -r '.[] | select(.name == "regression-test")')
if [[ -n "$REGRESSION" && "$REGRESSION" != "null" ]]; then
    REG_STATUS=$(echo "$REGRESSION" | jq -r '.status')
    echo "=== Test Details ==="
    echo "regression-test: $REG_STATUS (tests/regression/run_test.sh)"
    PERF_STATUS=$(echo "$JOBS" | jq -r '.[] | select(.name == "performance-test") | .status // "pending"')
    echo "performance-test: $PERF_STATUS (JMeter via scripts/ci-helpers/run_performance_tests.sh)"
    PATTERN_STATUS=$(echo "$JOBS" | jq -r '.[] | select(.name == "pattern-test") | .status // "pending"')
    echo "pattern-test: $PATTERN_STATUS (tests/tavern pytest)"
fi

# If pipeline succeeded
if [[ "$STATUS" == "success" ]]; then
    echo ""
    echo "Pipeline PASSED - all jobs successful"
fi

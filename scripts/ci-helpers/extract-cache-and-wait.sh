#!/bin/bash
# Extract NFS cache and wait for PostgreSQL to be ready
# Usage: extract-cache-and-wait.sh <cache_type> <cache_key> [--skip-postgres-wait]
#
# This script is used by CI test jobs to:
# 1. Extract cached data from NFS using cache-manager
# 2. Wait for the haf-instance PostgreSQL service to be ready
#
# The script handles the race condition between cache extraction and service startup
# by checking if PostgreSQL is already running (files in use) before extracting.

set -e

CACHE_TYPE="${1:?Usage: $0 <cache_type> <cache_key> [--skip-postgres-wait]}"
CACHE_KEY="${2:?Usage: $0 <cache_type> <cache_key> [--skip-postgres-wait]}"
SKIP_POSTGRES_WAIT="${3:-}"

LOCAL_CACHE="/cache/${CACHE_TYPE}_${CACHE_KEY}"
CACHE_MANAGER="${CI_PROJECT_DIR}/haf/scripts/ci-helpers/cache-manager.sh"
MARKER_FILE="${LOCAL_CACHE}/.ready"

echo "=== Cache Extraction ==="
echo "CACHE_TYPE: ${CACHE_TYPE}"
echo "CACHE_KEY: ${CACHE_KEY}"
echo "LOCAL_CACHE: ${LOCAL_CACHE}"
echo "MARKER_FILE: ${MARKER_FILE}"
echo "Local cache exists: $([[ -d "${LOCAL_CACHE}/datadir" ]] && echo yes || echo no)"

# Check if marker file has the current pipeline ID (means fresh extraction was done)
MARKER_PIPELINE=""
if [[ -f "$MARKER_FILE" ]]; then
    MARKER_PIPELINE=$(cat "$MARKER_FILE" 2>/dev/null || echo "")
fi
echo "Marker pipeline: ${MARKER_PIPELINE:-none} (current: ${CI_PIPELINE_ID})"

# Check if PostgreSQL is already running (service container started with data)
PG_RUNNING=false
if pg_isready -h haf-instance -p 5432 -q 2>/dev/null; then
    PG_RUNNING=true
    echo "PostgreSQL is running - will skip extraction"
fi

# Extract fresh data only if:
# 1. Marker doesn't have current pipeline ID AND
# 2. PostgreSQL is NOT running (otherwise files are in use)
if [[ "$MARKER_PIPELINE" != "${CI_PIPELINE_ID}" ]] && [[ "$PG_RUNNING" != "true" ]]; then
    echo "Fetching cache via cache-manager..."
    # Remove existing cache to avoid permission errors on overwrite
    sudo rm -rf "${LOCAL_CACHE}" 2>/dev/null || rm -rf "${LOCAL_CACHE}" 2>/dev/null || true
    if [[ -x "$CACHE_MANAGER" ]]; then
        if CACHE_HANDLING=haf "$CACHE_MANAGER" get "${CACHE_TYPE}" "${CACHE_KEY}" "${LOCAL_CACHE}"; then
            echo "Cache extraction complete: ${LOCAL_CACHE}"
            echo "${CI_PIPELINE_ID}" > "${MARKER_FILE}"
            echo "Created marker file: ${MARKER_FILE} (pipeline ${CI_PIPELINE_ID})"
        else
            echo "ERROR: cache-manager failed to get cache"
            exit 1
        fi
    else
        echo "ERROR: cache-manager.sh not found at $CACHE_MANAGER"
        exit 1
    fi
elif [[ "$PG_RUNNING" == "true" ]]; then
    echo "PostgreSQL already running - using existing data (files in use)"
    echo "${CI_PIPELINE_ID}" > "${MARKER_FILE}" 2>/dev/null || true
elif [[ -d "${LOCAL_CACHE}/datadir" ]]; then
    echo "Using existing local cache (marker already set)"
else
    echo "ERROR: No cache available"
    exit 1
fi

# Wait for PostgreSQL unless skipped
if [[ "$SKIP_POSTGRES_WAIT" != "--skip-postgres-wait" ]]; then
    echo ""
    echo "=== Waiting for PostgreSQL ==="
    WAIT_TIMEOUT=300
    WAIT_INTERVAL=2
    WAITED=0
    while ! pg_isready -h haf-instance -p 5432 -q 2>/dev/null; do
        if [[ $WAITED -ge $WAIT_TIMEOUT ]]; then
            echo "ERROR: Timed out waiting for haf-instance after ${WAIT_TIMEOUT}s"
            exit 1
        fi
        sleep $WAIT_INTERVAL
        WAITED=$((WAITED + WAIT_INTERVAL))
        echo "  Waiting for PostgreSQL... ($WAITED/${WAIT_TIMEOUT}s)"
    done
    echo "PostgreSQL is ready!"
fi

echo "=== Done ==="

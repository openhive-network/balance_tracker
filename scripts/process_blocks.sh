#!/bin/bash

set -e
set -o pipefail


print_help () {
    cat <<EOF
Usage: $0 [OPTION[=VALUE]]...

Script for processing blocks in the database.
OPTIONS:
    --host=VALUE             Allows to specify a PostgreSQL host location (defaults to localhost)
    --port=NUMBER            Allows to specify a PostgreSQL operating port (defaults to 5432)
    --user=VALUE             Allows to specify a PostgreSQL user (defaults to btracker_owner)
    --url=URL                Allows to specify a PostgreSQL URL (empty by default, overrides options above)
    --stop-at-block=VALUE            Allows to specify maximum number of blocks to process (defaults to 0, which means 'process all the blocks and wait for more')
    --help,-h,-?             Displays this help message
EOF
}

POSTGRES_USER=${POSTGRES_USER:-"btracker_owner"}
POSTGRES_HOST=${POSTGRES_HOST:-"localhost"}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_URL=${POSTGRES_URL:-""}
POSTGRES_DATABASE=${POSTGRES_DATABASE:-"haf_block_log"}
PROCESS_BLOCK_LIMIT=${PROCESS_BLOCK_LIMIT:-null}
BTRACKER_SCHEMA=${BTRACKER_SCHEMA:-"btracker_app"}

while [ $# -gt 0 ]; do
  case "$1" in
    --host=*)
        POSTGRES_HOST="${1#*=}"
        ;;
    --port=*)
        POSTGRES_PORT="${1#*=}"
        ;;
    --user=*)
        POSTGRES_USER="${1#*=}"
        ;;
    --postgres-url=*)
        POSTGRES_URL="${1#*=}"
        ;;
    --stop-at-block=*)
        PROCESS_BLOCK_LIMIT="${1#*=}"
        ;;
    --schema=*)
        BTRACKER_SCHEMA="${1#*=}"
        ;;
    --help|-h|-?)
        print_help
        exit 0
        ;;
    -*)
        echo "ERROR: '$1' is not a valid option"
        echo
        print_help
        exit 1
        ;;
    *)
        echo "ERROR: '$1' is not a valid argument"
        echo
        print_help
        exit 2
        ;;
    esac
    shift
done

# POSTGRES_ACCESS is no longer used - Python script uses individual connection parameters
# POSTGRES_ACCESS=${POSTGRES_URL:-"postgresql://$POSTGRES_USER@$POSTGRES_HOST:$POSTGRES_PORT/haf_block_log?application_name=btracker_block_processing"}

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Get the project root (parent of scripts directory)
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

process_blocks() {
    local n_blocks="${1:-null}"
    log_file="btracker_sync.log"
    date -uIseconds > /tmp/block_processing_startup_time.txt
    
    # Build arguments for Python script
    PYTHON_ARGS=(
        "--context=${BTRACKER_SCHEMA}"
        "--schema=${BTRACKER_SCHEMA}"
        "--host=${POSTGRES_HOST}"
        "--port=${POSTGRES_PORT}"
        "--user=${POSTGRES_USER}"
        "--database=${POSTGRES_DATABASE}"
    )
    
    # Add stop-at-block if specified and not null
    if [ "$n_blocks" != "null" ] && [ -n "$n_blocks" ]; then
        PYTHON_ARGS+=("--stop-at-block=${n_blocks}")
    fi
    
    # Add URL if specified, otherwise individual connection params are already added
    if [ -n "$POSTGRES_URL" ]; then
        PYTHON_ARGS=("--url=${POSTGRES_URL}" "--context=${BTRACKER_SCHEMA}" "--schema=${BTRACKER_SCHEMA}")
        if [ "$n_blocks" != "null" ] && [ -n "$n_blocks" ]; then
            PYTHON_ARGS+=("--stop-at-block=${n_blocks}")
        fi
    fi
    
    python3 "$PROJECT_ROOT/process_blocks.py" "${PYTHON_ARGS[@]}" 2>&1 | tee -i $log_file
}

process_blocks "$PROCESS_BLOCK_LIMIT"

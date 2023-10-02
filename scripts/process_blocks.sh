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
    --limit=VALUE            Allows to specify maximum number of blocks to process (defaults to 0, which means 'process all the blocks and wait for more')
    --help,-h,-?             Displays this help message
EOF
}

POSTGRES_USER=${POSTGRES_USER:-"btracker_owner"}
POSTGRES_HOST=${POSTGRES_HOST:-"localhost"}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_URL=${POSTGRES_URL:-""}
PROCESS_BLOCK_LIMIT=${PROCESS_BLOCK_LIMIT:-0}

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
    --limit=*)
        PROCESS_BLOCK_LIMIT="${1#*=}"
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

POSTGRES_ACCESS=${POSTGRES_URL:-"postgresql://$POSTGRES_USER@$POSTGRES_HOST:$POSTGRES_PORT/haf_block_log"}

process_blocks() {
    n_blocks="${1:-null}"
    log_file="btracker_sync.log"
    psql "$POSTGRES_ACCESS" -v "ON_ERROR_STOP=on" -c "\timing" -c "CALL btracker_app.main('btracker_app', $n_blocks);" 2>&1 | ts '%Y-%m-%d %H:%M:%.S' | tee -i $log_file
}

process_blocks "$PROCESS_BLOCK_LIMIT"

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

POSTGRES_ACCESS=${POSTGRES_URL:-"postgresql://$POSTGRES_USER@$POSTGRES_HOST:$POSTGRES_PORT/haf_block_log?application_name=btracker_block_processing"}

process_blocks() {
    local n_blocks="${1:-null}"
    log_file="btracker_sync.log"
    date -uIseconds > /tmp/block_processing_startup_time.txt
    psql "$POSTGRES_ACCESS" -v "ON_ERROR_STOP=on" -v BTRACKER_SCHEMA="${BTRACKER_SCHEMA}" -c "\timing" -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -c "CALL ${BTRACKER_SCHEMA}.main('${BTRACKER_SCHEMA}', $n_blocks);" 2>&1 | tee -i $log_file
}

process_blocks "$PROCESS_BLOCK_LIMIT"

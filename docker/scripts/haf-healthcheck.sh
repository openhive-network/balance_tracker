#!/usr/bin/bash

set -e

# Setup a trap to kill potentially pending healthcheck SQL query at script exit
trap 'trap - SIGINT SIGTERM && kill -- -$$' SIGINT SIGTERM

INSTANCE_READY=$(psql --dbname "haf_block_log" --quiet --tuples-only --command="SELECT hive.is_instance_ready()::VARCHAR;")

if [[ "$INSTANCE_READY" == " true" ]]; then
    echo "HAF instance ready!"
    exit 0
else
    echo "HAF instance still starting up."
    exit 1
fi
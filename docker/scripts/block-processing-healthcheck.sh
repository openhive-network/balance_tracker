#!/usr/bin/bash

set -e

# Setup a trap to kill potentially pending healthcheck SQL query at script exit
trap 'trap - SIGINT SIGTERM && kill -- -$$' SIGINT SIGTERM

postgres_user=${POSTGRES_USER:-"haf_admin"}
postgres_host=${POSTGRES_HOST:-"localhost"}
postgres_port=${POSTGRES_PORT:-5432}
postgres_url=${POSTGRES_URL:-""}
POSTGRES_ACCESS=${postgres_url:-"postgresql://$postgres_user@$postgres_host:$postgres_port/haf_block_log"}

INSTANCE_READY=$(psql "$POSTGRES_ACCESS" --quiet --tuples-only --command="SELECT hive.is_instance_ready();")
[ "$INSTANCE_READY" == "f" ] && exit 1

COMMAND="SELECT CASE WHEN (last_processed_block >= irreversible_block) and (irreversible_block > 0)  THEN 0 ELSE 1 END FROM btracker_app.app_status left join hive.contexts on (contexts.name = 'btracker_app') LIMIT 1;"
psql "$POSTGRES_ACCESS" --quiet --tuples-only --command="$COMMAND" | grep 0 &>/dev/null
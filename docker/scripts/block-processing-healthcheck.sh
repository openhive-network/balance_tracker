#! /bin/sh

set -e

# Setup a trap to kill potentially pending healthcheck SQL query at script exit
trap 'trap - 2 15 && kill -- -$$' 2 15

postgres_user=${POSTGRES_USER:-"haf_admin"}
postgres_host=${POSTGRES_HOST:-"localhost"}
postgres_port=${POSTGRES_PORT:-5432}
POSTGRES_ACCESS=${POSTGRES_URL:-"postgresql://$postgres_user@$postgres_host:$postgres_port/haf_block_log"}

exec [ "$(psql "$POSTGRES_ACCESS" --quiet --no-align --tuples-only --command="SELECT hive.is_app_in_sync('btracker_app');")" = t ]

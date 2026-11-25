#!/bin/bash
# The concurrent massive sync (scripts/btracker_parallel.py, db/parallel_sync.sql) against the
# serial one, on a database where the serial sync has already synced and finalized the balance
# tracker in --serial-schema (btracker_app):
#
#   1. installs a second balance tracker as haf_block_explorer's embedded instance (schema
#      hafbe_bal, registered as haf_block_explorer), so the run also covers a non-default schema;
#   2. syncs it to the serial schema's block with haf_app_driver.py and the concurrent range
#      processor, as scripts/process_blocks.sh runs it with BTRACKER_SYNC_PARALLELISM above 1,
#      killing it mid-batch twice once some groups have committed the batch and one is held on a
#      lock: first one worker backend (the driver reconnects and the batch is delivered again),
#      then the driver process itself (it is started again); more than one worker backend must
#      be seen busy at once;
#   3. finalizes it and compares every table with the serial schema's (compare_schemas.sql);
#   4. reinstalls the serial schema (the second install repointed the btracker_backend views at
#      hafbe_bal) and uninstalls hafbe_bal.
#
# Needs haf_app_driver.py on PATH, psql, and python3 with psycopg2.
#
#   tests/parallel_sync/run_test.sh --postgres-url=URL [--source-dir=DIR] [--parallelism=N]
#                                   [--serial-schema=NAME]
#
# URL connects as a role that may install the application (haf_admin); DIR holds the sources to
# install from (scripts/, db/, ...), by default this checkout.

set -euo pipefail

SOURCE_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
POSTGRES_URL=""
PARALLELISM=4
SERIAL_SCHEMA=btracker_app

SCHEMA=hafbe_bal
APP=haf_block_explorer
APPLICATION_NAME=btracker_parallel_test
LOCKER_NAME=btracker_parallel_test_lock
# The table only the rewards group writes: holding it locked stops that group mid-batch.
LOCKED_TABLE="$SCHEMA.account_rewards"

while [ $# -gt 0 ]; do
    case "$1" in
        --postgres-url=*) POSTGRES_URL="${1#*=}" ;;
        --source-dir=*) SOURCE_DIR="${1#*=}" ;;
        --parallelism=*) PARALLELISM="${1#*=}" ;;
        --serial-schema=*) SERIAL_SCHEMA="${1#*=}" ;;
        *) echo "ERROR: '$1' is not a valid option"; exit 2 ;;
    esac
    shift
done
[ -n "$POSTGRES_URL" ] || { echo "ERROR: --postgres-url is required"; exit 2; }

with_params() {
    case "$1" in
        *\?*) echo "$1&$2" ;;
        *) echo "$1?$2" ;;
    esac
}

# The workers inherit the driver's connection options: its role, and no NOTICE per stage.
DRIVER_URL="$(with_params "$POSTGRES_URL" "application_name=$APPLICATION_NAME&options=-c%20role%3Dbtracker_owner%20-c%20client_min_messages%3Dwarning")"

q() { psql "$POSTGRES_URL" -v ON_ERROR_STOP=on -X -q "$@"; }

current_block() { q -Atc "SELECT hive.app_get_current_block_num('$1')"; }

# wait_for SECONDS WHAT SQL: until SQL returns t.
wait_for() {
    local deadline=$((SECONDS + $1)) what="$2" sql="$3"
    until [ "$(q -Atc "$sql")" = t ]; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            echo "timed out after $1s waiting for $what"
            return 1
        fi
        sleep 0.5
    done
}

# Run as "${DRIVER[@]}" --stop-at-block=N, never through a function: started with &, $! is then
# the driver process itself (env execs it), which the test kills.
DRIVER=(env BTRACKER_SCHEMA="$SCHEMA" BTRACKER_SYNC_PARALLELISM="$PARALLELISM"
        PYTHONPATH="$SOURCE_DIR/scripts${PYTHONPATH:+:$PYTHONPATH}"
        haf_app_driver.py --app="$APP" --postgres-url="$DRIVER_URL" --lock="$APP"
        --startup-marker='' --server-messages=WARNING --retry-delay=1
        --process-python=btracker_parallel:process_blocks)

driver_pid=""
locker_pid=""
sampler_pid=""
installed=""

hold_lock() {
    psql "$(with_params "$POSTGRES_URL" "application_name=$LOCKER_NAME&options=-c%20role%3Dbtracker_owner")" -X -q -v ON_ERROR_STOP=on \
        -c "BEGIN" -c "LOCK TABLE $LOCKED_TABLE IN SHARE MODE" -c "SELECT pg_sleep(3600)" > /dev/null 2>&1 &
    locker_pid=$!
    wait_for 120 "the lock on $LOCKED_TABLE" \
        "SELECT EXISTS (SELECT FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
          WHERE a.application_name = '$LOCKER_NAME' AND l.relation = '$LOCKED_TABLE'::regclass AND l.granted)"
}

release_lock() {
    q -Atc "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE application_name = '$LOCKER_NAME'" > /dev/null
    wait "$locker_pid" 2> /dev/null || true
    locker_pid=""
}

# Until the rewards group waits for the lock while another group has committed the batch.
wait_mid_batch() {
    wait_for 600 "a batch half applied, its rewards group waiting for the lock" \
        "SELECT EXISTS (SELECT FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
          WHERE a.application_name = '$APPLICATION_NAME' AND l.relation = '$LOCKED_TABLE'::regclass AND NOT l.granted)
          AND EXISTS (SELECT FROM $SCHEMA.processor_group_progress)"
    q -c "SELECT * FROM $SCHEMA.processor_group_progress ORDER BY group_name"
}

progress_rows() { q -Atc "SELECT count(*) FROM $SCHEMA.processor_group_progress"; }

# The most worker backends seen busy at once, written to $1 whenever it grows.
sample_concurrency() {
    local max=0 n
    echo 0 > "$1"
    while :; do
        n="$(q -Atc "SELECT count(*) FROM pg_stat_activity WHERE application_name = '$APPLICATION_NAME' AND state = 'active'" 2> /dev/null || echo 0)"
        if [ "$n" -gt "$max" ]; then max=$n; echo "$max" > "$1"; fi
        sleep 0.2
    done
}

# install_app.sh loads scripts/set_version_in_sql.pgsql, which is generated, not committed.
# Generate it only when absent: the canary stages the source without .git and writes it itself.
if [ ! -f "$SOURCE_DIR/scripts/set_version_in_sql.pgsql" ]; then
    bash "$SOURCE_DIR/scripts/generate_version_sql.sh" "$SOURCE_DIR"
fi

cleanup() {
    local pid
    for pid in "$driver_pid" "$sampler_pid"; do
        [ -n "$pid" ] && kill -9 "$pid" 2> /dev/null && wait "$pid" 2> /dev/null
    done
    [ -n "$locker_pid" ] && release_lock
    [ -n "$installed" ] || return 0
    echo "== reinstall $SERIAL_SCHEMA, uninstall $SCHEMA"
    bash -e "$SOURCE_DIR/scripts/install_app.sh" --postgres-url="$POSTGRES_URL" --schema="$SERIAL_SCHEMA" > /dev/null
    q -c "SET ROLE btracker_owner; SELECT hive.app_unregister('$APP') FROM hafd.applications WHERE name = '$APP'" > /dev/null
    bash "$SOURCE_DIR/scripts/uninstall_app.sh" --postgres-url="$POSTGRES_URL" --schema="$SCHEMA" > /dev/null
}
trap cleanup EXIT

stop_at="$(current_block "$SERIAL_SCHEMA")"
[ "$stop_at" -gt 0 ] || { echo "$SERIAL_SCHEMA has not processed any block"; exit 1; }
if [ "$(q -Atc "SELECT to_regnamespace('$SCHEMA') IS NOT NULL")" = t ]; then
    echo "schema $SCHEMA exists already; this test installs its own"
    exit 1
fi
# Two failure points well inside the range, 40% and 70% of the way.
first_kill_after=$((stop_at * 2 / 5))
second_kill_after=$((stop_at * 7 / 10))

echo "== install $SCHEMA as $APP"
installed=yes
bash -e "$SOURCE_DIR/scripts/install_app.sh" --postgres-url="$POSTGRES_URL" --schema="$SCHEMA"

concurrency_file="$(mktemp)"
sample_concurrency "$concurrency_file" &
sampler_pid=$!

echo "== concurrent sync of $SCHEMA to block $first_kill_after (parallelism $PARALLELISM)"
"${DRIVER[@]}" --stop-at-block="$first_kill_after"

echo "== a worker backend killed mid-batch: the driver reconnects and redelivers the batch"
hold_lock
"${DRIVER[@]}" --stop-at-block="$second_kill_after" &
driver_pid=$!
wait_mid_batch
echo "killing a worker of the batch after block $(current_block "$SCHEMA")"
q -Atc "SELECT count(pg_terminate_backend(l.pid)) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
         WHERE a.application_name = '$APPLICATION_NAME' AND l.relation = '$LOCKED_TABLE'::regclass AND NOT l.granted"
release_lock
wait "$driver_pid" || { echo "the driver did not survive a worker backend's death"; exit 1; }
driver_pid=""
[ "$(current_block "$SCHEMA")" = "$second_kill_after" ] || { echo "expected block $second_kill_after"; exit 1; }

echo "== the driver process killed mid-batch, then started again"
hold_lock
"${DRIVER[@]}" --stop-at-block="$stop_at" &
driver_pid=$!
wait_mid_batch
before="$(current_block "$SCHEMA")"
kill -9 "$driver_pid"
wait "$driver_pid" 2> /dev/null || true
driver_pid=""
release_lock
# Its worker backends end on their own (client_connection_check_interval) or commit their group.
wait_for 120 "the killed driver's backends to end" \
    "SELECT NOT EXISTS (SELECT FROM pg_stat_activity WHERE application_name = '$APPLICATION_NAME')"
[ "$(current_block "$SCHEMA")" = "$before" ] || { echo "the killed driver's batch was committed"; exit 1; }
left="$(progress_rows)"
echo "groups that had committed the killed batch: $left"
[ "$left" -gt 0 ] || { echo "no group committed the killed batch before the kill"; exit 1; }
"${DRIVER[@]}" --stop-at-block="$stop_at"

kill "$sampler_pid"; wait "$sampler_pid" 2> /dev/null || true
sampler_pid=""
busy="$(cat "$concurrency_file")"
echo "most worker backends busy at once: $busy"
[ "$busy" -ge 2 ] || { echo "the concurrent sync never had two worker backends busy at once"; exit 1; }

block="$(current_block "$SCHEMA")"
echo "$SCHEMA processed up to block $block"
[ "$block" = "$stop_at" ] || { echo "expected block $stop_at"; exit 1; }
[ "$(progress_rows)" = 0 ] || { echo "processor_group_progress is not empty after the sync"; exit 1; }

echo "== finalize $SCHEMA and compare it with $SERIAL_SCHEMA"
q -c "SET search_path TO $SCHEMA, public; SET ROLE btracker_owner; SELECT finalize_massive_sync('$SCHEMA');" > /dev/null
q -v serial="$SERIAL_SCHEMA" -v parallel="$SCHEMA" -c "SET ROLE btracker_owner" -f "$(dirname "$0")/compare_schemas.sql"

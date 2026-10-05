#!/usr/bin/env bash
set -euo pipefail

# Always create an independent container and database. There is deliberately no
# connection-string option that could point these mutating fixtures at shared HAF.
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
test_image=${ORDER_LIFECYCLE_HAF_IMAGE:-registry.gitlab.syncad.com/hive/haf:3c237ec1}
test_container="btracker-order-lifecycle-${RANDOM}-$$"
test_database=order59_regression
test_logs=${ORDER_LIFECYCLE_TEST_LOG_DIR:-$(mktemp -d /tmp/btracker-order-lifecycle.XXXXXX)}
mkdir -p -- "$test_logs"
cleanup() {
  docker rm -f "$test_container" >/dev/null 2>&1 || true
}
trap cleanup EXIT
docker run --detach --rm --network none --name "$test_container" \
  -e HAF_CI_MODE=1 \
  "$test_image" --skip-hived >"$test_logs/container-id"

ready=false
for _attempt in $(seq 1 120); do
  if docker exec "$test_container" psql -X -U haf_admin -d haf_block_log -Atqc \
    "SELECT extversion FROM pg_extension WHERE extname='hive_fork_manager'" 2>/dev/null | grep -q .; then
    ready=true
    break
  fi
  if ! docker inspect --format '{{.State.Running}}' "$test_container" 2>/dev/null | grep -q true; then
    break
  fi
  sleep 1
done
if [[ $ready != true ]]; then
  docker logs "$test_container" >"$test_logs/startup.log" 2>&1 || true
  cat "$test_logs/startup.log" >&2
  exit 1
fi
# Copy current sources through Docker's API. A Docker-in-Docker service does not
# necessarily see the checkout's host path, so do not rely on bind mounts.
docker exec --user root "$test_container" mkdir -p /repo/tests/order_lifecycle
for source_dir in backend db endpoints; do
  docker cp "$repo_dir/$source_dir" "$test_container:/repo/$source_dir"
done
for test_sql in "$repo_dir"/tests/order_lifecycle/*.sql; do
  docker cp "$test_sql" "$test_container:/repo/tests/order_lifecycle/"
done
docker exec "$test_container" createdb -U haf_admin "$test_database"
docker exec "$test_container" psql -X -U haf_admin -d "$test_database" -v ON_ERROR_STOP=1 \
  -c 'CREATE EXTENSION hive_fork_manager CASCADE' >"$test_logs/extension.log" 2>&1
psql_test() {
  docker exec -i "$test_container" psql -X -U haf_admin -d "$test_database" -v ON_ERROR_STOP=1 "$@"
}
fail_log() {
  cat "$1" >&2
  echo "Test logs: $test_logs" >&2
  exit 1
}
expect_call_error() {
  local test_name=$1 expected=$2
  shift 2
  if psql_test -v VERBOSITY=verbose "$@" >"$test_logs/$test_name.log" 2>&1; then
    echo "Expected SQL failure: $test_name" >&2
    exit 1
  fi
  if ! grep -Fq -- "$expected" "$test_logs/$test_name.log"; then
    fail_log "$test_logs/$test_name.log"
  fi
}

psql_test -f /repo/tests/order_lifecycle/setup.sql \
  -f /repo/tests/order_lifecycle/core.sql \
  -f /repo/tests/order_lifecycle/api.sql \
  -f /repo/tests/order_lifecycle/fork.sql \
  -f /repo/tests/order_lifecycle/backfill_setup.sql >"$test_logs/core-api-fork.log" 2>&1 \
  || fail_log "$test_logs/core-api-fork.log"
echo 'Core, API and actual HAF fork rewind/replay passed.'

# A real app processor holds its shared lock in another backend/session.
psql_test -c "SET application_name='order59_lock_holder'" \
  -c "SELECT hive.acquire_app_block_processor_locks(ARRAY['order59_upgrade_lock'])" \
  -c 'SELECT pg_sleep(30)' >"$test_logs/shared-lock-holder.log" 2>&1 &
holder_process=$!
lock_pid=
for _attempt in $(seq 1 50); do
  lock_pid=$(psql_test -Atqc "SELECT a.pid FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid WHERE a.application_name='order59_lock_holder' AND l.locktype='advisory' AND l.classid=hashtext('hive_fork_manager_app_lock')::oid AND l.objid=hashtext('order59_upgrade_lock')::oid AND l.mode='ShareLock' AND l.granted")
  if [[ $lock_pid =~ ^[0-9]+$ ]]; then
    break
  fi
  sleep 0.1
done
if [[ ! $lock_pid =~ ^[0-9]+$ ]]; then
  fail_log "$test_logs/shared-lock-holder.log"
fi
expect_call_error busy-lock '55P03: Application processor is active' \
  -c 'SET ROLE btracker_owner' -c 'SET search_path TO order59_upgrade' \
  -c "CALL backfill_order_lifecycle('order59_upgrade',2,'order59_upgrade_lock')"
psql_test -f /repo/tests/order_lifecycle/backfill_preflight.sql \
  -c 'RESET ROLE' \
  -c "SELECT pg_terminate_backend($lock_pid)" >"$test_logs/shared-lock-checks.log" 2>&1 \
  || fail_log "$test_logs/shared-lock-checks.log"
wait "$holder_process" || true
psql_test -c "SELECT order59_test.check_that(hive.try_acquire_app_install_lock('order59_upgrade_lock'),'install lock available after blocked call and shared-holder connection close')" \
  >"$test_logs/lock-release.log" 2>&1 || fail_log "$test_logs/lock-release.log"
psql_test -c "SELECT order59_test.check_that(NOT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=hashtext('hive_fork_manager_app_lock')::oid AND objid=hashtext('order59_upgrade_lock')::oid AND granted),'dedicated install-lock probe releases lock on connection close')" \
  >>"$test_logs/lock-release.log" 2>&1 || fail_log "$test_logs/lock-release.log"

psql_test -c 'UPDATE hafd.hive_state SET pruning=1' >"$test_logs/pruning-setup.log" 2>&1
expect_call_error pruning 'Disable HAF pruning' \
  -c 'SET ROLE btracker_owner' -c 'SET search_path TO order59_upgrade' \
  -c "CALL backfill_order_lifecycle('order59_upgrade',2,'order59_upgrade_lock')"
psql_test -c 'UPDATE hafd.hive_state SET pruning=0' \
  -c 'CREATE TABLE order59_test.saved_source_block AS SELECT * FROM hafd.blocks_reversible WHERE num=6' \
  -c 'DELETE FROM hafd.blocks_reversible WHERE num=6' >"$test_logs/retention-setup.log" 2>&1
expect_call_error retention 'Complete retained block history is required' \
  -c 'SET ROLE btracker_owner' -c 'SET search_path TO order59_upgrade' \
  -c "CALL backfill_order_lifecycle('order59_upgrade',2,'order59_upgrade_lock')"
psql_test -c 'INSERT INTO hafd.blocks_reversible SELECT * FROM order59_test.saved_source_block' \
  -c 'DROP TABLE order59_test.saved_source_block' \
  -f /repo/tests/order_lifecycle/backfill_preflight.sql >"$test_logs/preflight-checks.log" 2>&1 \
  || fail_log "$test_logs/preflight-checks.log"

# CALL must run directly in a dedicated session: a test DO/exception wrapper
# would prevent the maintenance procedure's real per-batch COMMIT.
expect_call_error interrupted 'No active order alice/999' \
  -c 'SET ROLE btracker_owner' -c 'SET search_path TO order59_upgrade' \
  -c "CALL backfill_order_lifecycle('order59_upgrade',2,'order59_upgrade_lock')"
psql_test -f /repo/tests/order_lifecycle/backfill_failed.sql >"$test_logs/interruption-checks.log" 2>&1 \
  || fail_log "$test_logs/interruption-checks.log"
psql_test -c 'SET ROLE btracker_owner' -c 'SET search_path TO order59_upgrade' \
  -c "CALL backfill_order_lifecycle('order59_upgrade',2,'order59_upgrade_lock')" \
  -f /repo/tests/order_lifecycle/backfill_finished.sql >"$test_logs/resume.log" 2>&1 \
  || fail_log "$test_logs/resume.log"
psql_test -f /repo/tests/order_lifecycle/group.sql >"$test_logs/group.log" 2>&1 \
  || fail_log "$test_logs/group.log"
echo 'Actual HAF upgrade, fixed-target interruption/resume and embedded group passed.'
psql_test -At -c "SELECT 'Assertions passed: ' || last_value FROM order59_test.test_checks_count; SELECT 'HAF revision: ' || extversion FROM pg_extension WHERE extname='hive_fork_manager';"
echo "Test logs: $test_logs"

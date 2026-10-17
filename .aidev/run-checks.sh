#!/usr/bin/env bash
# The step functions are called indirectly, through `step`.
# shellcheck disable=SC2317,SC2329
# The checks AIDEV's verification slots run (.aidev/project.yaml), as one junit
# report per suite: each named step is a test case, its log the failure body.
#
#   .aidev/run-checks.sh <suite> <step>...
#
#   - shellcheck      ShellCheck at error severity on the project's scripts, and at
#                     the default severity on .aidev/'s own scripts
#   - sql-registered  every .sql file under db/, backend/ and endpoints/ is applied
#                     by scripts/install_app.sh (a file it misses never reaches a
#                     database)
#   - install         a copy-on-write clone of the shared 5M-block HAF for this run
#                     (haf_shared.clone_run_database on AIDEV_EXTERNAL_HAF_HOST:_PORT,
#                     as haf_shared_consumer), scripts/install_app.sh into it
#   - sync            the candidate's block processing over the clone's 5,000,000
#                     blocks (haf_app_driver.py, as scripts/process_blocks.sh runs it),
#                     then finalize_massive_sync and ANALYZE, as CI's `sync` job does
#   - parity          tests/parity: the SQL impacted-balances port against HAF's C visitor
#   - maintenance     tests/maintenance: the history tables' autovacuum reloptions, no
#                     HAF vacuum request from the sync, finalize or a later catch-up
#                     besides finalize's 'freeze' requests, and those freeze requests
#   - regression      tests/regression: every account's balances at block 5,000,000
#                     against hived's database_api.list_accounts dump
#   - tavern          the tavern pattern tests (tests/tavern/patterns-mainnet) against
#                     PostgREST over the clone, by .aidev/btracker_tavern.py; one junit
#                     case per test
#   - reinstall       scripts/install_app.sh again over the installed, synced schema
#                     (an upgrade in place, as CI's setup-scripts-test), then the
#                     synced block is checked to be unchanged and the history tables'
#                     reloptions, removed beforehand, to be back
#   - uninstall       scripts/uninstall_app.sh, then the context and schemas are gone,
#                     then install again from scratch
#
# Every step after install needs the shared HAF (sandbox.external haf, provider
# haf/haf); the clone is dropped when the script exits, pass or fail. sync needs
# install; parity, regression, maintenance and tavern need sync. Reports go to
# test-results/<suite>/: junit.xml (one case per step) plus tavern.xml.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

suite="${1:?usage: $0 <suite> <step>...}"; shift
out="test-results/$suite"
rm -rf "$out"; mkdir -p "$out"
cases="$out/cases.tsv"; : > "$cases"

SCHEMA=btracker_app
STOP_AT_BLOCK=5000000

# The shared HAF trusts haf_shared_consumer from the fleet LAN: no password.
CONSUMER_URL="postgresql://haf_shared_consumer@${AIDEV_EXTERNAL_HAF_HOST:-}:${AIDEV_EXTERNAL_HAF_PORT:-5432}"
clone=""; clone_id=""
synced=""
# The script's own stderr, which a step's redirected output does not reach: where
# the clone's name and its drop are reported.
exec 3>&2

status=0
# record CASES_FILE NAME RC SECONDS LOG
record() {
    if [ "$3" -eq 0 ]; then
        printf 'case\t%s\tpass\t%s\t\n' "$2" "$4" >> "$1"
    else
        printf 'case\t%s\tfail\t%s\texit %s\t%s\n' "$2" "$4" "$3" "$5" >> "$1"
    fi
}

step() {
    local name="$1"; shift
    local log="$out/$name.log" t0=$SECONDS rc=0
    echo "== $name" >&2
    "$@" > "$log" 2>&1 < /dev/null || rc=$?
    [ "$rc" -eq 0 ] || { status=1; tail -40 "$log" >&2; }
    record "$cases" "$name" "$rc" "$((SECONDS - t0))" "$log"
}

run_shellcheck() {
    shellcheck --version | head -2
    find scripts docker tests -name '*.sh' -type f -print0 | sort -z \
        | xargs -0 shellcheck -S error -f gcc || return 1
    shellcheck -f gcc .aidev/*.sh .aidev/runtime/*.sh
}

sql_registered() {
    local f missing=0
    while IFS= read -r f; do
        if ! grep -qF "/../$f\"" scripts/install_app.sh; then
            echo "not applied by scripts/install_app.sh: $f"
            missing=1
        fi
    done < <(find db backend endpoints -name '*.sql' | sort)
    [ "$missing" -eq 0 ] && echo "every .sql file under db/, backend/ and endpoints/ is applied"
    return "$missing"
}

q() { psql "$BTRACKER_TEST_DB_URL" -v ON_ERROR_STOP=on -X "$@"; }

# The install script writes its version file next to itself from git, which a
# workflow checkout may not have: stage the sources in a scratch directory and
# write the version there. The shared HAF provides btracker_owner and btracker_user
# and the consumer may neither create nor drop roles, so the staged
# db/builtin_roles.sql is empty and the staged uninstall drops no role: its DROP
# OWNED BY the two roles would also revoke their grants on every other run's clone
# (database privileges are cluster-wide).
stage=""
stage_sources() {
    [ -n "$stage" ] && return 0
    stage="$(mktemp -d)"
    cp -r db backend endpoints scripts "$stage/"
    echo "-- btracker_owner and btracker_user are provided by the shared HAF" > "$stage/db/builtin_roles.sql"
    sed -i '/DROP OWNED\|DROP ROLE/d' "$stage/scripts/uninstall_app.sh"
    local hash
    hash="$(git rev-parse HEAD 2>/dev/null || echo aidev)"
    printf "TRUNCATE TABLE version; INSERT INTO version(git_hash) VALUES ('%s');\n" \
        "$hash" > "$stage/scripts/set_version_in_sql.pgsql"
}

# This run's own clone of the shared HAF, named after the suite, the time and a
# random suffix so concurrent runs never share one.
open_clone() {
    [ -n "$clone" ] && return 0
    if [ -z "${AIDEV_EXTERNAL_HAF_HOST:-}" ]; then
        echo "AIDEV_EXTERNAL_HAF_HOST is unset: this slot needs sandbox.external haf (provider haf/haf)"
        return 1
    fi
    local run_id
    run_id="btracker-$suite-$(date -u +%Y%m%d%H%M%S)-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
    clone="$(psql "$CONSUMER_URL/haf_shared" -v ON_ERROR_STOP=on -Atc "select haf_shared.clone_run_database('$run_id')")" || return 1
    clone_id="$run_id"
    export BTRACKER_TEST_DB_URL="$CONSUMER_URL/$clone"
    echo "== shared HAF clone $clone ($CONSUMER_URL)" | tee /dev/fd/3
}

drop_clone() {
    [ -n "$clone" ] || return 0
    if psql "$CONSUMER_URL/haf_shared" -v ON_ERROR_STOP=on -qAtc "select haf_shared.drop_run_database('$clone_id')"; then
        echo "== dropped shared HAF clone $clone" >&3
    else
        echo "== could not drop shared HAF clone $clone; the provider reaps it after its TTL" >&3
    fi
}

cleanup() {
    drop_clone
    if [ -n "$stage" ]; then rm -rf "$stage"; fi
}
trap cleanup EXIT

# Run "$@" only when the step it depends on has produced its state.
needs_clone() {
    if [ -z "$clone" ]; then
        echo "no shared HAF clone: install did not run or could not create one"
        return 1
    fi
    "$@"
}
needs_sync() {
    if [ -z "$synced" ]; then
        echo "balance tracker is not synced: the sync step did not run or failed"
        return 1
    fi
    "$@"
}

install() {
    stage_sources && open_clone || return 1
    # The consumer owns the clone; the app's roles need to create its schemas in it.
    q -c "GRANT CREATE, CONNECT ON DATABASE \"$clone\" TO btracker_owner, btracker_user" || return 1
    (cd "$stage" && bash scripts/install_app.sh --postgres-url="$BTRACKER_TEST_DB_URL") || return 1
    q -Atc "select hive.app_context_exists('$SCHEMA')" | grep -qx t || { echo "context $SCHEMA was not created"; return 1; }
    q -Atc "select git_hash from $SCHEMA.version"
}

current_block() {
    q -Atc "select hive.app_get_current_block_num('$SCHEMA')"
}

# The candidate's own block processing over the clone's chain, as the production
# image's scripts/process_blocks.sh runs it, then the post-replay finalisation CI's
# sync job runs (in CI the massive stage never turns live).
sync_blocks() {
    local block
    haf_app_driver.py --app="$SCHEMA" \
        --postgres-url="$BTRACKER_TEST_DB_URL?application_name=btracker_block_processing&options=-c%20role%3Dbtracker_owner" \
        --lock=balance_tracker --stop-at-block="$STOP_AT_BLOCK" --startup-marker='' \
        --server-messages=WARNING || return 1
    block="$(current_block)" || return 1
    echo "processed up to block $block"
    [ "$block" -ge "$STOP_AT_BLOCK" ] || { echo "expected block $STOP_AT_BLOCK"; return 1; }
    # The massive sync ANALYZEs the history tables itself (no HAF vacuum request);
    # checked before finalize and the ANALYZE below can do it instead.
    q -Atc "SELECT relname FROM pg_stat_user_tables WHERE schemaname = '$SCHEMA'
              AND relname IN ('account_balance_history', 'account_vesting_history')
              AND last_analyze IS NOT NULL" | sort | paste -sd' ' \
        | grep -qx 'account_balance_history account_vesting_history' \
        || { echo "the massive sync did not ANALYZE both history tables"; return 1; }
    q -c "SET search_path TO $SCHEMA, public; SET ROLE btracker_owner; SELECT finalize_massive_sync('$SCHEMA');" || return 1
    q -c "SET ROLE btracker_owner; ANALYZE;" 2>&1 | grep -v 'WARNING:  permission denied to analyze' || true
    synced=yes
}

parity() {
    local f
    for f in tests/parity/*.sql; do
        echo "== $f"
        q -q -f "$f" || return 1
    done
}

# tests/regression/run_test.sh against the clone: install the test schema, load
# hived's account dump, compare every account, expect no difference. The dump is
# loaded server-side (one COPY, then load_expected_account per row in one
# statement) instead of by load_expected_balances.py's one round trip per account,
# which takes minutes when the shared HAF is not on this host.
regression() {
    local dir=tests/regression f differing
    for f in "$dir"/sql/*.sql; do
        q -q -f "$f" || return 1
    done
    q -c "TRUNCATE btracker_test.expected_account_balances; TRUNCATE btracker_test.differing_accounts;" || return 1
    gunzip -c "$dir/accounts_dump.json.gz" \
        | python3 -c 'import json, sys; [print(json.dumps(a)) for a in json.load(sys.stdin)["result"]["accounts"]]' \
        | q -c "CREATE TEMP TABLE accounts_dump(doc jsonb)" \
            -c "\\copy accounts_dump(doc) FROM STDIN WITH (FORMAT csv, QUOTE e'\\x01', DELIMITER e'\\x02')" \
            -c "SELECT count(btracker_test.load_expected_account(doc)) AS loaded FROM accounts_dump" || return 1
    q -c "SET search_path TO $SCHEMA;" -c "SELECT btracker_test.compare_accounts();" || return 1
    differing="$(q -Atc "SELECT count(*) FROM btracker_test.differing_accounts")" || return 1
    echo "accounts differing from hived at block $STOP_AT_BLOCK: $differing"
    if [ "$differing" != 0 ]; then
        q -c "SELECT * FROM btracker_test.differing_accounts LIMIT 20"
        return 1
    fi
}

maintenance() {
    local f
    for f in tests/maintenance/*.sql; do
        echo "== $f"
        q -q -v schema="$SCHEMA" -f "$f" || return 1
    done
}

history_reloptions() {
    q -Atc "SELECT relname || ' ' || coalesce(array_to_string(reloptions, ','), '-') FROM pg_class
              WHERE relnamespace = '$SCHEMA'::regnamespace
                AND relname IN ('account_balance_history', 'account_vesting_history') ORDER BY relname"
}

reinstall() {
    local before after options
    before="$(current_block)" || return 1
    options="$(history_reloptions)" || return 1
    [ "$(grep -c 'autovacuum_freeze_min_age=0' <<< "$options")" = 2 ] \
        || { echo "the install left the history tables with:"; echo "$options"; return 1; }
    # An install that predates the reloptions: the upgrade must add them.
    q -c "SET ROLE btracker_owner;
          ALTER TABLE $SCHEMA.account_balance_history RESET (autovacuum_vacuum_insert_scale_factor, autovacuum_vacuum_insert_threshold, autovacuum_freeze_min_age, autovacuum_analyze_scale_factor, autovacuum_analyze_threshold);
          ALTER TABLE $SCHEMA.account_vesting_history RESET (autovacuum_vacuum_insert_scale_factor, autovacuum_vacuum_insert_threshold, autovacuum_freeze_min_age, autovacuum_analyze_scale_factor, autovacuum_analyze_threshold);" || return 1
    (cd "$stage" && bash scripts/install_app.sh --postgres-url="$BTRACKER_TEST_DB_URL") || return 1
    after="$(current_block)" || return 1
    echo "synced block before $before, after $after"
    [ "$before" = "$after" ] || { echo "reinstalling over a synced schema moved its block"; return 1; }
    echo "history table reloptions before the reset:"; echo "$options"
    [ "$(history_reloptions)" = "$options" ] || { echo "reinstall left the history tables with:"; history_reloptions; return 1; }
}

uninstall() {
    local left
    (cd "$stage" && bash scripts/uninstall_app.sh --postgres-url="$BTRACKER_TEST_DB_URL") || return 1
    # The uninstall leaves btracker_endpoints and btracker_backend to its DROP OWNED,
    # which the staged copy cannot run.
    q -c "DROP SCHEMA IF EXISTS btracker_endpoints, btracker_backend CASCADE" || return 1
    left="$(q -Atc "select count(*) from pg_namespace where nspname in ('$SCHEMA','btracker_endpoints','btracker_backend')")" || return 1
    q -Atc "select hive.app_context_exists('$SCHEMA')" | grep -qx f || { echo "context $SCHEMA survived the uninstall"; return 1; }
    [ "$left" = 0 ] || { echo "$left btracker schemas survived the uninstall"; return 1; }
    synced=""
    install
}

for s in "$@"; do
    case "$s" in
        shellcheck) step shellcheck run_shellcheck ;;
        sql-registered) step sql-registered sql_registered ;;
        install) step install install ;;
        sync) step sync needs_clone sync_blocks ;;
        parity) step parity needs_clone parity ;;
        maintenance) step maintenance needs_sync maintenance ;;
        regression) step regression needs_sync regression ;;
        tavern) step tavern needs_sync python3 .aidev/btracker_tavern.py "$out" ;;
        reinstall) step reinstall needs_clone reinstall ;;
        uninstall) step uninstall needs_clone uninstall ;;
        *) echo "unknown step: $s" >&2; exit 2 ;;
    esac
done
python3 .aidev/junit_cases.py "$out/junit.xml" "$suite" "$cases"
exit "$status"

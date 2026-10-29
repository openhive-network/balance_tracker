#!/usr/bin/env bash
# cleared_accounts_prune_race.sh SCHEMA PSQL_COMMAND...
#
# The #61 repair against HAF pruning that advances WHILE it runs (two sessions; the
# single-session cleared_accounts_repair.sql covers a HAF pruned before it starts).
#
#   0. setup (committed, undone on exit): null's delayed_vests is made wrong, so the
#      rebuild's last write - its account_withdraws UPDATE, after every read of HAF data -
#      has a row to change; first_available_block is replaced by one that reports a
#      "pruned" first block from a table when the table holds one (see below);
#   1. a holder session locks null's account_withdraws row;
#   2. the repair session (finalize path, apply_cleared_accounts_repair, in a transaction
#      it rolls back) runs the whole rebuild and blocks on that row - after its last read;
#   3. a third session commits "pruning": the first available block becomes the one after
#      the first block the rebuild needs (cleared_accounts_first_needed_block + 1);
#   4. the holder releases the row; the repair writes it, reaches its final source check,
#      and must find the source incomplete: return FALSE, leave every table as it was (its
#      writes rolled back) and record nothing.
#
# What it shows: pruning committed after the rebuild's first check and after its last
# read is caught - so the final check runs after that write. What it does not: the mock HAF
# is shared with later CI steps and their cache, so no block is deleted - the later reads
# are not shown to come out wrong, and the replaced first_available_block stands in for
# the real one. Pruning is data, as in HAF: a READ COMMITTED statement sees it once it is
# committed. (A function replaced mid-transaction would not do: a session keeps its cached
# definition while it holds locks on the function's tables.)
#
# PSQL_COMMAND: how to start psql against the HAF database, e.g.
#   docker compose ... exec -T haf psql -U haf_admin -d haf_block_log
set -euo pipefail

SCHEMA="$1"; shift
PSQL=("$@")
WORK="$(mktemp -d)"
TIMEOUT=180
SETUP_DONE=0

psql_run() { "${PSQL[@]}" -X -q -v ON_ERROR_STOP=on -v schema="$SCHEMA" "$@"; }
is_original() {  # the real first_available_block (reads hafd.blocks, no test table)
  psql_run -At -c "SELECT prosrc LIKE '%hafd.blocks%' AND prosrc NOT LIKE '%bt61_prune_race%'
                   FROM pg_proc WHERE oid = 'btracker_backend.first_available_block(text)'::regprocedure"
}

[[ "$(is_original)" == "t" ]] || { echo "prune_race: first_available_block is not the original (a leftover override?)"; exit 1; }
ORIGINAL_DEF="$WORK/first_available_block.sql"
psql_run -At -c "SELECT pg_get_functiondef('btracker_backend.first_available_block(text)'::regprocedure)" > "$ORIGINAL_DEF"
NULL_DELAYED="$(psql_run -At -c "SET search_path TO \"$SCHEMA\"; SELECT aw.delayed_vests FROM account_withdraws aw JOIN accounts_view av ON av.id = aw.account AND av.name = 'null'")"
[[ -n "$NULL_DELAYED" ]] || { echo "prune_race: null has no account_withdraws row; the repair's last write would not wait"; exit 1; }

cleanup() {
  local rc=$?
  # the sessions' backends outlive a killed psql client: end them first, or they keep locks
  psql_run -At -c "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE application_name LIKE 'bt61_prune_race_%'" >/dev/null || rc=1
  jobs -p | xargs -r kill 2>/dev/null || true
  if [[ $SETUP_DONE == 1 ]]; then
    { echo "SET ROLE btracker_owner;"; cat "$ORIGINAL_DEF"; echo ";"; echo "RESET ROLE;"
      echo "UPDATE \"$SCHEMA\".account_withdraws aw SET delayed_vests = $NULL_DELAYED"
      echo "  FROM \"$SCHEMA\".accounts_view av WHERE av.id = aw.account AND av.name = 'null';"
    } | psql_run -f - || rc=1
  fi
  psql_run -c "DROP TABLE IF EXISTS public.bt61_prune_race_signal, public.bt61_prune_race_first_available;" || rc=1
  [[ "$(is_original)" == "t" ]] || { echo "prune_race: first_available_block was NOT restored"; rc=1; }
  if [[ $rc != 0 ]]; then
    for f in "$WORK"/*.log; do [[ -e "$f" ]] && { echo "---- $(basename "$f")"; cat "$f"; }; done
  fi
  rm -rf "$WORK"
  exit $rc
}
trap cleanup EXIT

# ---------------------------------------------------------------- 0. setup (committed)
SETUP_DONE=1
psql_run -f - <<'SQL'
SET search_path TO :"schema", public;
DROP TABLE IF EXISTS public.bt61_prune_race_signal, public.bt61_prune_race_first_available;
CREATE TABLE public.bt61_prune_race_signal (step TEXT);
CREATE TABLE public.bt61_prune_race_first_available (v INT);
GRANT SELECT ON public.bt61_prune_race_first_available TO PUBLIC;
SET ROLE btracker_owner;
CREATE OR REPLACE FUNCTION btracker_backend.first_available_block(_context TEXT)
RETURNS INT LANGUAGE sql STABLE AS $f$
  SELECT COALESCE(
    (SELECT MAX(p.v) FROM public.bt61_prune_race_first_available p),
    GREATEST((SELECT MIN(b.num) FROM hafd.blocks b),
             (SELECT hafd.operation_id_to_block_num(MIN(o.id)) FROM hafd.operations o)));
$f$;
RESET ROLE;
UPDATE account_withdraws aw SET delayed_vests = aw.delayed_vests + 4000000000
FROM accounts_view av WHERE av.id = aw.account AND av.name = 'null';
SQL
NEEDED="$(psql_run -At -c "SELECT btracker_backend.cleared_accounts_first_needed_block('$SCHEMA')")"
[[ "$NEEDED" =~ ^[0-9]+$ ]] || { echo "prune_race: no first needed block ('$NEEDED'): nothing to repair"; exit 1; }

# wait_for "<SQL returning true when ready>" "<what>"
wait_for() {
  local i
  for ((i = 0; i < TIMEOUT * 2; i++)); do
    [[ "$(psql_run -At -c "$1")" == "t" ]] && return 0
    sleep 0.5
  done
  echo "prune_race: timed out waiting for $2"; return 1
}

# ---------------------------------------------------------------- 1. the holder
psql_run -f - > "$WORK/holder.log" 2>&1 <<'SQL' &
SET application_name = 'bt61_prune_race_holder';
SET search_path TO :"schema", public;
BEGIN;
DO $$
BEGIN
  PERFORM 1 FROM account_withdraws aw
  WHERE aw.account = (SELECT av.id FROM accounts_view av WHERE av.name = 'null')
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'prune_race: no account_withdraws row of null to hold';
  END IF;
  FOR i IN 1..360 LOOP
    EXIT WHEN EXISTS (SELECT 1 FROM public.bt61_prune_race_signal WHERE step = 'pruned');
    PERFORM pg_sleep(0.5);
  END LOOP;
  IF NOT EXISTS (SELECT 1 FROM public.bt61_prune_race_signal WHERE step = 'pruned') THEN
    RAISE EXCEPTION 'prune_race: holder gave up waiting for the pruning step';
  END IF;
END
$$;
COMMIT;
SQL
HOLDER=$!
wait_for "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name = 'bt61_prune_race_holder' AND wait_event = 'PgSleep')" \
  "the holder to lock null's account_withdraws row"

# ---------------------------------------------------------------- 2. the repair
psql_run -f - > "$WORK/repair.log" 2>&1 <<'SQL' &
SET application_name = 'bt61_prune_race_repair';
SET search_path TO :"schema", public;
SELECT set_config('btracker_test.schema', :'schema', false) \g /dev/null
BEGIN;
CREATE TEMP TABLE t_accounts ON COMMIT DROP AS
SELECT av.id AS account FROM accounts_view av WHERE av.name IN ('null', 'steem.dao');
CREATE FUNCTION pg_temp.digest() RETURNS TEXT LANGUAGE plpgsql AS $f$
DECLARE _acc TEXT := ''; _part TEXT; _q TEXT;
BEGIN
  FOREACH _q IN ARRAY ARRAY[
    'SELECT h.account, h.nai, h.op_type_id, h.balance_seq_no, h.source_op, h.balance FROM account_balance_history h WHERE h.account IN (SELECT account FROM t_accounts)',
    'SELECT h.account, h.nai, h.op_type_id, h.balance_seq_no, h.source_op, h.balance FROM account_savings_history h WHERE h.account IN (SELECT account FROM t_accounts)',
    'SELECT c.* FROM current_account_balances c WHERE c.account IN (SELECT account FROM t_accounts)',
    'SELECT c.* FROM account_savings c WHERE c.account IN (SELECT account FROM t_accounts)',
    'SELECT b.nai, b.holders, b.total FROM balance_totals b',
    'SELECT d.account, d.nai, d.updated_at, d.balance, d.min_balance, d.max_balance, d.source_op FROM balance_history_by_day d WHERE d.account IN (SELECT account FROM t_accounts)',
    'SELECT d.account, d.nai, d.updated_at, d.balance, d.min_balance, d.max_balance, d.source_op FROM balance_history_by_month d WHERE d.account IN (SELECT account FROM t_accounts)',
    'SELECT d.account, d.nai, d.updated_at, d.balance, d.min_balance, d.max_balance, d.source_op FROM saving_history_by_day d WHERE d.account IN (SELECT account FROM t_accounts)',
    'SELECT d.account, d.nai, d.updated_at, d.balance, d.min_balance, d.max_balance, d.source_op FROM saving_history_by_month d WHERE d.account IN (SELECT account FROM t_accounts)',
    'SELECT r.account, r.nai, r.balance FROM account_rewards r WHERE r.account IN (SELECT account FROM t_accounts)',
    'SELECT w.* FROM account_withdraws w WHERE w.account IN (SELECT account FROM t_accounts)',
    'SELECT i.* FROM account_hbd_interest i WHERE i.account IN (SELECT account FROM t_accounts)',
    'SELECT r.name, r.outcome, r.at_block FROM btracker_repairs r'
  ] LOOP
    EXECUTE format('SELECT md5(COALESCE(string_agg(x::TEXT, ''|'' ORDER BY x::TEXT), '''')) FROM (%s) x', _q) INTO _part;
    _acc := _acc || _part;
  END LOOP;
  RETURN md5(_acc);
END
$f$;
-- unrecorded, so apply runs the rebuild (the wrong delayed_vests from setup is what a
-- completed rebuild would change)
DELETE FROM btracker_repairs WHERE name = 'cleared_accounts';
CREATE TEMP TABLE t_before ON COMMIT DROP AS SELECT pg_temp.digest() AS d;
DO $$
DECLARE
  _schema   TEXT := current_setting('btracker_test.schema');
  _repaired BOOLEAN;
BEGIN
  -- the source is complete when the repair starts: only pruning during the run can stop it
  IF btracker_backend.cleared_accounts_source_gap(_schema) IS NOT NULL THEN
    RAISE EXCEPTION 'prune_race: the source is already incomplete before the run: %', btracker_backend.cleared_accounts_source_gap(_schema);
  END IF;
  _repaired := btracker_backend.apply_cleared_accounts_repair(_schema);
  IF _repaired THEN
    RAISE EXCEPTION 'prune_race: the repair reported success although HAF was pruned while it ran';
  END IF;
  IF EXISTS (SELECT 1 FROM btracker_repairs WHERE name = 'cleared_accounts') THEN
    RAISE EXCEPTION 'prune_race: a repair was recorded although HAF was pruned while it ran';
  END IF;
  IF pg_temp.digest() <> (SELECT d FROM t_before) THEN
    RAISE EXCEPTION 'prune_race: the repair left changes although HAF was pruned while it ran';
  END IF;
  RAISE NOTICE 'prune_race OK: pruning committed during the repair -> not repaired, every write rolled back, nothing recorded';
END
$$;
ROLLBACK;
SQL
REPAIR=$!
wait_for "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name = 'bt61_prune_race_repair' AND wait_event_type = 'Lock')" \
  "the repair to block on null's account_withdraws row (its last write)"

# ---------------------------------------------------------------- 3. pruning, committed
psql_run -f - <<SQL
BEGIN;
INSERT INTO public.bt61_prune_race_first_available VALUES ($NEEDED + 1);
INSERT INTO public.bt61_prune_race_signal VALUES ('pruned');
COMMIT;
SQL

# ---------------------------------------------------------------- 4. outcome
rc=0
wait "$HOLDER" || { echo "prune_race: holder session failed"; rc=1; }
wait "$REPAIR" || { echo "prune_race: repair session failed"; rc=1; }
grep -q "prune_race OK" "$WORK/repair.log" || { echo "prune_race: no OK line from the repair session"; rc=1; }
[[ $rc == 0 ]] && cat "$WORK/repair.log"
exit $rc

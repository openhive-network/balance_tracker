/*
 * cleared_accounts_repair.sql
 * ===========================
 * The #61 repair (btracker_backend.rebuild_cleared_accounts, run once per install by
 * repair_cleared_accounts) against a clean replay:
 *
 *   1. snapshot what this code produced for the accounts that clearing ops emptied
 *      ('null', 'steem.dao'): liquid and savings history, current balances, savings,
 *      balance_totals, day/month rollups, pending rewards, delayed vests, HBD-interest
 *      last balance;
 *   2. corrupt it into what a version without the clears wrote: no clear rows, every credit
 *      accumulating, rollups and balance_totals to match, pending rewards and delayed vests
 *      still holding what the clears emptied;
 *   3. repair, and require every table to equal the snapshot; repair again (idempotent);
 *   4. the once-per-install gate: a recorded repair is a no-op; without the record the
 *      finalize path (apply_cleared_accounts_repair) repairs and records it, and so does the
 *      installer path (the repair_cleared_accounts procedure: detach, rebuild, attach) -
 *      leaving the context attached and nothing of the rewrite in the shadow table.
 *
 * A pruned HAF (simulated: first_available_block overridden inside a savepoint) must leave
 * the corrupted data untouched and unrecorded on both paths, and the rebuild must refuse.
 * Pruning that advances WHILE the repair runs is cleared_accounts_prune_race.sh (it needs
 * two sessions).
 *
 * Not compared: hive_rowid (HAF's row id, renumbered by a re-insert). The mock has no HF25
 * entry, so the HF25 bound on the HBD-interest replay is not exercised here; steem.dao holds
 * no pending rewards or delayed vests in the mock.
 *
 * Everything runs in a transaction that is rolled back.
 * Expects psql variable `schema` (the application schema, also its context name) and a
 * synced, finalized schema. With `-v require_clears=1` (the mock lane, whose fixture has
 * clears) it fails when there is nothing to repair; without it, a chain with no clearing ops
 * (the 5M-block replay) skips with a notice.
 */

\set ON_ERROR_STOP on
SET search_path TO :"schema", public;
\if :{?require_clears}
\else
  \set require_clears 0
\endif
SELECT set_config('btracker_test.schema', :'schema', false);
SELECT set_config('btracker_test.require_clears', :'require_clears', false);
BEGIN;

-- The accounts the clearing ops empty. Not every account with a row written by such an op:
-- hive.fund is credited by each consolidate and is no part of the repair.
CREATE TEMP TABLE t_accounts ON COMMIT DROP AS
SELECT DISTINCT h.account
FROM account_balance_history h
JOIN accounts_view av ON av.id = h.account AND av.name IN ('null', 'steem.dao')
WHERE h.op_type_id IN (btracker_backend.op_clear_null_account_balance(),
                       btracker_backend.op_consolidate_treasury_balance());

SELECT count(*) > 0 AS has_clears FROM t_accounts \gset
\if :has_clears
\else
  DO $$
  BEGIN
    IF current_setting('btracker_test.require_clears', true) = '1' THEN
      RAISE EXCEPTION 'cleared_accounts_repair: no account has a clear row - the test would prove nothing';
    END IF;
    RAISE NOTICE 'cleared_accounts_repair: no clearing ops on this chain - skipped';
  END
  $$;
  ROLLBACK;
  \quit
\endif

-- ---------------------------------------------------------------- 1. snapshot
CREATE TEMP TABLE s_hist ON COMMIT DROP AS SELECT h.account, h.nai, h.op_type_id, h.balance_seq_no, h.source_op, h.balance FROM account_balance_history h WHERE h.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_sav ON COMMIT DROP AS SELECT h.account, h.nai, h.op_type_id, h.balance_seq_no, h.source_op, h.balance FROM account_savings_history h WHERE h.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_cab ON COMMIT DROP AS SELECT c.account, c.nai, c.balance, c.balance_change_count, c.source_op FROM current_account_balances c WHERE c.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_acs ON COMMIT DROP AS SELECT c.account, c.nai, c.balance, c.balance_change_count, c.source_op, c.savings_withdraw_requests FROM account_savings c WHERE c.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_bt ON COMMIT DROP AS SELECT b.nai, b.holders, b.total FROM balance_totals b;
CREATE TEMP TABLE s_bday ON COMMIT DROP AS SELECT d.account, d.nai, d.updated_at, d.balance, d.min_balance, d.max_balance, d.source_op FROM balance_history_by_day d WHERE d.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_bmon ON COMMIT DROP AS SELECT d.account, d.nai, d.updated_at, d.balance, d.min_balance, d.max_balance, d.source_op FROM balance_history_by_month d WHERE d.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_sday ON COMMIT DROP AS SELECT d.account, d.nai, d.updated_at, d.balance, d.min_balance, d.max_balance, d.source_op FROM saving_history_by_day d WHERE d.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_smon ON COMMIT DROP AS SELECT d.account, d.nai, d.updated_at, d.balance, d.min_balance, d.max_balance, d.source_op FROM saving_history_by_month d WHERE d.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_rew ON COMMIT DROP AS SELECT r.account, r.nai, r.balance FROM account_rewards r WHERE r.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_wd ON COMMIT DROP AS SELECT w.account, w.delayed_vests FROM account_withdraws w WHERE w.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_hbd ON COMMIT DROP AS SELECT i.account, i.hbd_seconds, i.hbd_seconds_last_update, i.last_balance, i.hbd_last_interest_payment FROM account_hbd_interest i WHERE i.account IN (SELECT account FROM t_accounts);

-- Differences between the live tables and the snapshot, one line per table that differs.
-- Rollups compare their own columns: HAF adds hive_rowid to registered tables, and a
-- re-insert renumbers it.
CREATE FUNCTION pg_temp.repair_mismatches()
RETURNS TEXT[] LANGUAGE plpgsql AS $f$
DECLARE
  _out TEXT[] := '{}';
  _n BIGINT;
  _sample TEXT;
  _q TEXT;
  _t RECORD;
BEGIN
  FOR _t IN SELECT * FROM (VALUES
    ('account_balance_history', 'SELECT h.account, h.nai, h.op_type_id, h.balance_seq_no, h.source_op, h.balance FROM account_balance_history h WHERE h.account IN (SELECT account FROM t_accounts)', 's_hist'),
    ('account_savings_history', 'SELECT h.account, h.nai, h.op_type_id, h.balance_seq_no, h.source_op, h.balance FROM account_savings_history h WHERE h.account IN (SELECT account FROM t_accounts)', 's_sav'),
    ('current_account_balances', 'SELECT c.account, c.nai, c.balance, c.balance_change_count, c.source_op FROM current_account_balances c WHERE c.account IN (SELECT account FROM t_accounts)', 's_cab'),
    ('account_savings', 'SELECT c.account, c.nai, c.balance, c.balance_change_count, c.source_op, c.savings_withdraw_requests FROM account_savings c WHERE c.account IN (SELECT account FROM t_accounts)', 's_acs'),
    ('balance_totals', 'SELECT b.nai, b.holders, b.total FROM balance_totals b', 's_bt'),
    ('balance_history_by_day', 'SELECT d.account, d.nai, d.updated_at, d.balance, d.min_balance, d.max_balance, d.source_op FROM balance_history_by_day d WHERE d.account IN (SELECT account FROM t_accounts)', 's_bday'),
    ('balance_history_by_month', 'SELECT d.account, d.nai, d.updated_at, d.balance, d.min_balance, d.max_balance, d.source_op FROM balance_history_by_month d WHERE d.account IN (SELECT account FROM t_accounts)', 's_bmon'),
    ('saving_history_by_day', 'SELECT d.account, d.nai, d.updated_at, d.balance, d.min_balance, d.max_balance, d.source_op FROM saving_history_by_day d WHERE d.account IN (SELECT account FROM t_accounts)', 's_sday'),
    ('saving_history_by_month', 'SELECT d.account, d.nai, d.updated_at, d.balance, d.min_balance, d.max_balance, d.source_op FROM saving_history_by_month d WHERE d.account IN (SELECT account FROM t_accounts)', 's_smon'),
    ('account_rewards', 'SELECT r.account, r.nai, r.balance FROM account_rewards r WHERE r.account IN (SELECT account FROM t_accounts)', 's_rew'),
    ('account_withdraws.delayed_vests', 'SELECT w.account, w.delayed_vests FROM account_withdraws w WHERE w.account IN (SELECT account FROM t_accounts)', 's_wd'),
    ('account_hbd_interest', 'SELECT i.account, i.hbd_seconds, i.hbd_seconds_last_update, i.last_balance, i.hbd_last_interest_payment FROM account_hbd_interest i WHERE i.account IN (SELECT account FROM t_accounts)', 's_hbd')
  ) AS v(label, live, snap)
  LOOP
    _q := format('SELECT count(*), string_agg(x.side || x.r, '' | '') FILTER (WHERE x.rn <= 4) FROM ('
                 '  SELECT y.*, ROW_NUMBER() OVER (PARTITION BY y.side ORDER BY y.r) AS rn FROM ('
                 '    SELECT ''now '' AS side, l::TEXT AS r FROM (%1$s EXCEPT ALL SELECT * FROM %2$s) l'
                 '    UNION ALL'
                 '    SELECT ''want '', s::TEXT FROM (SELECT * FROM %2$s EXCEPT ALL %1$s) s) y) x', _t.live, _t.snap);
    EXECUTE _q INTO _n, _sample;
    IF _n > 0 THEN
      _out := _out || format('%s: %s row(s) differ, e.g. %s', _t.label, _n, _sample);
    END IF;
  END LOOP;
  RETURN _out;
END
$f$;

-- One hash over every compared table's full content (not a sample) plus the context row:
-- "nothing changed" checks.
CREATE FUNCTION pg_temp.state_digest()
RETURNS TEXT LANGUAGE plpgsql AS $f$
DECLARE
  _acc TEXT := '';
  _part TEXT;
  _q TEXT;
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
    'SELECT r.name, r.outcome, r.at_block FROM btracker_repairs r',
    format('SELECT c.name, c.current_block_num, c.irreversible_block, c.is_forking, c.events_id FROM hafd.contexts c WHERE c.name = %L',
           current_setting('btracker_test.schema'))
  ] LOOP
    EXECUTE format('SELECT md5(COALESCE(string_agg(x::TEXT, ''|'' ORDER BY x::TEXT), '''')) FROM (%s) x', _q) INTO _part;
    _acc := _acc || _part;
  END LOOP;
  RETURN md5(_acc);
END
$f$;

DO $$
DECLARE
  _null INT := (SELECT av.id FROM accounts_view av WHERE av.name = 'null');
  _dao  INT := (SELECT av.id FROM accounts_view av WHERE av.name = 'steem.dao');
BEGIN
  -- the HBD-interest comparison must have something to compare: an accrual and a reset
  IF NOT EXISTS (SELECT 1 FROM s_hbd WHERE account = _null AND hbd_seconds > 0) THEN
    RAISE EXCEPTION 'cleared_accounts_repair: null has no accrued hbd_seconds in the replay';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM s_hbd WHERE account = _dao AND hbd_last_interest_payment <> 'epoch'::TIMESTAMP) THEN
    RAISE EXCEPTION 'cleared_accounts_repair: steem.dao has no interest reset in the replay';
  END IF;
  IF cardinality(pg_temp.repair_mismatches()) > 0 THEN
    RAISE EXCEPTION 'cleared_accounts_repair: the snapshot does not match itself: %', pg_temp.repair_mismatches();
  END IF;
  -- the corruption below must change something, or the comparison proves nothing
  IF NOT EXISTS (SELECT 1 FROM s_hist WHERE op_type_id IN (btracker_backend.op_clear_null_account_balance(),
                                                           btracker_backend.op_consolidate_treasury_balance())) THEN
    RAISE EXCEPTION 'cleared_accounts_repair: no clear row in the snapshot';
  END IF;
END
$$;

-- ---------------------------------------------------------------- 2. corrupt (pre-#61 state)
-- History without the clears: the same deltas, every credit accumulating, renumbered.
CREATE TEMP TABLE c_hist ON COMMIT DROP AS
WITH d AS (
  SELECT s.*, s.balance - LAG(s.balance, 1, 0::BIGINT) OVER (PARTITION BY s.account, s.nai ORDER BY s.balance_seq_no) AS delta
  FROM s_hist s
)
SELECT account, nai, op_type_id, source_op,
  (SUM(delta) OVER w)::BIGINT AS balance, (ROW_NUMBER() OVER w)::INT AS balance_seq_no
FROM d
WHERE op_type_id NOT IN (btracker_backend.op_clear_null_account_balance(), btracker_backend.op_consolidate_treasury_balance())
WINDOW w AS (PARTITION BY account, nai ORDER BY balance_seq_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

CREATE TEMP TABLE c_sav ON COMMIT DROP AS
WITH d AS (
  SELECT s.*, s.balance - LAG(s.balance, 1, 0::BIGINT) OVER (PARTITION BY s.account, s.nai ORDER BY s.balance_seq_no) AS delta
  FROM s_sav s
)
SELECT account, nai, op_type_id, source_op,
  (SUM(delta) OVER w)::BIGINT AS balance, (ROW_NUMBER() OVER w)::INT AS balance_seq_no
FROM d
WHERE op_type_id NOT IN (btracker_backend.op_clear_null_account_balance(), btracker_backend.op_consolidate_treasury_balance())
WINDOW w AS (PARTITION BY account, nai ORDER BY balance_seq_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

-- balance_totals carried the phantom balances too
UPDATE balance_totals bt
SET holders = bt.holders + x.holders, total = bt.total + x.total
FROM (
  SELECT y.nai, SUM(y.holders) AS holders, SUM(y.total) AS total
  FROM (
    SELECT DISTINCT ON (c.account, c.nai) c.nai, (c.balance > 0)::INT AS holders, c.balance::NUMERIC AS total
    FROM c_hist c ORDER BY c.account, c.nai, c.balance_seq_no DESC
  ) y
  GROUP BY y.nai
) x
WHERE bt.nai = x.nai;
UPDATE balance_totals bt
SET holders = bt.holders - x.holders, total = bt.total - x.total
FROM (SELECT s.nai, SUM((s.balance > 0)::INT) AS holders, SUM(s.balance::NUMERIC) AS total FROM s_cab s GROUP BY s.nai) x
WHERE bt.nai = x.nai;

DELETE FROM account_balance_history h WHERE h.account IN (SELECT account FROM t_accounts);
INSERT INTO account_balance_history (account, nai, op_type_id, balance_seq_no, source_op, balance)
SELECT account, nai, op_type_id, balance_seq_no, source_op, balance FROM c_hist;
DELETE FROM account_savings_history h WHERE h.account IN (SELECT account FROM t_accounts);
INSERT INTO account_savings_history (account, nai, op_type_id, balance_seq_no, source_op, balance)
SELECT account, nai, op_type_id, balance_seq_no, source_op, balance FROM c_sav;

UPDATE current_account_balances cab
SET balance = l.balance, balance_change_count = l.balance_seq_no, source_op = l.source_op
FROM (SELECT DISTINCT ON (account, nai) * FROM c_hist ORDER BY account, nai, balance_seq_no DESC) l
WHERE cab.account = l.account AND cab.nai = l.nai;
UPDATE account_savings acs
SET balance = l.balance, balance_change_count = l.balance_seq_no, source_op = l.source_op
FROM (SELECT DISTINCT ON (account, nai) * FROM c_sav ORDER BY account, nai, balance_seq_no DESC) l
WHERE acs.account = l.account AND acs.nai = l.nai;

-- rollups as the old series produced them
DELETE FROM balance_history_by_day d WHERE d.account IN (SELECT account FROM t_accounts);
DELETE FROM balance_history_by_month d WHERE d.account IN (SELECT account FROM t_accounts);
INSERT INTO balance_history_by_day (account, nai, source_op, updated_at, balance, min_balance, max_balance)
SELECT h.account, h.nai, (array_agg(h.source_op ORDER BY h.balance_seq_no DESC))[1], date_trunc('day', bv.created_at),
       (array_agg(h.balance ORDER BY h.balance_seq_no DESC))[1], MIN(h.balance), MAX(h.balance)
FROM c_hist h JOIN blocks_view bv ON bv.num = hafd.operation_id_to_block_num(h.source_op)
GROUP BY h.account, h.nai, date_trunc('day', bv.created_at);
INSERT INTO balance_history_by_month (account, nai, source_op, updated_at, balance, min_balance, max_balance)
SELECT h.account, h.nai, (array_agg(h.source_op ORDER BY h.balance_seq_no DESC))[1], date_trunc('month', bv.created_at),
       (array_agg(h.balance ORDER BY h.balance_seq_no DESC))[1], MIN(h.balance), MAX(h.balance)
FROM c_hist h JOIN blocks_view bv ON bv.num = hafd.operation_id_to_block_num(h.source_op)
GROUP BY h.account, h.nai, date_trunc('month', bv.created_at);
DELETE FROM saving_history_by_day d WHERE d.account IN (SELECT account FROM t_accounts);
DELETE FROM saving_history_by_month d WHERE d.account IN (SELECT account FROM t_accounts);
INSERT INTO saving_history_by_day (account, nai, source_op, updated_at, balance, min_balance, max_balance)
SELECT h.account, h.nai, (array_agg(h.source_op ORDER BY h.balance_seq_no DESC))[1], date_trunc('day', bv.created_at),
       (array_agg(h.balance ORDER BY h.balance_seq_no DESC))[1], MIN(h.balance), MAX(h.balance)
FROM c_sav h JOIN blocks_view bv ON bv.num = hafd.operation_id_to_block_num(h.source_op)
GROUP BY h.account, h.nai, date_trunc('day', bv.created_at);
INSERT INTO saving_history_by_month (account, nai, source_op, updated_at, balance, min_balance, max_balance)
SELECT h.account, h.nai, (array_agg(h.source_op ORDER BY h.balance_seq_no DESC))[1], date_trunc('month', bv.created_at),
       (array_agg(h.balance ORDER BY h.balance_seq_no DESC))[1], MIN(h.balance), MAX(h.balance)
FROM c_sav h JOIN blocks_view bv ON bv.num = hafd.operation_id_to_block_num(h.source_op)
GROUP BY h.account, h.nai, date_trunc('month', bv.created_at);

-- pending rewards, delayed vests and the interest anchor still holding what the clears emptied
UPDATE account_rewards r SET balance = r.balance + 777777 WHERE r.account IN (SELECT account FROM t_accounts);
UPDATE account_withdraws w SET delayed_vests = w.delayed_vests + 4000000000 WHERE w.account IN (SELECT account FROM t_accounts);
-- the interest accumulator ran on the phantom balance: every field differs
UPDATE account_hbd_interest i
SET last_balance = i.last_balance + 123456, hbd_seconds = i.hbd_seconds + 987654321,
    hbd_seconds_last_update = i.hbd_seconds_last_update - INTERVAL '1 day',
    hbd_last_interest_payment = 'epoch'::TIMESTAMP
WHERE i.account IN (SELECT account FROM t_accounts);

DO $$
DECLARE _m TEXT[] := pg_temp.repair_mismatches();
BEGIN
  IF NOT ('account_balance_history' = ANY (SELECT split_part(x, ':', 1) FROM unnest(_m) x)) THEN
    RAISE EXCEPTION 'cleared_accounts_repair: the corruption did not change the liquid history (%); the test would prove nothing', _m;
  END IF;
  RAISE NOTICE 'cleared_accounts_repair: corrupted state differs from the replay in: %', _m;
END
$$;

-- ---------------------------------------------------------------- 2b. pruned HAF: hands off
-- Old application history whose blocks and operations HAF no longer holds: simulated by
-- making the first available block later than every history row. Inside a savepoint, so
-- the override and anything the paths do are undone before step 3.
-- The boundary first: the rebuild needs blocks from the one before the accounts' first
-- history row (the HBD-interest replay reads the previous block's time). The mock has no
-- HF25 record, which a pruned HAF must have; one is added for the boundary cases.
SAVEPOINT boundary;
INSERT INTO hafd.applied_hardforks (hardfork_num, block_num, hardfork_vop_id)
SELECT btracker_backend.hf_hbd_interest(), 90000064, hafd.operation_id(90000064, 1)
WHERE NOT EXISTS (SELECT 1 FROM hafd.applied_hardforks WHERE hardfork_num = btracker_backend.hf_hbd_interest());
CREATE TEMP TABLE t_first_needed ON COMMIT DROP AS
SELECT MIN(hafd.operation_id_to_block_num(x.source_op)) AS b FROM (
  SELECT h.source_op FROM account_balance_history h WHERE h.balance_seq_no = 1 AND h.account IN (SELECT account FROM t_accounts)
  UNION ALL
  SELECT h.source_op FROM account_savings_history h WHERE h.balance_seq_no = 1 AND h.account IN (SELECT account FROM t_accounts)) x;
DO $$
DECLARE
  _schema TEXT := current_setting('btracker_test.schema');
  _b INT := (SELECT b FROM t_first_needed);
BEGIN
  IF _b <= 2 THEN
    RAISE EXCEPTION 'cleared_accounts_repair: history starts at block %, the boundary cannot be tested', _b;
  END IF;
  EXECUTE format('CREATE OR REPLACE FUNCTION btracker_backend.first_available_block(_context TEXT) RETURNS INT LANGUAGE sql STABLE AS $f$ SELECT %s $f$', _b - 1);
  IF btracker_backend.cleared_accounts_source_gap(_schema) IS NOT NULL THEN
    RAISE EXCEPTION 'cleared_accounts_repair: data from block % (the one before the history) reported as a gap: %', _b - 1,
      btracker_backend.cleared_accounts_source_gap(_schema);
  END IF;
  EXECUTE format('CREATE OR REPLACE FUNCTION btracker_backend.first_available_block(_context TEXT) RETURNS INT LANGUAGE sql STABLE AS $f$ SELECT %s $f$', _b);
  IF btracker_backend.cleared_accounts_source_gap(_schema) IS NULL THEN
    RAISE EXCEPTION 'cleared_accounts_repair: data only from block % (the first history row) not reported as a gap', _b;
  END IF;
  EXECUTE format('CREATE OR REPLACE FUNCTION btracker_backend.first_available_block(_context TEXT) RETURNS INT LANGUAGE sql STABLE AS $f$ SELECT %s $f$', _b - 1);
  DELETE FROM hafd.applied_hardforks WHERE hardfork_num = btracker_backend.hf_hbd_interest();
  IF btracker_backend.cleared_accounts_source_gap(_schema) IS NULL THEN
    RAISE EXCEPTION 'cleared_accounts_repair: a pruned HAF without its HF25 record not reported as a gap';
  END IF;
  RAISE NOTICE 'cleared_accounts_repair: source boundary OK (block % complete, % not, missing HF25 record caught)', _b - 1, _b;
END
$$;
ROLLBACK TO SAVEPOINT boundary;

SAVEPOINT pruned;
CREATE OR REPLACE FUNCTION btracker_backend.first_available_block(_context TEXT)
RETURNS INT LANGUAGE sql STABLE AS $$ SELECT 2147483647 $$;
DELETE FROM btracker_repairs WHERE name = 'cleared_accounts';
CREATE TEMP TABLE p_before ON COMMIT DROP AS SELECT pg_temp.state_digest() AS d;
DO $$
DECLARE
  _schema TEXT := current_setting('btracker_test.schema');
  _failed BOOLEAN := FALSE;
BEGIN
  IF btracker_backend.cleared_accounts_source_gap(_schema) IS NULL THEN
    RAISE EXCEPTION 'cleared_accounts_repair: the simulated pruned HAF reports no source gap';
  END IF;
  IF btracker_backend.apply_cleared_accounts_repair(_schema) THEN
    RAISE EXCEPTION 'cleared_accounts_repair: the finalize path repaired on a pruned HAF';
  END IF;
  CALL btracker_backend.repair_cleared_accounts(_schema, FALSE);
  IF EXISTS (SELECT 1 FROM btracker_repairs WHERE name = 'cleared_accounts') THEN
    RAISE EXCEPTION 'cleared_accounts_repair: a repair was recorded on a pruned HAF';
  END IF;
  IF NOT hive.app_context_is_attached(_schema) THEN
    RAISE EXCEPTION 'cleared_accounts_repair: the installer path detached context % on a pruned HAF', _schema;
  END IF;
  IF pg_temp.state_digest() <> (SELECT d FROM p_before) THEN
    RAISE EXCEPTION 'cleared_accounts_repair: data or the context row changed on a pruned HAF';
  END IF;
  BEGIN
    PERFORM btracker_backend.rebuild_cleared_accounts(_schema);
  EXCEPTION WHEN SQLSTATE 'BT061' THEN
    _failed := SQLERRM LIKE '%pruned HAF%';
  END;
  IF NOT _failed THEN
    RAISE EXCEPTION 'cleared_accounts_repair: rebuild_cleared_accounts ran on a pruned HAF';
  END IF;
  RAISE NOTICE 'cleared_accounts_repair: pruned HAF OK (nothing changed, nothing recorded, rebuild refuses)';
END
$$;
ROLLBACK TO SAVEPOINT pruned;

-- ---------------------------------------------------------------- 3. repair, twice
DO $$
DECLARE _m TEXT[];
BEGIN
  PERFORM btracker_backend.rebuild_cleared_accounts(current_setting('btracker_test.schema'));
  _m := pg_temp.repair_mismatches();
  IF cardinality(_m) > 0 THEN
    RAISE EXCEPTION 'cleared_accounts_repair: repaired data differs from the clean replay: %', _m;
  END IF;
  PERFORM btracker_backend.rebuild_cleared_accounts(current_setting('btracker_test.schema'));
  _m := pg_temp.repair_mismatches();
  IF cardinality(_m) > 0 THEN
    RAISE EXCEPTION 'cleared_accounts_repair: a second repair changed the data: %', _m;
  END IF;
  RAISE NOTICE 'cleared_accounts_repair: repair equals the clean replay, and repeating it changes nothing';
END
$$;

-- ---------------------------------------------------------------- 3b. the HF25 bound of the interest replay
-- (the mock has no HF25 record). At block 90000058 every HBD row of the two accounts is at
-- or below it: the replay must still equal the clean one. At 90000057 nothing of steem.dao
-- is (its row goes) and null's accumulator stops at its pre-mock history.
SAVEPOINT hf25;
INSERT INTO hafd.applied_hardforks (hardfork_num, block_num, hardfork_vop_id)
VALUES (btracker_backend.hf_hbd_interest(), 90000058, hafd.operation_id(90000058, 1));
DO $$
BEGIN
  PERFORM btracker_backend.rebuild_cleared_accounts(current_setting('btracker_test.schema'));
  IF EXISTS (SELECT i.account, i.hbd_seconds, i.hbd_seconds_last_update, i.last_balance, i.hbd_last_interest_payment
             FROM account_hbd_interest i WHERE i.account IN (SELECT account FROM t_accounts)
             EXCEPT SELECT * FROM s_hbd)
     OR EXISTS (SELECT * FROM s_hbd
                EXCEPT SELECT i.account, i.hbd_seconds, i.hbd_seconds_last_update, i.last_balance, i.hbd_last_interest_payment
                FROM account_hbd_interest i WHERE i.account IN (SELECT account FROM t_accounts)) THEN
    RAISE EXCEPTION 'cleared_accounts_repair: HF25 at block 90000058 changed the interest replay';
  END IF;
END
$$;
UPDATE hafd.applied_hardforks SET block_num = 90000057, hardfork_vop_id = hafd.operation_id(90000057, 1)
WHERE hardfork_num = btracker_backend.hf_hbd_interest();
DO $$
DECLARE
  _null INT := (SELECT av.id FROM accounts_view av WHERE av.name = 'null');
  _dao  INT := (SELECT av.id FROM accounts_view av WHERE av.name = 'steem.dao');
  _want BIGINT := (SELECT h.balance FROM s_hist h WHERE h.account = _null AND h.nai = 13
                   AND hafd.operation_id_to_block_num(h.source_op) <= 90000057
                   ORDER BY h.balance_seq_no DESC LIMIT 1);
BEGIN
  PERFORM btracker_backend.rebuild_cleared_accounts(current_setting('btracker_test.schema'));
  IF EXISTS (SELECT 1 FROM account_hbd_interest WHERE account = _dao) THEN
    RAISE EXCEPTION 'cleared_accounts_repair: steem.dao keeps an interest row although none of its HBD rows is at or below HF25';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM account_hbd_interest WHERE account = _null AND last_balance = _want
                 AND hbd_seconds_last_update < TIMESTAMP '2016-09-16') THEN
    RAISE EXCEPTION 'cleared_accounts_repair: the interest replay of null did not stop at HF25 (want last_balance %, got %)', _want,
      (SELECT row(i.*)::TEXT FROM account_hbd_interest i WHERE i.account = _null);
  END IF;
  RAISE NOTICE 'cleared_accounts_repair: HF25 bound OK (at 90000058 same as the replay; at 90000057 the steem.dao row goes and null stops at %)', _want;
END
$$;
ROLLBACK TO SAVEPOINT hf25;

-- ---------------------------------------------------------------- 4. the once-per-install gate
DO $$
DECLARE
  _schema TEXT := current_setting('btracker_test.schema');
  _shadow TEXT := hive.get_shadow_table_name(_schema, 'account_balance_history');
  _logged BIGINT;
BEGIN
  -- a recorded repair (here the fresh install's record) is a no-op even on corrupted data
  IF NOT EXISTS (SELECT 1 FROM btracker_repairs WHERE name = 'cleared_accounts') THEN
    RAISE EXCEPTION 'cleared_accounts_repair: a fresh install did not record btracker_repairs.cleared_accounts';
  END IF;
  UPDATE account_rewards r SET balance = r.balance + 1 WHERE r.account IN (SELECT account FROM t_accounts);
  CALL btracker_backend.repair_cleared_accounts(_schema, FALSE);
  IF NOT ('account_rewards' = ANY (SELECT split_part(x, ':', 1) FROM unnest(pg_temp.repair_mismatches()) x)) THEN
    RAISE EXCEPTION 'cleared_accounts_repair: repair_cleared_accounts ran although the repair is recorded';
  END IF;

  -- the finalize path (no detach): repairs and records
  DELETE FROM btracker_repairs WHERE name = 'cleared_accounts';
  IF NOT btracker_backend.apply_cleared_accounts_repair(_schema) THEN
    RAISE EXCEPTION 'cleared_accounts_repair: apply_cleared_accounts_repair did not repair without a record';
  END IF;
  IF cardinality(pg_temp.repair_mismatches()) > 0 THEN
    RAISE EXCEPTION 'cleared_accounts_repair: the finalize path left differences: %', pg_temp.repair_mismatches();
  END IF;

  -- the installer path on a forking, attached context: detach, rebuild, record, attach -
  -- and nothing of the rewrite in the fork-rewind shadow table (a rebuild that ran attached
  -- would have logged every deleted and inserted row of the two accounts there)
  IF NOT (hive.app_is_forking(_schema) AND hive.app_context_is_attached(_schema)) THEN
    RAISE EXCEPTION 'cleared_accounts_repair: context % is not forking and attached; the installer path would not be exercised', _schema;
  END IF;
  DELETE FROM btracker_repairs WHERE name = 'cleared_accounts';
  UPDATE account_rewards r SET balance = r.balance + 1 WHERE r.account IN (SELECT account FROM t_accounts);
  CALL btracker_backend.repair_cleared_accounts(_schema, FALSE);
  IF cardinality(pg_temp.repair_mismatches()) > 0 THEN
    RAISE EXCEPTION 'cleared_accounts_repair: the installer path left differences: %', pg_temp.repair_mismatches();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM btracker_repairs WHERE name = 'cleared_accounts' AND outcome LIKE 'repaired:%') THEN
    RAISE EXCEPTION 'cleared_accounts_repair: the installer path did not record the repair';
  END IF;
  IF NOT hive.app_context_is_attached(_schema) THEN
    RAISE EXCEPTION 'cleared_accounts_repair: the installer path left context % detached', _schema;
  END IF;
  EXECUTE format('SELECT count(*) FROM hafd.%I WHERE account IN (SELECT account FROM t_accounts)', _shadow) INTO _logged;
  IF _logged > 0 THEN
    RAISE EXCEPTION 'cleared_accounts_repair: % row(s) of the repair went into the shadow table %', _logged, _shadow;
  END IF;
  RAISE NOTICE 'cleared_accounts_repair: gate OK (recorded = no-op; finalize path repairs; installer path repairs detached, records, re-attaches)';
END
$$;

ROLLBACK;

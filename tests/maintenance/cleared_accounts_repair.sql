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
 *   4. through repair_cleared_accounts: a recorded repair is a no-op; with the record gone
 *      the installer path (detach, rebuild, attach) repairs and records it.
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
CREATE TEMP TABLE s_hbd ON COMMIT DROP AS SELECT i.account, i.last_balance FROM account_hbd_interest i WHERE i.account IN (SELECT account FROM t_accounts);

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
    ('account_hbd_interest.last_balance', 'SELECT i.account, i.last_balance FROM account_hbd_interest i WHERE i.account IN (SELECT account FROM t_accounts)', 's_hbd')
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

DO $$
BEGIN
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
UPDATE account_hbd_interest i SET last_balance = i.last_balance + 123456 WHERE i.account IN (SELECT account FROM t_accounts);

DO $$
DECLARE _m TEXT[] := pg_temp.repair_mismatches();
BEGIN
  IF NOT ('account_balance_history' = ANY (SELECT split_part(x, ':', 1) FROM unnest(_m) x)) THEN
    RAISE EXCEPTION 'cleared_accounts_repair: the corruption did not change the liquid history (%); the test would prove nothing', _m;
  END IF;
  RAISE NOTICE 'cleared_accounts_repair: corrupted state differs from the replay in: %', _m;
END
$$;

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

-- ---------------------------------------------------------------- 4. the once-per-install gate
DO $$
DECLARE _schema TEXT := current_setting('btracker_test.schema');
BEGIN
  -- a recorded repair (here the fresh install's record) is a no-op even on corrupted data
  IF NOT EXISTS (SELECT 1 FROM btracker_repairs WHERE name = 'cleared_accounts') THEN
    RAISE EXCEPTION 'cleared_accounts_repair: a fresh install did not record btracker_repairs.cleared_accounts';
  END IF;
  UPDATE account_rewards r SET balance = r.balance + 1 WHERE r.account IN (SELECT account FROM t_accounts);
  PERFORM btracker_backend.repair_cleared_accounts(_schema, TRUE);
  IF NOT ('account_rewards' = ANY (SELECT split_part(x, ':', 1) FROM unnest(pg_temp.repair_mismatches()) x)) THEN
    RAISE EXCEPTION 'cleared_accounts_repair: repair_cleared_accounts ran although the repair is recorded';
  END IF;

  -- without the record, the installer path (detach, rebuild, attach) repairs and records it
  DELETE FROM btracker_repairs WHERE name = 'cleared_accounts';
  PERFORM btracker_backend.repair_cleared_accounts(_schema, TRUE);
  IF cardinality(pg_temp.repair_mismatches()) > 0 THEN
    RAISE EXCEPTION 'cleared_accounts_repair: the installer path left differences: %', pg_temp.repair_mismatches();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM btracker_repairs WHERE name = 'cleared_accounts' AND outcome LIKE 'repaired:%') THEN
    RAISE EXCEPTION 'cleared_accounts_repair: the installer path did not record the repair';
  END IF;
  IF NOT hive.app_context_is_attached(_schema) THEN
    RAISE EXCEPTION 'cleared_accounts_repair: the installer path left context % detached', _schema;
  END IF;
  RAISE NOTICE 'cleared_accounts_repair: gate OK (recorded = no-op; installer path repairs, records, re-attaches)';
END
$$;

ROLLBACK;

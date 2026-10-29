/*
 * cleared_accounts_batch.sql
 * ==========================
 * The #61 clears processed as one multi-block range (massive sync) must leave 'null' and
 * 'steem.dao' exactly as block-by-block processing (LIVE) did:
 *
 *   1. snapshot the two accounts' state after the normal sync (the mock blocks are
 *      processed one block at a time): liquid and savings history from the first clearing
 *      block on, current balances, savings, pending rewards, delayed vests, and the liquid
 *      day rollups of the days involved (month and savings rollups are not compared);
 *   2. take the two accounts back to the block before the first clearing block (history from
 *      it on deleted, current rows from the last remaining history row);
 *   3. process every block from it to the head as ONE range with the balances, withdrawals,
 *      savings and rewards stages, and require the two accounts to equal the snapshot.
 *
 * Pending rewards and delayed vests are not taken back: the range holds clears of these
 * accounts, which empty both, so the result does not depend on their starting value - and
 * delayed vests start from a large wrong value to show it. For 'null' the range holds four
 * clears (blocks ...58, 59, 61, 63), delayed-vests credits before and after them, the last
 * after the last clear, and after that clear a delayed_voting debit (1e9) larger than what
 * is pending (0: clamped) but smaller than what a missed reset would leave (2e9), so a clear
 * that is not applied changes the result instead of being hidden by the clamp:
 * process_withdrawals folds only the last clear of the range, which must give the same
 * value as one clear per block. (Block ...63 holds no delayed op of null: in the
 * block-by-block sync that made the snapshot, its reset comes from the stored-value arm of
 * process_withdrawals' guard - without it the snapshot would hold 3e9, not 2e9.)
 * Other accounts are processed a second time by step 3 and are not compared (neither is
 * balance_totals); everything runs in a transaction that is rolled back. The range runs
 * with the LIVE layout (indexes present, rollups maintained per range), not massive sync's
 * (indexes dropped, rollups deferred to finalize): it covers the multi-block code paths of
 * the clears, not massive sync's table layout.
 * Expects psql variable `schema`. With `-v require_clears=1` it fails when the chain has no
 * clearing op; otherwise it skips with a notice.
 */

\set ON_ERROR_STOP on
SET search_path TO :"schema", public;
\if :{?require_clears}
\else
  \set require_clears 0
\endif
SELECT set_config('btracker_test.require_clears', :'require_clears', false);
BEGIN;

CREATE TEMP TABLE t_accounts ON COMMIT DROP AS
SELECT av.id AS account FROM accounts_view av WHERE av.name IN ('null', 'steem.dao');

-- the first clearing block of these accounts, and the processed head
CREATE TEMP TABLE t_range ON COMMIT DROP AS
SELECT
  (SELECT MIN(hafd.operation_id_to_block_num(h.source_op))
   FROM account_balance_history h
   WHERE h.account IN (SELECT account FROM t_accounts)
     AND h.op_type_id IN (btracker_backend.op_clear_null_account_balance(),
                          btracker_backend.op_consolidate_treasury_balance())) AS from_block,
  (SELECT c.current_block_num FROM hafd.contexts c WHERE c.name = :'schema') AS to_block;

SELECT (from_block IS NOT NULL) AS has_clears FROM t_range \gset
\if :has_clears
\else
  DO $$
  BEGIN
    IF current_setting('btracker_test.require_clears', true) = '1' THEN
      RAISE EXCEPTION 'cleared_accounts_batch: no clear row for null/steem.dao - the test would prove nothing';
    END IF;
    RAISE NOTICE 'cleared_accounts_batch: no clearing ops on this chain - skipped';
  END
  $$;
  ROLLBACK;
  \quit
\endif

SELECT from_block, to_block FROM t_range;

-- ---------------------------------------------------------------- 1. snapshot (LIVE result)
CREATE TEMP TABLE s_hist ON COMMIT DROP AS
SELECT h.account, h.nai, h.op_type_id, h.balance_seq_no, h.source_op, h.balance FROM account_balance_history h
WHERE h.account IN (SELECT account FROM t_accounts) AND hafd.operation_id_to_block_num(h.source_op) >= (SELECT from_block FROM t_range);
CREATE TEMP TABLE s_sav ON COMMIT DROP AS
SELECT h.account, h.nai, h.op_type_id, h.balance_seq_no, h.source_op, h.balance FROM account_savings_history h
WHERE h.account IN (SELECT account FROM t_accounts) AND hafd.operation_id_to_block_num(h.source_op) >= (SELECT from_block FROM t_range);
CREATE TEMP TABLE s_cab ON COMMIT DROP AS SELECT c.account, c.nai, c.balance, c.balance_change_count, c.source_op FROM current_account_balances c WHERE c.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_acs ON COMMIT DROP AS SELECT c.account, c.nai, c.balance, c.balance_change_count, c.source_op, c.savings_withdraw_requests FROM account_savings c WHERE c.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_rew ON COMMIT DROP AS SELECT r.account, r.nai, r.balance FROM account_rewards r WHERE r.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_wd ON COMMIT DROP AS SELECT w.account, w.delayed_vests FROM account_withdraws w WHERE w.account IN (SELECT account FROM t_accounts);
-- rollups of the days of the range (earlier days are untouched by step 3)
CREATE TEMP TABLE s_bday ON COMMIT DROP AS
SELECT d.account, d.nai, d.updated_at, d.balance, d.min_balance, d.max_balance, d.source_op FROM balance_history_by_day d
WHERE d.account IN (SELECT account FROM t_accounts)
  AND d.updated_at >= (SELECT date_trunc('day', bv.created_at) FROM blocks_view bv WHERE bv.num = (SELECT from_block FROM t_range));

CREATE FUNCTION pg_temp.batch_mismatches()
RETURNS TEXT[] LANGUAGE plpgsql AS $f$
DECLARE
  _out TEXT[] := '{}';
  _n BIGINT;
  _sample TEXT;
  _t RECORD;
BEGIN
  FOR _t IN SELECT * FROM (VALUES
    ('account_balance_history', 'SELECT h.account, h.nai, h.op_type_id, h.balance_seq_no, h.source_op, h.balance FROM account_balance_history h WHERE h.account IN (SELECT account FROM t_accounts) AND hafd.operation_id_to_block_num(h.source_op) >= (SELECT from_block FROM t_range)', 's_hist'),
    ('account_savings_history', 'SELECT h.account, h.nai, h.op_type_id, h.balance_seq_no, h.source_op, h.balance FROM account_savings_history h WHERE h.account IN (SELECT account FROM t_accounts) AND hafd.operation_id_to_block_num(h.source_op) >= (SELECT from_block FROM t_range)', 's_sav'),
    ('current_account_balances', 'SELECT c.account, c.nai, c.balance, c.balance_change_count, c.source_op FROM current_account_balances c WHERE c.account IN (SELECT account FROM t_accounts)', 's_cab'),
    ('account_savings', 'SELECT c.account, c.nai, c.balance, c.balance_change_count, c.source_op, c.savings_withdraw_requests FROM account_savings c WHERE c.account IN (SELECT account FROM t_accounts)', 's_acs'),
    ('account_rewards', 'SELECT r.account, r.nai, r.balance FROM account_rewards r WHERE r.account IN (SELECT account FROM t_accounts)', 's_rew'),
    ('account_withdraws.delayed_vests', 'SELECT w.account, w.delayed_vests FROM account_withdraws w WHERE w.account IN (SELECT account FROM t_accounts)', 's_wd'),
    ('balance_history_by_day', 'SELECT d.account, d.nai, d.updated_at, d.balance, d.min_balance, d.max_balance, d.source_op FROM balance_history_by_day d WHERE d.account IN (SELECT account FROM t_accounts) AND d.updated_at >= (SELECT date_trunc(''day'', bv.created_at) FROM blocks_view bv WHERE bv.num = (SELECT from_block FROM t_range))', 's_bday')
  ) AS v(label, live, snap)
  LOOP
    EXECUTE format('SELECT count(*), string_agg(x.side || x.r, '' | '') FILTER (WHERE x.rn <= 4) FROM ('
                   '  SELECT y.*, ROW_NUMBER() OVER (PARTITION BY y.side ORDER BY y.r) AS rn FROM ('
                   '    SELECT ''now '' AS side, l::TEXT AS r FROM (%1$s EXCEPT ALL SELECT * FROM %2$s) l'
                   '    UNION ALL'
                   '    SELECT ''want '', s::TEXT FROM (SELECT * FROM %2$s EXCEPT ALL %1$s) s) y) x', _t.live, _t.snap)
      INTO _n, _sample;
    IF _n > 0 THEN
      _out := _out || format('%s: %s row(s) differ, e.g. %s', _t.label, _n, _sample);
    END IF;
  END LOOP;
  RETURN _out;
END
$f$;

-- ---------------------------------------------------------------- 2. back to before the range
DELETE FROM account_balance_history h
WHERE h.account IN (SELECT account FROM t_accounts) AND hafd.operation_id_to_block_num(h.source_op) >= (SELECT from_block FROM t_range);
DELETE FROM account_savings_history h
WHERE h.account IN (SELECT account FROM t_accounts) AND hafd.operation_id_to_block_num(h.source_op) >= (SELECT from_block FROM t_range);

DELETE FROM current_account_balances c
WHERE c.account IN (SELECT account FROM t_accounts)
  AND NOT EXISTS (SELECT 1 FROM account_balance_history h WHERE h.account = c.account AND h.nai = c.nai);
UPDATE current_account_balances c
SET balance = l.balance, balance_change_count = l.balance_seq_no, source_op = l.source_op
FROM (SELECT DISTINCT ON (h.account, h.nai) h.account, h.nai, h.balance, h.balance_seq_no, h.source_op
      FROM account_balance_history h WHERE h.account IN (SELECT account FROM t_accounts)
      ORDER BY h.account, h.nai, h.balance_seq_no DESC) l
WHERE c.account = l.account AND c.nai = l.nai;

DELETE FROM account_savings c
WHERE c.account IN (SELECT account FROM t_accounts)
  AND NOT EXISTS (SELECT 1 FROM account_savings_history h WHERE h.account = c.account AND h.nai = c.nai);
UPDATE account_savings c
SET balance = l.balance, balance_change_count = l.balance_seq_no, source_op = l.source_op
FROM (SELECT DISTINCT ON (h.account, h.nai) h.account, h.nai, h.balance, h.balance_seq_no, h.source_op
      FROM account_savings_history h WHERE h.account IN (SELECT account FROM t_accounts)
      ORDER BY h.account, h.nai, h.balance_seq_no DESC) l
WHERE c.account = l.account AND c.nai = l.nai;

DELETE FROM balance_history_by_day d
WHERE d.account IN (SELECT account FROM t_accounts)
  AND d.updated_at >= (SELECT date_trunc('day', bv.created_at) FROM blocks_view bv WHERE bv.num = (SELECT from_block FROM t_range));

-- a stored delayed-vests value of null's that the range's clears must empty (the
-- snapshot is what block-by-block processing left; this value must not survive into it).
-- Only null's existing row: a row created here for steem.dao, which has none, would differ
-- from the snapshot for a reason that is not under test.
DO $$
DECLARE
  _null INT := (SELECT av.id FROM accounts_view av WHERE av.name = 'null');
BEGIN
  -- non-vacuity: the credit after null's last clear (block ...64) must leave something
  IF NOT EXISTS (SELECT 1 FROM s_wd s WHERE s.account = _null AND s.delayed_vests > 0) THEN
    RAISE EXCEPTION 'cleared_accounts_batch: null has no delayed vests after the mock blocks; the credit after its last clear is not exercised';
  END IF;
  UPDATE account_withdraws aw SET delayed_vests = aw.delayed_vests + 4000000000 WHERE aw.account = _null;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'cleared_accounts_batch: null has no account_withdraws row to start from';
  END IF;
END
$$;

-- ---------------------------------------------------------------- 3. one range, then compare
DO $$
DECLARE
  _from INT := (SELECT from_block FROM t_range);
  _to   INT := (SELECT to_block FROM t_range);
  _m    TEXT[];
BEGIN
  IF _to - _from < 2 THEN
    RAISE EXCEPTION 'cleared_accounts_batch: range %..% is not a multi-block batch', _from, _to;
  END IF;
  PERFORM btracker_prefetch_operations(_from, _to);
  PERFORM process_block_range_balances(_from, _to);
  PERFORM process_block_range_withdrawals(_from, _to);
  PERFORM process_block_range_savings(_from, _to);
  PERFORM process_block_range_rewards(_from, _to);
  _m := pg_temp.batch_mismatches();
  IF cardinality(_m) > 0 THEN
    RAISE EXCEPTION 'cleared_accounts_batch: blocks %..% as one range differ from block-by-block processing: %', _from, _to, _m;
  END IF;
  RAISE NOTICE 'cleared_accounts_batch: blocks %..% as one range equal block-by-block processing for null/steem.dao', _from, _to;
END
$$;

ROLLBACK;

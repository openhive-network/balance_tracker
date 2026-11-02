/*
 * cleared_accounts_batch.sql
 * ==========================
 * The #61 clears of 'null' and 'steem.dao' (clear_null_account_balance /
 * consolidate_treasury_balance) through ordinary block processing:
 *
 *   0. the state the normal install and sync left: finalized and in LIVE. On the
 *      account_clears mock fixture (blocks 90000058-64), its values are checked one by one
 *      (pg_temp.fixture_mismatches below), and so are the HBD-interest accumulator,
 *      balance_totals and the liquid and savings day/month rollups of the accounts;
 *   1. take the two accounts back to the block before their first clearing block (history
 *      from it on deleted, current rows from the last remaining history row, the day
 *      rollups of those days deleted), and seed stored values a clear must not touch or
 *      must work from: null's delayed vests set to a large wrong value, its HBD-interest
 *      accumulator, a pending savings withdrawal, received vests and lifetime rewards;
 *   2. process the blocks from it to the head ONE BLOCK AT A TIME with the balances,
 *      withdrawals, savings and rewards stages; on the fixture, check its values again and
 *      the exact HBD-interest fields and seeded values; snapshot the two accounts;
 *   3. take them back again in the same way, process the same blocks as ONE range, and
 *      require the two accounts to equal the snapshot.
 *
 * Step 2 is the LIVE path: each clear works on what earlier blocks stored. Block ...63
 * holds a clear of null and none of its liquid ops, so its VESTS debit is the stored
 * balance, and its delayed-vests reset comes from the stored-value arm of
 * process_withdrawals' guard. Step 3 is the multi-block path massive sync takes.
 *
 * Pending rewards are not taken back: the range holds clears of these accounts, which empty
 * them, so the result does not depend on their starting value. Delayed vests start from a
 * wrong value to show the same for them. For 'null' the range holds four clears (blocks
 * ...58, 59, 61, 63), delayed-vests credits before and after them, the last after the last
 * clear, and after that clear a delayed_voting debit (1e9) larger than what is pending (0:
 * clamped) but smaller than what a missed reset would leave (2e9), so a clear that is not
 * applied changes the result instead of being hidden by the clamp:
 *   - as one range, process_withdrawals folds only the last clear (...63);
 *   - block by block, block ...63 holds the clear but no delayed op of null, so its reset
 *     comes from the stored-value arm of process_withdrawals' guard.
 * Other accounts are processed again by steps 2 and 3 and are not compared (neither is
 * balance_totals after step 0); everything runs in a transaction that is rolled back.
 * Steps 2 and 3 run with the LIVE layout (indexes present, rollups maintained per range),
 * not massive sync's (indexes dropped, rollups deferred to finalize): they cover the
 * multi-block code paths of the clears, not massive sync's table layout. Step 0 covers
 * what the real sync left, whichever path it took.
 *
 * Expects psql variable `schema`. With `-v require_clears=1` it fails when the chain has no
 * clearing op or is not the account_clears fixture; otherwise it skips (all, or the fixture
 * values) with a notice.
 */

\set ON_ERROR_STOP on
SET search_path TO :"schema", public;
\if :{?require_clears}
\else
  \set require_clears 0
\endif
SELECT set_config('btracker_test.require_clears', :'require_clears', false);
SELECT set_config('btracker_test.schema', :'schema', false);
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

-- The account_clears fixture: its first clearing block is 90000058 and its blocks are
-- 3 s apart (...63 opens the next day). Its values are checked only there.
CREATE TEMP TABLE t_fixture ON COMMIT DROP AS
SELECT
  r.from_block = 90000058 AS is_fixture,
  (SELECT av.id FROM accounts_view av WHERE av.name = 'null')      AS null_id,
  (SELECT av.id FROM accounts_view av WHERE av.name = 'steem.dao') AS dao_id,
  (SELECT av.id FROM accounts_view av WHERE av.name = 'hive.fund') AS fund_id,
  (SELECT av.id FROM accounts_view av WHERE av.name = 'poloniex')  AS polo_id,
  (SELECT bv.created_at FROM blocks_view bv WHERE bv.num = r.from_block - 1) AS t57,
  (SELECT bv.created_at FROM blocks_view bv WHERE bv.num = r.from_block)     AS t58
FROM t_range r;

DO $$
BEGIN
  IF NOT (SELECT is_fixture FROM t_fixture) THEN
    IF current_setting('btracker_test.require_clears', true) = '1' THEN
      RAISE EXCEPTION 'cleared_accounts_batch: the first clearing block is %, not the account_clears fixture''s 90000058',
        (SELECT from_block FROM t_range);
    END IF;
    RAISE NOTICE 'cleared_accounts_batch: not the account_clears fixture - its values are not checked';
  END IF;
END
$$;

-- ---------------------------------------------------------------- helpers for the checks
-- '{}' when _got = _want, else one line saying what differs
CREATE FUNCTION pg_temp.expect(_what TEXT, _got TEXT, _want TEXT)
RETURNS TEXT[] LANGUAGE sql IMMUTABLE AS $f$
  SELECT CASE WHEN _got IS NOT DISTINCT FROM _want THEN '{}'::TEXT[]
              ELSE ARRAY[format('%s: %s, want %s', _what, coalesce(_got, 'NULL'), coalesce(_want, 'NULL'))] END
$f$;

-- an account's liquid or savings balances of one asset from a block on, in order
CREATE FUNCTION pg_temp.hist(_savings BOOLEAN, _account INT, _nai INT, _from_block INT)
RETURNS TEXT LANGUAGE sql STABLE AS $f$
  SELECT coalesce(array_agg(h.balance ORDER BY h.balance_seq_no), '{}')::TEXT
  FROM (SELECT account, nai, balance, balance_seq_no, source_op FROM account_balance_history WHERE NOT _savings
        UNION ALL
        SELECT account, nai, balance, balance_seq_no, source_op FROM account_savings_history WHERE _savings) h
  WHERE h.account = _account AND h.nai = _nai AND hafd.operation_id_to_block_num(h.source_op) >= _from_block
$f$;

-- the blocks of an account's clear rows (liquid or savings) of one asset
CREATE FUNCTION pg_temp.clear_blocks(_savings BOOLEAN, _account INT, _nai INT)
RETURNS TEXT LANGUAGE sql STABLE AS $f$
  SELECT coalesce(array_agg(DISTINCT hafd.operation_id_to_block_num(h.source_op)), '{}')::TEXT
  FROM (SELECT account, nai, op_type_id, source_op FROM account_balance_history WHERE NOT _savings
        UNION ALL
        SELECT account, nai, op_type_id, source_op FROM account_savings_history WHERE _savings) h
  WHERE h.account = _account AND h.nai = _nai
    AND h.op_type_id IN (btracker_backend.op_clear_null_account_balance(),
                         btracker_backend.op_consolidate_treasury_balance())
$f$;

CREATE FUNCTION pg_temp.current(_savings BOOLEAN, _account INT, _nai INT)
RETURNS TEXT LANGUAGE sql STABLE AS $f$
  SELECT CASE WHEN _savings
              THEN (SELECT s.balance FROM account_savings s WHERE s.account = _account AND s.nai = _nai)
              ELSE (SELECT c.balance FROM current_account_balances c WHERE c.account = _account AND c.nai = _nai)
         END::TEXT
$f$;

-- An account's HBD-interest accumulator against the wanted fields, compared as values (a
-- NUMERIC's text depends on its scale); a NULL wanted seconds or payment is not compared.
CREATE FUNCTION pg_temp.hbd_mismatch(_what TEXT, _account INT, _seconds NUMERIC, _update TIMESTAMP,
                                     _balance BIGINT, _payment TIMESTAMP)
RETURNS TEXT[] LANGUAGE plpgsql STABLE AS $f$
DECLARE
  hi RECORD;
BEGIN
  SELECT * INTO hi FROM account_hbd_interest a WHERE a.account = _account;
  IF NOT FOUND
     OR (_seconds IS NOT NULL AND hi.hbd_seconds IS DISTINCT FROM _seconds)
     OR hi.hbd_seconds_last_update IS DISTINCT FROM _update
     OR hi.last_balance IS DISTINCT FROM _balance
     OR (_payment IS NOT NULL AND hi.hbd_last_interest_payment IS DISTINCT FROM _payment) THEN
    RETURN ARRAY[format('%s HBD interest (seconds, update, balance, payment): %s, want (%s, %s, %s, %s)',
                        _what, CASE WHEN FOUND THEN ROW(hi.hbd_seconds, hi.hbd_seconds_last_update,
                                                        hi.last_balance, hi.hbd_last_interest_payment)::TEXT
                                    ELSE 'no row' END,
                        coalesce(_seconds::TEXT, 'any'), _update, _balance, coalesce(_payment::TEXT, 'any'))];
  END IF;
  RETURN '{}';
END
$f$;

/*
 * What hived leaves on the fixture's accounts, checked after the sync (step 0) and after
 * block-by-block processing (step 2). Block by block, other accounts' histories hold the
 * mock blocks twice, so nothing here reads another account's history.
 */
CREATE FUNCTION pg_temp.fixture_mismatches()
RETURNS TEXT[] LANGUAGE plpgsql AS $f$
DECLARE
  f   RECORD;
  _o  TEXT[] := '{}';
  _vests_clears INT[];
BEGIN
  SELECT * INTO f FROM t_fixture;

  -- null, liquid. HIVE: +1000 and the clear in ...58, +500 and the clear in ...59; HBD:
  -- +250 and the clear in ...58. Repeated clears, each debiting what is held and nothing
  -- when nothing is (no row at ...61 and ...63 for HIVE, none after ...58 for HBD).
  _o := _o || pg_temp.expect('null HIVE from ...58 (after the first)',
                             ((pg_temp.hist(FALSE, f.null_id, 21, 90000058))::BIGINT[])[2:]::TEXT, '{0,500,0}');
  _o := _o || pg_temp.expect('null HIVE clear blocks', pg_temp.clear_blocks(FALSE, f.null_id, 21), '{90000058,90000059}');
  _o := _o || pg_temp.expect('null HBD from ...58 (after the first)',
                             ((pg_temp.hist(FALSE, f.null_id, 13, 90000058))::BIGINT[])[2:]::TEXT, '{0}');
  _o := _o || pg_temp.expect('null HBD clear blocks', pg_temp.clear_blocks(FALSE, f.null_id, 13), '{90000058}');
  -- VESTS: +4000 VESTS and the clear in ...61, +2000 in ...62, the clear in ...63 (a block
  -- with no liquid op of null: block by block it debits the stored balance), +2000 in ...64.
  -- A credit after a clear starts from 0 (mzander's 10, +5, clear, +2 ends at 2).
  _o := _o || pg_temp.expect('null VESTS from ...61', pg_temp.hist(FALSE, f.null_id, 37, 90000061),
                             '{4000000000,0,2000000000,0,2000000000}');
  _vests_clears := pg_temp.clear_blocks(FALSE, f.null_id, 37)::INT[];
  IF NOT (_vests_clears @> '{90000061,90000063}' AND _vests_clears <@ '{90000058,90000061,90000063}') THEN
    _o := _o || format('null VESTS clear blocks: %s, want ...61 and ...63 (and ...58 if the base chain left VESTS)', _vests_clears);
  END IF;
  _o := _o || pg_temp.expect('null current HIVE', pg_temp.current(FALSE, f.null_id, 21), '0');
  _o := _o || pg_temp.expect('null current HBD', pg_temp.current(FALSE, f.null_id, 13), '0');
  _o := _o || pg_temp.expect('null current VESTS', pg_temp.current(FALSE, f.null_id, 37), '2000000000');

  -- null, savings: +2 HIVE and +4 HBD in ...58, then the clear; nothing later
  _o := _o || pg_temp.expect('null HIVE savings from ...58 (after the first)',
                             ((pg_temp.hist(TRUE, f.null_id, 21, 90000058))::BIGINT[])[2:]::TEXT, '{0}');
  _o := _o || pg_temp.expect('null HBD savings from ...58 (after the first)',
                             ((pg_temp.hist(TRUE, f.null_id, 13, 90000058))::BIGINT[])[2:]::TEXT, '{0}');
  _o := _o || pg_temp.expect('null HIVE savings clear blocks', pg_temp.clear_blocks(TRUE, f.null_id, 21), '{90000058}');
  _o := _o || pg_temp.expect('null HBD savings clear blocks', pg_temp.clear_blocks(TRUE, f.null_id, 13), '{90000058}');
  _o := _o || pg_temp.expect('null current HIVE savings', pg_temp.current(TRUE, f.null_id, 21), '0');
  _o := _o || pg_temp.expect('null current HBD savings', pg_temp.current(TRUE, f.null_id, 13), '0');

  -- steem.dao (created in ...58): +2000 HIVE, +300 HBD, +3 HIVE savings, then the consolidate
  _o := _o || pg_temp.expect('steem.dao HIVE', pg_temp.hist(FALSE, f.dao_id, 21, 0), '{2000,0}');
  _o := _o || pg_temp.expect('steem.dao HBD', pg_temp.hist(FALSE, f.dao_id, 13, 0), '{300,0}');
  _o := _o || pg_temp.expect('steem.dao VESTS', pg_temp.hist(FALSE, f.dao_id, 37, 0), '{}');
  _o := _o || pg_temp.expect('steem.dao HIVE savings', pg_temp.hist(TRUE, f.dao_id, 21, 0), '{3,0}');
  _o := _o || pg_temp.expect('steem.dao HIVE clear blocks', pg_temp.clear_blocks(FALSE, f.dao_id, 21), '{90000058}');
  _o := _o || pg_temp.expect('steem.dao current HIVE', pg_temp.current(FALSE, f.dao_id, 21), '0');
  _o := _o || pg_temp.expect('steem.dao current HBD', pg_temp.current(FALSE, f.dao_id, 13), '0');
  _o := _o || pg_temp.expect('steem.dao current HIVE savings', pg_temp.current(TRUE, f.dao_id, 21), '0');

  -- pending rewards: only those after null's last clear (...63 op 1): benefactor 4 HIVE,
  -- 5 HBD, 3 VESTS; curation 1 VESTS; author 2 HIVE, 1 HBD, 1 VESTS. The four before it
  -- (...58, 60, 61, 63 op 0) are emptied.
  _o := _o || pg_temp.expect('null pending rewards (HBD, HIVE, VESTS)',
                             (SELECT array_agg(coalesce(r.balance, 0) ORDER BY n.nai)
                              FROM (VALUES (13), (21), (37)) n(nai)
                              LEFT JOIN account_rewards r ON r.account = f.null_id AND r.nai = n.nai)::TEXT,
                             '{6,6,5000000}');
  _o := _o || pg_temp.expect('steem.dao pending rewards',
                             (SELECT coalesce(sum(r.balance), 0) FROM account_rewards r WHERE r.account = f.dao_id)::TEXT, '0');

  -- delayed vests: reset at ...61 and ...63, then the delayed_voting debit of 1e9 in ...64
  -- clamps at 0 and its +2e9 power-up stays (1e9 unclamped, 3e9 without the ...63 reset)
  _o := _o || pg_temp.expect('null delayed vests',
                             (SELECT w.delayed_vests FROM account_withdraws w WHERE w.account = f.null_id)::TEXT, '2000000000');
  _o := _o || pg_temp.expect('steem.dao delayed vests',
                             (SELECT coalesce(sum(w.delayed_vests), 0) FROM account_withdraws w WHERE w.account = f.dao_id)::TEXT, '0');

  -- unaffected: hive.fund keeps what the consolidate moved (as HAF's collector emits it,
  -- liquid only), and neither it nor poloniex, which paid null and steem.dao, has a clear row
  _o := _o || pg_temp.expect('hive.fund current HIVE', pg_temp.current(FALSE, f.fund_id, 21), '2003');
  _o := _o || pg_temp.expect('hive.fund current HBD', pg_temp.current(FALSE, f.fund_id, 13), '300');
  _o := _o || pg_temp.expect('clear rows of hive.fund and poloniex',
                             (SELECT count(*) FROM (SELECT account, op_type_id FROM account_balance_history
                                                    UNION ALL SELECT account, op_type_id FROM account_savings_history) h
                              WHERE h.account IN (f.fund_id, f.polo_id)
                                AND h.op_type_id IN (btracker_backend.op_clear_null_account_balance(),
                                                     btracker_backend.op_consolidate_treasury_balance()))::TEXT, '0');
  RETURN _o;
END
$f$;

-- rows of a day/month rollup of the given accounts from a period on, against the history
-- they are built from (end-of-period balance and source_op, min and max of the period)
CREATE FUNCTION pg_temp.rollup_mismatches(_history TEXT, _rollup TEXT, _period TEXT, _accounts INT[], _since TIMESTAMP)
RETURNS TEXT[] LANGUAGE plpgsql AS $f$
DECLARE
  _n BIGINT;
  _sample TEXT;
BEGIN
  EXECUTE format($q$
    WITH want AS (
      SELECT DISTINCT ON (x.account, x.nai, x.p)
        x.account, x.nai, x.p AS updated_at, x.balance,
        MIN(x.balance) OVER w AS min_balance, MAX(x.balance) OVER w AS max_balance, x.source_op
      FROM (SELECT h.account, h.nai, h.balance, h.source_op, date_trunc(%3$L, bv.created_at) AS p
            FROM %1$I h JOIN blocks_view bv ON bv.num = hafd.operation_id_to_block_num(h.source_op)
            WHERE h.account = ANY ($1)) x
      WHERE x.p >= date_trunc(%3$L, $2)
      WINDOW w AS (PARTITION BY x.account, x.nai, x.p)
      ORDER BY x.account, x.nai, x.p, x.source_op DESC
    ),
    have AS (
      SELECT r.account, r.nai, r.updated_at, r.balance, r.min_balance, r.max_balance, r.source_op
      FROM %2$I r WHERE r.account = ANY ($1) AND r.updated_at >= date_trunc(%3$L, $2)
    ),
    diff AS (
      SELECT 'have ' || h::TEXT AS r FROM (SELECT * FROM have EXCEPT ALL SELECT * FROM want) h
      UNION ALL
      SELECT 'want ' || w::TEXT FROM (SELECT * FROM want EXCEPT ALL SELECT * FROM have) w
    )
    SELECT count(*), string_agg(r, ' | ') FILTER (WHERE r IS NOT NULL) FROM (SELECT r FROM diff ORDER BY r LIMIT 4) s
    $q$, _history, _rollup, _period)
    INTO _n, _sample USING _accounts, _since;
  IF _n > 0 THEN
    RETURN ARRAY[format('%s by %s differs from %s: %s', _rollup, _period, _history, _sample)];
  END IF;
  RETURN '{}';
END
$f$;

-- ---------------------------------------------------------------- 0. the synced state
DO $$
DECLARE
  _schema TEXT := current_setting('btracker_test.schema');
  _stage  TEXT := hive.get_current_stage_name(current_setting('btracker_test.schema'));
  f  RECORD;
  _m TEXT[] := '{}';
BEGIN
  -- the sync went the whole normal way: massive sync, finalize_massive_sync, LIVE
  IF NOT isIndexesCreated() THEN
    RAISE EXCEPTION 'cleared_accounts_batch: % is not finalized (isIndexesCreated() is false)', _schema;
  END IF;
  IF _stage IS DISTINCT FROM 'live' THEN
    RAISE EXCEPTION 'cleared_accounts_batch: % is in stage %, not live', _schema, _stage;
  END IF;

  -- balance_totals as the aggregates over current_account_balances (as
  -- tests/maintenance/balance_totals.sql checks them), with the clears applied
  _m := _m || (
    WITH agg AS (
      SELECT cab.nai, COUNT(*) FILTER (WHERE cab.balance > 0) AS holders, SUM(cab.balance::NUMERIC) AS total
      FROM current_account_balances cab GROUP BY cab.nai
    )
    SELECT coalesce(array_agg(format('balance_totals %s: stored %s/%s, aggregated %s/%s',
                                     coalesce(bt.nai, agg.nai), bt.holders, bt.total, agg.holders, agg.total)), '{}')
    FROM balance_totals bt FULL JOIN agg ON agg.nai = bt.nai
    WHERE bt.holders IS DISTINCT FROM coalesce(agg.holders, 0) OR bt.total IS DISTINCT FROM coalesce(agg.total, 0));

  SELECT * INTO f FROM t_fixture;
  IF f.is_fixture THEN
    _m := _m || pg_temp.fixture_mismatches();

    -- The HBD-interest accumulator (HF25 is not applied on the mock chain, so it runs).
    -- steem.dao, new: +300 HBD by a transaction in ...58 (time of ...57), the consolidate
    -- in ...58 (its own time). The first row accrues nothing; the clear accrues 300 x 3 s,
    -- and with the epoch as the last payment that is a compound-interval reset at the
    -- clear: hbd_seconds 0, payment and update at ...58, balance 0. Without the clear:
    -- 0, ...57, 300, epoch.
    _m := _m || pg_temp.hbd_mismatch('steem.dao', f.dao_id, 0, f.t58, 0, f.t58);
    -- null: its last HBD change is the clear in ...58 (the later clears find 0 HBD); its
    -- seconds and payment depend on the base chain
    _m := _m || pg_temp.hbd_mismatch('null', f.null_id, NULL, f.t58, 0, NULL);

    -- rollups of the accounts the fixture moves, built incrementally or by finalize
    _m := _m || pg_temp.rollup_mismatches('account_balance_history', 'balance_history_by_day', 'day',
                                          ARRAY[f.null_id, f.dao_id, f.fund_id], f.t58)
             || pg_temp.rollup_mismatches('account_balance_history', 'balance_history_by_month', 'month',
                                          ARRAY[f.null_id, f.dao_id, f.fund_id], f.t58)
             || pg_temp.rollup_mismatches('account_savings_history', 'saving_history_by_day', 'day',
                                          ARRAY[f.null_id, f.dao_id, f.fund_id], f.t58)
             || pg_temp.rollup_mismatches('account_savings_history', 'saving_history_by_month', 'month',
                                          ARRAY[f.null_id, f.dao_id, f.fund_id], f.t58);
    -- non-vacuity, by value: both fixture days hold a clear of null's VESTS, so each day
    -- dips to 0 and ends at the credit after its last clear (...16: +4000, clear, +2000;
    -- ...17: clear, +2000)
    _m := _m || pg_temp.expect('null VESTS day rollups from ...58 (balance, min, max)',
                               (SELECT array_agg(ARRAY[d.balance, d.min_balance, d.max_balance] ORDER BY d.updated_at)
                                FROM balance_history_by_day d
                                WHERE d.account = f.null_id AND d.nai = 37 AND d.updated_at >= date_trunc('day', f.t58))::TEXT,
                               '{{2000000000,0,4000000000},{2000000000,0,2000000000}}');
  END IF;

  IF cardinality(_m) > 0 THEN
    RAISE EXCEPTION 'cleared_accounts_batch: the synced state of % (stage %) is wrong: %', _schema, _stage, _m;
  END IF;
  RAISE NOTICE 'cleared_accounts_batch: the synced state of % (stage %, finalized) holds the clears%', _schema, _stage,
    CASE WHEN f.is_fixture THEN ' and the account_clears fixture values' ELSE '' END;
END
$$;

-- null's delayed vests as the sync left them; every start below uses this + 4e9. Only
-- null's existing row: a row created for steem.dao, which has none, would be compared
-- against itself and prove nothing.
CREATE TEMP TABLE t_seed ON COMMIT DROP AS
SELECT aw.account, aw.delayed_vests + 4000000000 AS delayed_vests
FROM account_withdraws aw JOIN accounts_view av ON av.id = aw.account AND av.name = 'null';
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM t_seed) THEN
    RAISE EXCEPTION 'cleared_accounts_batch: null has no account_withdraws row to start from';
  END IF;
END
$$;

-- ---------------------------------------------------------------- the starting state
CREATE FUNCTION pg_temp.take_back()
RETURNS VOID LANGUAGE plpgsql AS $f$
DECLARE
  _from INT := (SELECT from_block FROM t_range);
  f RECORD;
BEGIN
  SELECT * INTO f FROM t_fixture;
  DELETE FROM account_balance_history h
  WHERE h.account IN (SELECT account FROM t_accounts) AND hafd.operation_id_to_block_num(h.source_op) >= _from;
  DELETE FROM account_savings_history h
  WHERE h.account IN (SELECT account FROM t_accounts) AND hafd.operation_id_to_block_num(h.source_op) >= _from;

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
    AND d.updated_at >= (SELECT date_trunc('day', bv.created_at) FROM blocks_view bv WHERE bv.num = _from);

  UPDATE account_withdraws aw SET delayed_vests = s.delayed_vests FROM t_seed s WHERE aw.account = s.account;

  -- Stored values a clear must work from or leave alone (null's):
  --   * the HBD-interest accumulator, last updated 1000 s before ...57 at the balance it
  --     holds now, paid a day earlier (inside the compound interval: no reset), and
  --     steem.dao's taken back to none;
  --   * a pending HIVE savings withdrawal (hived's clear does not cancel it), received
  --     vests and lifetime rewards.
  INSERT INTO account_hbd_interest AS hi (account, hbd_seconds, hbd_seconds_last_update, last_balance, hbd_last_interest_payment)
  SELECT f.null_id, 1000000, f.t57 - INTERVAL '1000 seconds',
         coalesce((SELECT c.balance FROM current_account_balances c WHERE c.account = f.null_id AND c.nai = 13), 0),
         f.t57 - INTERVAL '1 day'
  ON CONFLICT ON CONSTRAINT pk_account_hbd_interest DO UPDATE SET
    hbd_seconds = EXCLUDED.hbd_seconds, hbd_seconds_last_update = EXCLUDED.hbd_seconds_last_update,
    last_balance = EXCLUDED.last_balance, hbd_last_interest_payment = EXCLUDED.hbd_last_interest_payment;
  DELETE FROM account_hbd_interest hi WHERE hi.account = f.dao_id;

  INSERT INTO account_savings AS s (account, nai, balance, balance_change_count, source_op, savings_withdraw_requests)
  VALUES (f.null_id, 21, 0, 0, 0, 1)
  ON CONFLICT ON CONSTRAINT pk_account_savings DO UPDATE SET savings_withdraw_requests = 1;
  INSERT INTO account_delegations AS d (account, received_vests, delegated_vests) VALUES (f.null_id, 7, 0)
  ON CONFLICT ON CONSTRAINT pk_temp_vests DO UPDATE SET received_vests = 7;
  INSERT INTO account_info_rewards AS r (account, posting_rewards, curation_rewards) VALUES (f.null_id, 100, 200)
  ON CONFLICT ON CONSTRAINT pk_account_info_rewards DO UPDATE SET posting_rewards = 100, curation_rewards = 200;
END
$f$;

-- null's HBD balance the seed starts from (the same at every take-back)
CREATE TEMP TABLE t_hbd_start ON COMMIT DROP (balance BIGINT);

CREATE FUNCTION pg_temp.process(_from INT, _to INT)
RETURNS VOID LANGUAGE plpgsql AS $f$
BEGIN
  PERFORM btracker_prefetch_operations(_from, _to);
  PERFORM process_block_range_balances(_from, _to);
  PERFORM process_block_range_withdrawals(_from, _to);
  PERFORM process_block_range_savings(_from, _to);
  PERFORM process_block_range_rewards(_from, _to);
END
$f$;

-- ---------------------------------------------------------------- 1-2. block by block, snapshot
DO $$
DECLARE
  _from INT := (SELECT from_block FROM t_range);
  _to   INT := (SELECT to_block FROM t_range);
BEGIN
  IF _to - _from < 2 THEN
    RAISE EXCEPTION 'cleared_accounts_batch: range %..% is not a multi-block batch', _from, _to;
  END IF;
  PERFORM pg_temp.take_back();
  INSERT INTO t_hbd_start
  SELECT hi.last_balance FROM account_hbd_interest hi WHERE hi.account = (SELECT null_id FROM t_fixture);
  FOR _b IN _from.._to LOOP
    PERFORM pg_temp.process(_b, _b);
  END LOOP;
END
$$;

-- on the fixture: its values again, now from the LIVE path, and the stored state
DO $$
DECLARE
  f  RECORD;
  _b BIGINT := (SELECT balance FROM t_hbd_start);
  _m TEXT[];
BEGIN
  SELECT * INTO f FROM t_fixture;
  IF NOT f.is_fixture THEN
    RETURN;
  END IF;
  _m := pg_temp.fixture_mismatches();

  -- null's accumulator from the seed: +250 HBD by a transaction in ...58 (time of ...57)
  -- accrues the seeded balance over 1000 s; the clear in ...58 (its own time) accrues the
  -- balance plus 250 over 3 s and leaves 0. The later clears find no HBD and add nothing.
  -- The payment stays the seeded one (inside the compound interval).
  _m := _m || pg_temp.hbd_mismatch('null (from the seed)', f.null_id,
                                   1000000 + _b * 1000::NUMERIC + (_b + 250) * EXTRACT(EPOCH FROM f.t58 - f.t57),
                                   f.t58, 0, f.t57 - INTERVAL '1 day');
  _m := _m || pg_temp.hbd_mismatch('steem.dao', f.dao_id, 0, f.t58, 0, f.t58);

  -- what the clears must leave alone
  _m := _m || pg_temp.expect('null pending HIVE savings withdrawals',
                             (SELECT s.savings_withdraw_requests FROM account_savings s WHERE s.account = f.null_id AND s.nai = 21)::TEXT, '1');
  _m := _m || pg_temp.expect('null received vests',
                             (SELECT d.received_vests FROM account_delegations d WHERE d.account = f.null_id)::TEXT, '7');
  IF NOT EXISTS (SELECT 1 FROM account_info_rewards r
                 WHERE r.account = f.null_id AND r.posting_rewards >= 100 AND r.curation_rewards >= 200) THEN
    _m := _m || format('null lifetime rewards: %s, want at least the seeded 100/200',
                       (SELECT ROW(r.posting_rewards, r.curation_rewards)::TEXT FROM account_info_rewards r WHERE r.account = f.null_id));
  END IF;

  IF cardinality(_m) > 0 THEN
    RAISE EXCEPTION 'cleared_accounts_batch: block-by-block processing is wrong: %', _m;
  END IF;
  RAISE NOTICE 'cleared_accounts_batch: block-by-block processing holds the account_clears fixture values';
END
$$;

CREATE TEMP TABLE s_hist ON COMMIT DROP AS
SELECT h.account, h.nai, h.op_type_id, h.balance_seq_no, h.source_op, h.balance FROM account_balance_history h
WHERE h.account IN (SELECT account FROM t_accounts) AND hafd.operation_id_to_block_num(h.source_op) >= (SELECT from_block FROM t_range);
CREATE TEMP TABLE s_sav ON COMMIT DROP AS
SELECT h.account, h.nai, h.op_type_id, h.balance_seq_no, h.source_op, h.balance FROM account_savings_history h
WHERE h.account IN (SELECT account FROM t_accounts) AND hafd.operation_id_to_block_num(h.source_op) >= (SELECT from_block FROM t_range);
CREATE TEMP TABLE s_cab ON COMMIT DROP AS SELECT c.account, c.nai, c.balance, c.balance_change_count, c.source_op FROM current_account_balances c WHERE c.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_acs ON COMMIT DROP AS SELECT c.account, c.nai, c.balance, c.balance_change_count, c.source_op, c.savings_withdraw_requests FROM account_savings c WHERE c.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_rew ON COMMIT DROP AS SELECT r.account, r.nai, r.balance FROM account_rewards r WHERE r.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_info ON COMMIT DROP AS SELECT r.account, r.posting_rewards, r.curation_rewards FROM account_info_rewards r WHERE r.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_wd ON COMMIT DROP AS SELECT w.account, w.delayed_vests FROM account_withdraws w WHERE w.account IN (SELECT account FROM t_accounts);
CREATE TEMP TABLE s_hbd ON COMMIT DROP AS SELECT hi.account, hi.hbd_seconds, hi.hbd_seconds_last_update, hi.last_balance, hi.hbd_last_interest_payment FROM account_hbd_interest hi WHERE hi.account IN (SELECT account FROM t_accounts);
-- rollups of the days of the range (earlier days are untouched by steps 2 and 3)
CREATE TEMP TABLE s_bday ON COMMIT DROP AS
SELECT d.account, d.nai, d.updated_at, d.balance, d.min_balance, d.max_balance, d.source_op FROM balance_history_by_day d
WHERE d.account IN (SELECT account FROM t_accounts)
  AND d.updated_at >= (SELECT date_trunc('day', bv.created_at) FROM blocks_view bv WHERE bv.num = (SELECT from_block FROM t_range));

DO $$
DECLARE
  _null INT := (SELECT av.id FROM accounts_view av WHERE av.name = 'null');
BEGIN
  -- non-vacuity: the credit after null's last clear (block ...64) must leave something, and
  -- the snapshot must hold clear rows
  IF NOT EXISTS (SELECT 1 FROM s_wd s WHERE s.account = _null AND s.delayed_vests > 0) THEN
    RAISE EXCEPTION 'cleared_accounts_batch: null has no delayed vests after the mock blocks; the credit after its last clear is not exercised';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM s_hist WHERE op_type_id IN (btracker_backend.op_clear_null_account_balance(),
                                                           btracker_backend.op_consolidate_treasury_balance())) THEN
    RAISE EXCEPTION 'cleared_accounts_batch: block-by-block processing wrote no clear row';
  END IF;
END
$$;

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
    ('account_info_rewards', 'SELECT r.account, r.posting_rewards, r.curation_rewards FROM account_info_rewards r WHERE r.account IN (SELECT account FROM t_accounts)', 's_info'),
    ('account_withdraws.delayed_vests', 'SELECT w.account, w.delayed_vests FROM account_withdraws w WHERE w.account IN (SELECT account FROM t_accounts)', 's_wd'),
    ('account_hbd_interest', 'SELECT hi.account, hi.hbd_seconds, hi.hbd_seconds_last_update, hi.last_balance, hi.hbd_last_interest_payment FROM account_hbd_interest hi WHERE hi.account IN (SELECT account FROM t_accounts)', 's_hbd'),
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

-- ---------------------------------------------------------------- 3. one range, then compare
DO $$
DECLARE
  _from INT := (SELECT from_block FROM t_range);
  _to   INT := (SELECT to_block FROM t_range);
  _m    TEXT[];
BEGIN
  PERFORM pg_temp.take_back();
  PERFORM pg_temp.process(_from, _to);
  _m := pg_temp.batch_mismatches();
  IF cardinality(_m) > 0 THEN
    RAISE EXCEPTION 'cleared_accounts_batch: blocks %..% as one range differ from block-by-block processing: %', _from, _to, _m;
  END IF;
  RAISE NOTICE 'cleared_accounts_batch: blocks %..% as one range equal block-by-block processing for null/steem.dao', _from, _to;
END
$$;

ROLLBACK;

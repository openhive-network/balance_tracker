/*
Hardfork-specific processing functions.
Called by: Multiple process files (process_balances.sql, process_delegations.sql, etc.)

Handles HF23 (Hive fork from Steem) balance resets:
- Steemit Inc accounts had all balances zeroed
- Affected: main balances, rewards, savings, delegations, withdrawals

This function processes hardfork_hive_operation at the specific HF23 block,
resetting all financial state for impacted accounts to zero.

Note: This function may become unnecessary after changes to hardfork_hive_operation
in HAF that provide more structured data.
*/

SET ROLE btracker_owner;

-- Process HF23 balance resets for Steemit Inc accounts.
-- At the Hive fork, affected accounts had all financial state zeroed:
-- - Main balances (HIVE, HBD, VESTS)
-- - Pending rewards
-- - Savings balances and pending withdrawals
-- Delegations and withdrawals are handled by their respective process files.
CREATE OR REPLACE FUNCTION btracker_backend.process_hf_23(__hardfork_23_block INT)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  _op_hardfork_hive INT := btracker_backend.op_hardfork_hive();
  __balances INT;
  __rewards INT;
  __savings INT;
BEGIN
  -- Find all accounts affected by HF23 at the specific block
  WITH hardfork_hive_operation AS MATERIALIZED (
    SELECT
      (SELECT av.id FROM accounts_view av WHERE av.name = ov.body_value->>'account') AS account_id
    FROM operations_view ov
    WHERE ov.op_type_id = _op_hardfork_hive AND block_num = __hardfork_23_block
  ),
  -- Zero out main account balances
  update_balances AS (
    UPDATE current_account_balances cab SET
      balance = 0
    FROM hardfork_hive_operation hho
    WHERE cab.account = hho.account_id
    RETURNING cab.account AS cleaned_account_id
  ),
  -- Zero out pending rewards
  update_rewards AS (
    UPDATE account_rewards ar SET
      balance = 0
    FROM hardfork_hive_operation hho
    WHERE ar.account = hho.account_id
    RETURNING ar.account AS cleaned_account_id
  ),
  -- Zero out savings and clear pending withdrawal requests
  update_savings AS (
    UPDATE account_savings acs SET
      balance = 0,
      savings_withdraw_requests = 0
    FROM hardfork_hive_operation hho
    WHERE acs.account = hho.account_id
    RETURNING acs.account AS cleaned_account_id
  )
  SELECT
    (SELECT count(*) FROM update_balances) AS balances,
    (SELECT count(*) FROM update_rewards) AS rewards,
    (SELECT count(*) FROM update_savings) AS savings
  INTO __balances, __rewards, __savings;

END
$$;

-- Balance-clearing virtual operations in a batch (#61).
--
-- hived empties two accounts in consensus, every block in which they hold anything:
--   * clear_null_account_balance_operation   -> 'null': liquid, savings, vesting and reward
--     balances are burned (database::clear_null_account_balance);
--   * consolidate_treasury_balance_operation -> 'steem.dao' (the pre-HF24 treasury): the
--     same balances move to hive.fund (database::consolidate_treasury_balance).
-- Neither op names the account it empties (it is a protocol constant), and HAF's balance
-- collector deliberately emits no debit for it, leaving the clear to consumers - the same
-- contract process_hf_23 honours for hardfork_hive_operation. get_impacted_balances stays
-- a faithful port of that collector (tests/parity); the pipelines apply these clears
-- after it: process_balances and process_savings reset the running balance at the op,
-- process_rewards zeroes the account before applying later rewards.
--
-- Reads the batch staged by btracker_prefetch_operations. Returns one row per op.
CREATE OR REPLACE FUNCTION btracker_backend.account_clears_in_range(_from INT, _to INT)
RETURNS TABLE (
  account_id INT,
  source_op BIGINT,
  op_type_id SMALLINT,
  source_op_block INT
)
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  _op_clear_null  INT := btracker_backend.op_clear_null_account_balance();
  _op_consolidate INT := btracker_backend.op_consolidate_treasury_balance();
BEGIN
  -- Never returns a NULL account: callers use NOT IN over account_id, which a NULL would
  -- turn into "no rows at all". (Both accounts exist on any chain that emits these ops.)
  RETURN QUERY
  SELECT av.id::INT, ob.id::BIGINT, ob.op_type_id::SMALLINT, ob.block_num::INT
  FROM _btracker_ops_batch ob
  JOIN accounts_view av
    ON av.name = CASE WHEN ob.op_type_id = _op_clear_null THEN 'null' ELSE 'steem.dao' END
  WHERE ob.op_type_id IN (_op_clear_null, _op_consolidate)
    AND ob.block_num BETWEEN _from AND _to;
END
$$;

-- Rebuild the liquid and savings history of 'null' and 'steem.dao' with their clears (#61).
--
-- For an install that processed blocks before the clears were applied. Every stored
-- history row is turned back into its delta (balance minus the previous row's balance;
-- rows a clear wrote earlier are dropped and re-derived), a clear marker is added at every
-- balance-clearing op processed so far, and the series is recomputed with the same reset
-- rule as process_balances / process_savings. Then the current balances, balance_totals
-- and - when they are maintained (LIVE) - the day/month rollups of the two accounts are
-- rewritten from it, and their pending rewards are zeroed. Idempotent: run on a rebuilt
-- series it reproduces it. Not rebuilt: account_hbd_interest (interest seconds accrued on
-- the phantom HBD) - nothing reads it back for these two accounts' balances.
CREATE OR REPLACE FUNCTION btracker_backend.rebuild_cleared_accounts()
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  _op_clear_null  INT := btracker_backend.op_clear_null_account_balance();
  _op_consolidate INT := btracker_backend.op_consolidate_treasury_balance();
  _null           INT := (SELECT av.id FROM accounts_view av WHERE av.name = 'null');
  _dao            INT := (SELECT av.id FROM accounts_view av WHERE av.name = 'steem.dao');
  _schema         TEXT;
  _head           INT;
  _rollups        BOOLEAN := isIndexesCreated();
  _rows           BIGINT;
BEGIN
  SHOW SEARCH_PATH INTO _schema;
  SELECT c.current_block_num INTO _head FROM hafd.contexts c WHERE c.name = _schema;

  DROP TABLE IF EXISTS _rb_clears;
  CREATE TEMP TABLE _rb_clears AS
  SELECT
    CASE WHEN ov.op_type_id = _op_clear_null THEN _null ELSE _dao END AS account,
    ov.id AS source_op,
    ov.op_type_id::SMALLINT AS op_type_id
  FROM operations_view ov
  WHERE ov.op_type_id IN (_op_clear_null, _op_consolidate)
    AND ov.block_num <= COALESCE(_head, 0);
  DELETE FROM _rb_clears WHERE account IS NULL;

  -- ---------------------------------------------------------------- liquid balances
  DROP TABLE IF EXISTS _rb_balances;
  CREATE TEMP TABLE _rb_balances AS
  WITH hist AS (
    SELECT
      h.account, h.nai, h.op_type_id, h.source_op, h.balance_seq_no AS ord,
      h.balance - LAG(h.balance, 1, 0::BIGINT) OVER (PARTITION BY h.account, h.nai ORDER BY h.balance_seq_no) AS delta
    FROM account_balance_history h
    WHERE h.account IN (_null, _dao)
  ),
  u AS (
    SELECT account, nai, op_type_id, source_op, ord, delta, FALSE AS is_clear
    FROM hist WHERE op_type_id NOT IN (_op_clear_null, _op_consolidate)
    UNION ALL
    SELECT c.account, n.nai, c.op_type_id, c.source_op, 0, 0::BIGINT, TRUE
    FROM _rb_clears c CROSS JOIN (VALUES (13::SMALLINT), (21::SMALLINT), (37::SMALLINT)) AS n(nai)
  ),
  g AS (
    SELECT u.*,
      SUM(u.is_clear::INT) OVER (PARTITION BY u.account, u.nai ORDER BY u.source_op, u.ord
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - u.is_clear::INT AS grp
    FROM u
  ),
  d AS (
    SELECT account, nai, op_type_id, source_op, ord, is_clear,
      CASE WHEN is_clear THEN -SUM(delta) OVER (PARTITION BY account, nai, grp) ELSE delta END AS delta
    FROM g
  )
  SELECT
    account, nai, op_type_id, source_op,
    SUM(delta) OVER w AS balance,
    ROW_NUMBER() OVER w AS balance_seq_no
  FROM d
  WHERE NOT is_clear OR delta <> 0
  WINDOW w AS (PARTITION BY account, nai ORDER BY source_op, ord ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

  -- balance_totals: replace the two accounts' old contribution by the new one
  WITH old_rows AS (
    SELECT cab.nai, cab.balance FROM current_account_balances cab WHERE cab.account IN (_null, _dao)
  ),
  new_rows AS (
    SELECT DISTINCT ON (r.account, r.nai) r.nai, r.balance
    FROM _rb_balances r ORDER BY r.account, r.nai, r.balance_seq_no DESC
  ),
  diff AS (
    SELECT x.nai, SUM(x.holders) AS holders, SUM(x.total) AS total
    FROM (
      SELECT nai, (balance > 0)::INT AS holders, balance::NUMERIC AS total FROM new_rows
      UNION ALL
      SELECT nai, -(balance > 0)::INT, -balance::NUMERIC FROM old_rows
    ) x
    GROUP BY x.nai
  )
  UPDATE balance_totals bt
  SET holders = bt.holders + diff.holders, total = bt.total + diff.total
  FROM diff WHERE bt.nai = diff.nai;

  DELETE FROM account_balance_history h WHERE h.account IN (_null, _dao);
  INSERT INTO account_balance_history (account, nai, op_type_id, balance_seq_no, source_op, balance)
  SELECT r.account, r.nai, r.op_type_id, r.balance_seq_no, r.source_op, r.balance
  FROM _rb_balances r ORDER BY r.account, r.nai, r.balance_seq_no;
  GET DIAGNOSTICS _rows = ROW_COUNT;

  INSERT INTO current_account_balances AS cab (account, nai, balance_change_count, source_op, balance)
  SELECT DISTINCT ON (r.account, r.nai) r.account, r.nai, r.balance_seq_no, r.source_op, r.balance
  FROM _rb_balances r ORDER BY r.account, r.nai, r.balance_seq_no DESC
  ON CONFLICT ON CONSTRAINT pk_current_account_balances DO UPDATE SET
    balance = EXCLUDED.balance,
    balance_change_count = EXCLUDED.balance_change_count,
    source_op = EXCLUDED.source_op;

  IF _rollups THEN
    DELETE FROM balance_history_by_day d WHERE d.account IN (_null, _dao);
    DELETE FROM balance_history_by_month m WHERE m.account IN (_null, _dao);
    INSERT INTO balance_history_by_day (account, nai, source_op, updated_at, balance, min_balance, max_balance)
    SELECT h.account, h.nai,
      (array_agg(h.source_op ORDER BY h.balance_seq_no DESC))[1],
      date_trunc('day', bv.created_at),
      (array_agg(h.balance ORDER BY h.balance_seq_no DESC))[1],
      MIN(h.balance), MAX(h.balance)
    FROM _rb_balances h
    JOIN blocks_view bv ON bv.num = hafd.operation_id_to_block_num(h.source_op)
    GROUP BY h.account, h.nai, date_trunc('day', bv.created_at);
    INSERT INTO balance_history_by_month (account, nai, source_op, updated_at, balance, min_balance, max_balance)
    SELECT h.account, h.nai,
      (array_agg(h.source_op ORDER BY h.balance_seq_no DESC))[1],
      date_trunc('month', bv.created_at),
      (array_agg(h.balance ORDER BY h.balance_seq_no DESC))[1],
      MIN(h.balance), MAX(h.balance)
    FROM _rb_balances h
    JOIN blocks_view bv ON bv.num = hafd.operation_id_to_block_num(h.source_op)
    GROUP BY h.account, h.nai, date_trunc('month', bv.created_at);
  END IF;

  -- ---------------------------------------------------------------- savings
  DROP TABLE IF EXISTS _rb_savings;
  CREATE TEMP TABLE _rb_savings AS
  WITH hist AS (
    SELECT
      h.account, h.nai, h.op_type_id, h.source_op, h.balance_seq_no AS ord,
      h.balance - LAG(h.balance, 1, 0::BIGINT) OVER (PARTITION BY h.account, h.nai ORDER BY h.balance_seq_no) AS delta
    FROM account_savings_history h
    WHERE h.account IN (_null, _dao)
  ),
  u AS (
    SELECT account, nai, op_type_id, source_op, ord, delta, FALSE AS is_clear
    FROM hist WHERE op_type_id NOT IN (_op_clear_null, _op_consolidate)
    UNION ALL
    SELECT c.account, n.nai, c.op_type_id, c.source_op, 0, 0::BIGINT, TRUE
    FROM _rb_clears c CROSS JOIN (VALUES (13::SMALLINT), (21::SMALLINT)) AS n(nai)
  ),
  g AS (
    SELECT u.*,
      SUM(u.is_clear::INT) OVER (PARTITION BY u.account, u.nai ORDER BY u.source_op, u.ord
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - u.is_clear::INT AS grp
    FROM u
  ),
  d AS (
    SELECT account, nai, op_type_id, source_op, ord, is_clear,
      CASE WHEN is_clear THEN -SUM(delta) OVER (PARTITION BY account, nai, grp) ELSE delta END AS delta
    FROM g
  )
  SELECT
    account, nai, op_type_id, source_op,
    SUM(delta) OVER w AS balance,
    ROW_NUMBER() OVER w AS balance_seq_no
  FROM d
  WHERE NOT is_clear OR delta <> 0
  WINDOW w AS (PARTITION BY account, nai ORDER BY source_op, ord ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

  DELETE FROM account_savings_history h WHERE h.account IN (_null, _dao);
  INSERT INTO account_savings_history (account, nai, op_type_id, balance_seq_no, source_op, balance)
  SELECT r.account, r.nai, r.op_type_id, r.balance_seq_no, r.source_op, r.balance
  FROM _rb_savings r ORDER BY r.account, r.nai, r.balance_seq_no;

  -- savings_withdraw_requests is kept: the clears do not cancel pending withdrawals
  INSERT INTO account_savings AS acs (account, nai, balance_change_count, source_op, balance)
  SELECT DISTINCT ON (r.account, r.nai) r.account, r.nai, r.balance_seq_no, r.source_op, r.balance
  FROM _rb_savings r ORDER BY r.account, r.nai, r.balance_seq_no DESC
  ON CONFLICT ON CONSTRAINT pk_account_savings DO UPDATE SET
    balance = EXCLUDED.balance,
    balance_change_count = EXCLUDED.balance_change_count,
    source_op = EXCLUDED.source_op;

  IF _rollups THEN
    DELETE FROM saving_history_by_day d WHERE d.account IN (_null, _dao);
    DELETE FROM saving_history_by_month m WHERE m.account IN (_null, _dao);
    INSERT INTO saving_history_by_day (account, nai, source_op, updated_at, balance, min_balance, max_balance)
    SELECT h.account, h.nai,
      (array_agg(h.source_op ORDER BY h.balance_seq_no DESC))[1],
      date_trunc('day', bv.created_at),
      (array_agg(h.balance ORDER BY h.balance_seq_no DESC))[1],
      MIN(h.balance), MAX(h.balance)
    FROM _rb_savings h
    JOIN blocks_view bv ON bv.num = hafd.operation_id_to_block_num(h.source_op)
    GROUP BY h.account, h.nai, date_trunc('day', bv.created_at);
    INSERT INTO saving_history_by_month (account, nai, source_op, updated_at, balance, min_balance, max_balance)
    SELECT h.account, h.nai,
      (array_agg(h.source_op ORDER BY h.balance_seq_no DESC))[1],
      date_trunc('month', bv.created_at),
      (array_agg(h.balance ORDER BY h.balance_seq_no DESC))[1],
      MIN(h.balance), MAX(h.balance)
    FROM _rb_savings h
    JOIN blocks_view bv ON bv.num = hafd.operation_id_to_block_num(h.source_op)
    GROUP BY h.account, h.nai, date_trunc('month', bv.created_at);
  END IF;

  -- ---------------------------------------------------------------- pending rewards
  -- No history to rebuild: hived holds both at zero after every clear.
  UPDATE account_rewards ar SET balance = 0 WHERE ar.account IN (_null, _dao) AND ar.balance <> 0;

  RAISE NOTICE 'btracker: rebuilt null/steem.dao balances with % clearing ops (% liquid history rows)',
    (SELECT count(*) FROM _rb_clears), _rows;

  DROP TABLE _rb_clears;
  DROP TABLE _rb_balances;
  DROP TABLE _rb_savings;
END
$$;

-- Apply it once to an install that processed clearing ops before this code: the history
-- of 'null' / 'steem.dao' then holds no row written by a clearing op although such ops
-- were processed. Only once the history indexes exist (LIVE): without them both probes
-- would scan the whole history. Called at install (below) and at the end of
-- finalize_massive_sync, which covers an install upgraded in the middle of massive sync.
CREATE OR REPLACE FUNCTION btracker_backend.rebuild_cleared_accounts_if_needed()
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  _schema TEXT;
  _head INT;
  _ids INT[];
  _op_clear_null  INT := btracker_backend.op_clear_null_account_balance();
  _op_consolidate INT := btracker_backend.op_consolidate_treasury_balance();
BEGIN
  IF NOT isIndexesCreated() THEN
    RETURN;
  END IF;
  SHOW SEARCH_PATH INTO _schema;
  SELECT c.current_block_num INTO _head FROM hafd.contexts c WHERE c.name = _schema;
  SELECT array_agg(av.id) INTO _ids FROM accounts_view av WHERE av.name IN ('null', 'steem.dao');
  IF COALESCE(_head, 0) > 0
     AND EXISTS (
       SELECT 1 FROM operations_view ov
       WHERE ov.op_type_id IN (_op_clear_null, _op_consolidate) AND ov.block_num <= _head)
     AND NOT EXISTS (
       SELECT 1 FROM account_balance_history h
       WHERE h.account = ANY(_ids) AND h.op_type_id IN (_op_clear_null, _op_consolidate)) THEN
    PERFORM btracker_backend.rebuild_cleared_accounts();
  END IF;
END
$$;

DO $$
BEGIN
  -- A fresh install has no tables processed yet: nothing to rebuild.
  IF to_regclass('account_balance_history') IS NOT NULL THEN
    PERFORM btracker_backend.rebuild_cleared_accounts_if_needed();
  END IF;
END
$$;

RESET ROLE;

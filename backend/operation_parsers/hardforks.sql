/*
Hardfork-specific processing functions.
Called by: Multiple process files (process_balances.sql, process_delegations.sql, etc.)

Handles HF23 (Hive fork from Steem) balance resets:
- Steemit Inc accounts had all balances zeroed
- Affected: main balances, rewards, savings (with pending withdrawals), open limit orders,
  delegations, withdrawals

This function processes hardfork_hive_operation at the specific HF23 block,
resetting all financial state for impacted accounts to zero.

Note: This function may become unnecessary after changes to hardfork_hive_operation
in HAF that provide more structured data.
*/

SET ROLE btracker_owner;

-- Accounts cleared at HF23: the accounts of the hardfork_hive_operations in the HF23 block.
CREATE OR REPLACE FUNCTION btracker_backend.hf_23_cleared_accounts(__hardfork_23_block INT)
RETURNS TABLE (account_id INT)
LANGUAGE 'sql' STABLE
AS
$$
  SELECT
    (SELECT av.id FROM accounts_view av WHERE av.name = ov.body_value->>'account')
  FROM operations_view ov
  WHERE ov.op_type_id = btracker_backend.op_hardfork_hive() AND ov.block_num = __hardfork_23_block
$$;

-- Process HF23 balance resets for Steemit Inc accounts.
-- At the Hive fork, affected accounts had all financial state zeroed:
-- - Main balances (HIVE, HBD, VESTS)
-- - Pending rewards
-- - Savings balances and pending withdrawals (hived removes the withdrawals
--   without a fill or cancel operation, so their transfer_saving_id rows go here)
-- - Open limit orders (hived cancels them without a limit_order_cancelled
--   operation, so they leave order_state and are closed as cancelled here,
--   also in the counts of their creation block and its group)
-- Delegations and withdrawals are handled by their respective process files.
CREATE OR REPLACE FUNCTION btracker_backend.process_hf_23(__hardfork_23_block INT)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  __balances INT;
  __rewards INT;
  __savings INT;
  __withdraw_requests INT;
  __orders INT;
  __order_lifecycle INT;
  __order_stats INT;
  __group_blocks CONSTANT INT := btracker_backend.order_stats_group_blocks();
BEGIN
  -- Find all accounts affected by HF23 at the specific block
  WITH hardfork_hive_operation AS MATERIALIZED (
    SELECT account_id FROM btracker_backend.hf_23_cleared_accounts(__hardfork_23_block)
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
  ),
  delete_withdraw_requests AS (
    DELETE FROM transfer_saving_id tsi
    USING hardfork_hive_operation hho
    WHERE tsi.account = hho.account_id
    RETURNING tsi.account AS cleaned_account_id
  ),
  delete_orders AS (
    DELETE FROM order_state os
    USING hardfork_hive_operation hho
    WHERE os.owner_id = hho.account_id
    RETURNING os.owner_id AS cleaned_account_id
  ),
  close_order_lifecycle AS (
    UPDATE order_lifecycle ol SET
      end_block = __hardfork_23_block,
      outcome = 'cancelled'
    FROM hardfork_hive_operation hho
    WHERE ol.owner_id = hho.account_id AND ol.end_block IS NULL
    RETURNING ol.block_created
  ),
  -- The closed orders' creation blocks already count them as created.
  closed_per_block AS MATERIALIZED (
    SELECT block_created, count(*) AS closed
    FROM close_order_lifecycle
    GROUP BY block_created
  ),
  close_order_stats_by_block AS (
    UPDATE order_stats_by_block s SET
      cancelled = s.cancelled + c.closed
    FROM closed_per_block c
    WHERE s.block_created = c.block_created
    RETURNING 1
  ),
  close_order_stats_by_block_group AS (
    UPDATE order_stats_by_block_group s SET
      cancelled = s.cancelled + c.closed
    FROM (
      SELECT block_created / __group_blocks AS block_group, sum(closed) AS closed
      FROM closed_per_block
      GROUP BY block_created / __group_blocks
    ) c
    WHERE s.block_group = c.block_group
    RETURNING 1
  )
  SELECT
    (SELECT count(*) FROM update_balances) AS balances,
    (SELECT count(*) FROM update_rewards) AS rewards,
    (SELECT count(*) FROM update_savings) AS savings,
    (SELECT count(*) FROM delete_withdraw_requests) AS withdraw_requests,
    (SELECT count(*) FROM delete_orders) AS orders,
    (SELECT count(*) FROM close_order_lifecycle) AS order_lifecycle,
    (SELECT count(*) FROM close_order_stats_by_block) + (SELECT count(*) FROM close_order_stats_by_block_group) AS order_stats
  INTO __balances, __rewards, __savings, __withdraw_requests, __orders, __order_lifecycle, __order_stats;

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
-- process_withdrawals resets account_withdraws.delayed_vests at the same ops (hived clears
-- the delayed votes HF24 introduced along with the vesting shares).
--
-- Known limit, not reached on mainnet (checked on a full node 2026-10-23: btracker holds no
-- savings or reward rows for steem.dao):
--   * hive.fund's side of a consolidate stays as HAF's collector emits it: total_moved is
--     credited as liquid, while hived moves savings and pending rewards into hive.fund's
--     savings and rewards.
--
-- Resolved once per batch by btracker_prefetch_operations into the temp table
-- _btracker_clears_batch (prefetch_account_clears below); the five readers (balances,
-- withdrawals, savings, and twice in rewards) take their range from it with
-- account_clears_in_range.

-- Fills _btracker_clears_batch with one row per clearing op in [_from, _to]:
-- (account_id, source_op, op_type_id, source_op_block). _btracker_ops_batch must hold
-- that range's operations (_batch_rows of them). A single-block batch (live) keeps the
-- previous batch's table (btracker_reset_batch_table), so the readers' plans stay cached.
CREATE OR REPLACE FUNCTION btracker_backend.prefetch_account_clears(_from INT, _to INT, _batch_rows BIGINT)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  _op_clear_null  INT := btracker_backend.op_clear_null_account_balance();
  _op_consolidate INT := btracker_backend.op_consolidate_treasury_balance();
  -- A batch up to this many operations (a live block holds a few dozen) is searched in
  -- _btracker_ops_batch; a larger one (massive sync) probes operations_view through HAF's
  -- (op_type_id, block_num) index instead: a scan of the unindexed batch cost 0.5-0.7 s
  -- on a 10k-block batch, the probe milliseconds - still far more than a live block's scan.
  __scan_batch    BOOLEAN := _batch_rows <= 5000;
BEGIN
  IF NOT btracker_reset_batch_table('_btracker_clears_batch', _from = _to) THEN
    CREATE TEMP TABLE _btracker_clears_batch(
      account_id      INT,
      source_op       BIGINT,
      op_type_id      SMALLINT,
      source_op_block INT
    );
  END IF;

  -- The account is a scalar lookup by name (an index probe, run only once a clear is
  -- found), not a join on the computed name: that join sorts all of hafd.accounts.
  -- Never a NULL account: callers split rows by EXISTS over account_id. (Both accounts
  -- exist on any chain that emits these ops.)
  INSERT INTO _btracker_clears_batch
  WITH clear_ops AS (
    SELECT ov.id, ov.op_type_id, ov.block_num
    FROM _btracker_ops_batch ov
    WHERE __scan_batch
      AND ov.op_type_id IN (_op_clear_null, _op_consolidate)
      AND ov.block_num BETWEEN _from AND _to

    UNION ALL

    SELECT ov.id, ov.op_type_id, ov.block_num
    FROM operations_view ov
    WHERE NOT __scan_batch
      AND ov.op_type_id IN (_op_clear_null, _op_consolidate)
      AND ov.block_num BETWEEN _from AND _to
  ),
  resolved AS (
    SELECT
      CASE WHEN co.op_type_id = _op_clear_null
        THEN (SELECT av.id FROM accounts_view av WHERE av.name = 'null')
        ELSE (SELECT av.id FROM accounts_view av WHERE av.name = 'steem.dao')
      END::INT AS account_id,
      co.id::BIGINT AS source_op,
      co.op_type_id::SMALLINT AS op_type_id,
      co.block_num::INT AS source_op_block
    FROM clear_ops co
  )
  SELECT r.account_id, r.source_op, r.op_type_id, r.source_op_block
  FROM resolved r
  WHERE r.account_id IS NOT NULL;
END
$$;

-- The batch's clearing ops in [_from, _to] (a sub-range of the prefetched batch, as
-- around HF23), one row per op.
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
BEGIN
  RETURN QUERY
  SELECT cb.account_id, cb.source_op, cb.op_type_id, cb.source_op_block
  FROM _btracker_clears_batch cb
  WHERE cb.source_op_block BETWEEN _from AND _to;
END
$$;

RESET ROLE;

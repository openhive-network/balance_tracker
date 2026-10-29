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
-- process_withdrawals resets account_withdraws.delayed_vests at the same ops (hived clears
-- the delayed votes HF24 introduced along with the vesting shares).
--
-- Known limit, not reached on mainnet (checked on a full node 2026-10-23: btracker holds no
-- savings or reward rows for steem.dao):
--   * hive.fund's side of a consolidate stays as HAF's collector emits it: total_moved is
--     credited as liquid, while hived moves savings and pending rewards into hive.fund's
--     savings and rewards.
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
  -- Never returns a NULL account: callers split rows by EXISTS over account_id. (Both
  -- accounts exist on any chain that emits these ops.)
  RETURN QUERY
  SELECT av.id::INT, ob.id::BIGINT, ob.op_type_id::SMALLINT, ob.block_num::INT
  FROM _btracker_ops_batch ob
  JOIN accounts_view av
    ON av.name = CASE WHEN ob.op_type_id = _op_clear_null THEN 'null' ELSE 'steem.dao' END
  WHERE ob.op_type_id IN (_op_clear_null, _op_consolidate)
    AND ob.block_num BETWEEN _from AND _to;
END
$$;

-- The repair of data an older btracker processed without these clears is in
-- backend/repairs/cleared_accounts.sql.

RESET ROLE;

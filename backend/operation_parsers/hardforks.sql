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
      (SELECT av.id FROM accounts_view av WHERE av.name = (ov.body)->'value'->>'account') AS account_id
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

RESET ROLE;

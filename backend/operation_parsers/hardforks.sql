/*
Hardfork-specific processing functions.
Called by: Multiple process files (process_balances.sql, process_delegations.sql, etc.)

Handles HF23 (Hive fork from Steem) balance resets:
- Steemit Inc accounts had all balances zeroed
- Affected: main balances, rewards, savings (with pending withdrawals), delegations, withdrawals

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
  )
  SELECT
    (SELECT count(*) FROM update_balances) AS balances,
    (SELECT count(*) FROM update_rewards) AS rewards,
    (SELECT count(*) FROM update_savings) AS savings,
    (SELECT count(*) FROM delete_withdraw_requests) AS withdraw_requests
  INTO __balances, __rewards, __savings, __withdraw_requests;

END
$$;

RESET ROLE;

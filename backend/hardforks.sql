SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION process_hardfork_hive_operation(body JSONB)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
WITH hardfork_hive_operation AS MATERIALIZED
(
  SELECT 
    (SELECT id FROM accounts_view WHERE name = (body)->'value'->>'account') AS _account
),
balances AS (
  UPDATE current_account_balances SET
    balance = 0
  FROM hardfork_hive_operation
  WHERE account = _account
),
rewards AS (
  UPDATE account_rewards SET
    balance = 0
  FROM hardfork_hive_operation
  WHERE account = _account
),
delegations AS (
  UPDATE account_delegations SET
    delegated_vests = 0
  FROM hardfork_hive_operation
  WHERE account = _account
),
savings AS (
  UPDATE account_savings SET
    saving_balance = 0,
    savings_withdraw_requests = 0
  FROM hardfork_hive_operation
  WHERE account = _account
)
  UPDATE account_withdraws SET
    vesting_withdraw_rate = 0,
    to_withdraw = 0,
    withdrawn = 0,
    delayed_vests = 0
  FROM hardfork_hive_operation
  WHERE account = _account;

END
$$;

CREATE OR REPLACE FUNCTION process_hardfork(_hardfork_id INT)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN

  CASE 

  WHEN _hardfork_id = 1 THEN
  WITH account_withdraws AS MATERIALIZED 
  (
  SELECT 
    account as account_id, 
    (to_withdraw * 1000000) AS _to_withdraw,
    ((to_withdraw * 1000000)/ 104) AS _vesting_withdraw_rate,
    (withdrawn * 1000000) AS _withdrawn
  FROM account_withdraws
  )
  UPDATE account_withdraws SET
    vesting_withdraw_rate = ad._vesting_withdraw_rate,
    to_withdraw = ad._to_withdraw,
    withdrawn = ad._withdrawn 
  FROM account_withdraws ad
  WHERE account = ad.account_id;

  WHEN _hardfork_id = 16 THEN
  UPDATE btracker_app_status SET withdraw_rate = 13;

  WHEN _hardfork_id = 24 THEN
  UPDATE btracker_app_status SET start_delayed_vests = TRUE;

  ELSE
  END CASE;

END
$$;

RESET ROLE;

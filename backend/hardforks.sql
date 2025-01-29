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

--this function won't be necessary after changes in hardfork_hive_operation
CREATE OR REPLACE FUNCTION process_hf_23(__hardfork_23_block INT)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  __balances INT;
  __rewards INT;
  __savings INT;
BEGIN
  WITH hardfork_hive_operation AS MATERIALIZED (
    SELECT 
      (SELECT av.id FROM accounts_view av WHERE av.name = (ov.body)->'value'->>'account') AS account_id
    FROM operations_view ov
    WHERE ov.op_type_id = 68 AND block_num = __hardfork_23_block
  ),
  update_balances AS (
    UPDATE current_account_balances cab SET
      balance = 0
    FROM hardfork_hive_operation hho
    WHERE cab.account = hho.account_id
    RETURNING cab.account AS cleaned_account_id
  ),
  update_rewards AS (
    UPDATE account_rewards ar SET
      balance = 0
    FROM hardfork_hive_operation hho
    WHERE ar.account = hho.account_id
    RETURNING ar.account AS cleaned_account_id
  ),
  update_savings AS (
    UPDATE account_savings acs SET
      saving_balance = 0,
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

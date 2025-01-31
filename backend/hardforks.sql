SET ROLE btracker_owner;

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

RESET ROLE;

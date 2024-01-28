SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_app.process_block_range_data_a(
    IN _from INT, IN _to INT, IN _report_step INT = 1000
)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE 
 _result INT;
BEGIN
--RAISE NOTICE 'Processing balances';
WITH balance_impacting_ops AS MATERIALIZED
(
  SELECT ot.id
  FROM hive.operation_types ot
  WHERE ot.name IN (SELECT * FROM hive.get_balance_impacting_operations())
),
ops_in_range AS  
(
  SELECT 
    ho.id AS source_op,
    ho.block_num AS source_op_block,
    ho.body_binary
  FROM hive.btracker_app_operations_view ho --- APP specific view must be used, to correctly handle reversible part of the data.
  WHERE ho.op_type_id = ANY(SELECT id FROM balance_impacting_ops) AND ho.block_num BETWEEN _from AND _to
  ORDER BY ho.block_num, ho.id
),
get_impacted AS (
SELECT 
  hive.get_impacted_balances(gib.body_binary, gib.source_op_block > 905693) AS get_impacted_balances,
  gib.source_op,
  gib.source_op_block 
FROM ops_in_range gib
),
get_impacted_two AS (
SELECT 

  (SELECT av.id FROM hive.btracker_app_accounts_view av WHERE av.name = (gi.get_impacted_balances).account_name) AS account_id,
  (gi.get_impacted_balances).asset_symbol_nai AS nai,
  (gi.get_impacted_balances).amount AS balance,
  gi.source_op,
  gi.source_op_block
FROM get_impacted gi
),
insert_balances AS MATERIALIZED (
SELECT
  git.source_op,
  git.source_op_block,
  btracker_app.process_balances(git.account_id, git.nai, git.balance, git.source_op, git.source_op_block)

FROM get_impacted_two git
ORDER BY git.source_op, git.source_op_block
)

SELECT COUNT(*) INTO _result
FROM insert_balances;
	--	if __balance_change.source_op_block = 199701 then
	--	RAISE NOTICE 'Balance change data: account: %, amount change: %, source_op: %, source_block: %, current_balance: %', __balance_change.account, __balance_change.balance, __balance_change.source_op, __balance_change.source_op_block, COALESCE(__current_balance, 0);
	--	end if;

  --IF __balance_change.source_op_block % _report_step = 0 AND __last_reported_block != __balance_change.source_op_block THEN
  --  RAISE NOTICE 'Processed data for block: %', __balance_change.source_op_block;
  --  __last_reported_block := __balance_change.source_op_block;
  --END IF;
END
$$;

CREATE OR REPLACE FUNCTION btracker_app.process_block_range_data_b(IN _from INT, IN _to INT, IN _report_step INT = 1000)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  _result INT;
BEGIN
--RAISE NOTICE 'Processing delegations, rewards, savings, withdraws';)
--delegations (40,41,62)
--savings (32,33,34,59,55)
--rewards (39,51,52,63,53)
--withdraws (4,20,56)
--hardforks (60)

WITH process_block_range_data_b AS MATERIALIZED 
( 
SELECT 
  ov.body AS body,
  ov.id AS source_op,
  ov.block_num as source_op_block,
  ov.op_type_id 
FROM hive.btracker_app_operations_view ov
WHERE 
  (ov.op_type_id IN (40,41,62,32,33,34,59,39,4,20,56,60,52,53,77,70,68) or
  (ov.op_type_id = 55 and (ov.body->'value'->>'is_saved_into_hbd_balance')::BOOLEAN = false)  or
  (ov.op_type_id IN (51,63) and (ov.body->'value'->>'payout_must_be_claimed')::BOOLEAN = true))
  AND ov.block_num BETWEEN _from AND _to
),
insert_balance AS MATERIALIZED 
(
SELECT 
  pbr.source_op,
  pbr.source_op_block,
  (CASE 
  WHEN pbr.op_type_id = 40 THEN
    btracker_app.process_delegate_vesting_shares_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 41 THEN
    btracker_app.process_account_create_with_delegation_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 62 THEN
    btracker_app.process_return_vesting_delegation_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 32 THEN
    btracker_app.process_transfer_to_savings_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 33 THEN
    btracker_app.process_transfer_from_savings_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 34 THEN
    btracker_app.process_cancel_transfer_from_savings_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 59 THEN
    btracker_app.process_fill_transfer_from_savings_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 55 THEN
    btracker_app.process_interest_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 39 THEN
    btracker_app.process_claim_reward_balance_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 51 THEN
    btracker_app.process_author_reward_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 52 THEN
    btracker_app.process_curation_reward_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 63 THEN
    btracker_app.process_comment_benefactor_reward_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 4 THEN
    btracker_app.process_withdraw_vesting_operation(pbr.body, (SELECT withdraw_rate FROM btracker_app.app_status))

  WHEN pbr.op_type_id = 20 THEN
    btracker_app.process_set_withdraw_vesting_route_operation(pbr.body)

  WHEN pbr.op_type_id = 56 THEN
    btracker_app.process_fill_vesting_withdraw_operation(pbr.body, (SELECT start_delayed_vests FROM btracker_app.app_status))

  WHEN pbr.op_type_id = 53 THEN
    btracker_app.process_comment_reward_operation(pbr.body)

  WHEN pbr.op_type_id = 77 AND (SELECT start_delayed_vests FROM btracker_app.app_status) = TRUE THEN
    btracker_app.process_transfer_to_vesting_completed_operation(pbr.body)

  WHEN pbr.op_type_id = 70 AND (SELECT start_delayed_vests FROM btracker_app.app_status) = TRUE THEN
    btracker_app.process_delayed_voting_operation(pbr.body)

  WHEN pbr.op_type_id = 68 THEN
    btracker_app.process_hardfork_hive_operation(pbr.body)

  WHEN pbr.op_type_id = 60 THEN
    btracker_app.process_hardfork(((pbr.body)->'value'->>'hardfork_id')::INT)
  END)
FROM process_block_range_data_b pbr
ORDER BY pbr.source_op_block, pbr.source_op
)

SELECT COUNT(*) INTO _result 
FROM insert_balance;

END
$$;

CREATE OR REPLACE FUNCTION btracker_app.process_hardfork_hive_operation(body JSONB)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
WITH hardfork_hive_operation AS MATERIALIZED
(
  SELECT 
    (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'account') AS _account
),
balances AS (
  UPDATE btracker_app.current_account_balances SET
    balance = 0
  FROM hardfork_hive_operation
  WHERE account = _account
),
rewards AS (
  UPDATE btracker_app.account_rewards SET
    balance = 0
  FROM hardfork_hive_operation
  WHERE account = _account
),
delegations AS (
  UPDATE btracker_app.account_delegations SET
    delegated_vests = 0
  FROM hardfork_hive_operation
  WHERE account = _account
),
savings AS (
  UPDATE btracker_app.account_savings SET
    saving_balance = 0,
    savings_withdraw_requests = 0
  FROM hardfork_hive_operation
  WHERE account = _account
)
  UPDATE btracker_app.account_withdraws SET
    vesting_withdraw_rate = 0,
    to_withdraw = 0,
    withdrawn = 0,
    delayed_vests = 0
  FROM hardfork_hive_operation
  WHERE account = _account;

END
$$;

CREATE OR REPLACE FUNCTION btracker_app.process_hardfork(_hardfork_id INT)
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
  FROM btracker_app.account_withdraws
  )
  UPDATE btracker_app.account_withdraws SET 
    vesting_withdraw_rate = ad._vesting_withdraw_rate,
    to_withdraw = ad._to_withdraw,
    withdrawn = ad._withdrawn 
  FROM account_withdraws ad
  WHERE account = ad.account_id;

  WHEN _hardfork_id = 16 THEN
  UPDATE btracker_app.app_status SET withdraw_rate = 13;

  WHEN _hardfork_id = 24 THEN
  UPDATE btracker_app.app_status SET start_delayed_vests = TRUE;

  ELSE
  END CASE;

END
$$;

CREATE OR REPLACE FUNCTION btracker_app.process_balances(
    _account_id INT,
    _nai INT,
    _balance BIGINT,
    _source_op BIGINT,
    _source_op_block INT
)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  __current_balance BIGINT;
BEGIN

INSERT INTO btracker_app.current_account_balances
  (account, nai, source_op, source_op_block, balance)
SELECT 
  _account_id,
  _nai, 
  _source_op, 
  _source_op_block, 
  _balance
ON CONFLICT ON CONSTRAINT pk_current_account_balances DO
  UPDATE SET balance = btracker_app.current_account_balances.balance + EXCLUDED.balance,
              source_op = EXCLUDED.source_op,
              source_op_block = EXCLUDED.source_op_block
RETURNING balance into __current_balance;

INSERT INTO btracker_app.account_balance_history
  (account, nai, source_op, source_op_block, balance)
SELECT 
_account_id, _nai, _source_op, _source_op_block, COALESCE(__current_balance, 0);

END 
$$;

RESET ROLE;

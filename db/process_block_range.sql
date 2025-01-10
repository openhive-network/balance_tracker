SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_block_range_data_a(
    IN _from INT, IN _to INT
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
 __balance_history INT;
 __current_balances INT;
BEGIN
--RAISE NOTICE 'Processing balances';

-- get all operations that can impact balances
WITH balance_impacting_ops AS MATERIALIZED
(
  SELECT ot.id
  FROM hafd.operation_types ot
  WHERE ot.name IN (SELECT * FROM hive.get_balance_impacting_operations())
),
ops_in_range AS  
(
  SELECT 
    ho.id AS source_op,
    ho.block_num AS source_op_block,
    ho.body_binary
  FROM operations_view ho --- APP specific view must be used, to correctly handle reversible part of the data.
  WHERE ho.op_type_id = ANY(SELECT id FROM balance_impacting_ops) AND ho.block_num BETWEEN _from AND _to
  ORDER BY ho.block_num, ho.id
),

-- convert balances depending on hardforks
get_impacted_bal AS (
  SELECT 
    hive.get_impacted_balances(gib.body_binary, gib.source_op_block > 905693) AS get_impacted_balances,
    gib.source_op,
    gib.source_op_block 
  FROM ops_in_range gib
),
convert_parameters AS (
  SELECT 

    (SELECT av.id FROM accounts_view av WHERE av.name = (gi.get_impacted_balances).account_name) AS account_id,
    (gi.get_impacted_balances).asset_symbol_nai AS nai,
    (gi.get_impacted_balances).amount AS balance,
    gi.source_op,
    gi.source_op_block
  FROM get_impacted_bal gi
),

-- prepare accounts that were impacted by the operations
group_by_account_nai AS (
  SELECT 
    cp.account_id,
    cp.nai
  FROM convert_parameters cp
  GROUP BY cp.account_id, cp.nai
),
get_latest_balance AS (
  SELECT 
    gan.account_id,
    gan.nai,
    COALESCE(cab.balance, 0) as balance,
    0 AS source_op,
    0 AS source_op_block
--    (CASE WHEN cab.balance IS NULL THEN FALSE ELSE TRUE END) AS prev_balance_exists
  FROM group_by_account_nai gan
  LEFT JOIN current_account_balances cab ON cab.account = gan.account_id AND cab.nai = gan.nai
),

-- prepare balances (sum balances for each account)
union_latest_balance_with_impacted_balances AS (
  SELECT 
    cp.account_id, 
    cp.nai,
    cp.balance,
    cp.source_op,
    cp.source_op_block
  FROM convert_parameters cp

  UNION ALL

-- latest stored balance is needed to replecate balance history
  SELECT 
    glb.account_id,
    glb.nai,
    glb.balance,
    glb.source_op,
    glb.source_op_block
  FROM get_latest_balance glb
),

/*
sum previous balances

 id  | balance | sum-balance
  2       10     10 + 5 + 1
  1       5        5 + 1
  0       1          1

*/

prepare_balance_history AS MATERIALIZED (
  SELECT 
    ulb.account_id,
    ulb.nai,
    SUM(ulb.balance) OVER (PARTITION BY ulb.account_id, ulb.nai ORDER BY ulb.source_op, ulb.balance ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS balance,
    ulb.source_op,
    ulb.source_op_block
  FROM union_latest_balance_with_impacted_balances ulb
),

-- find the new latest balance
prepare_newest_balance AS (
  SELECT
    account_id,
    nai,
    MAX(source_op) AS source_op
  FROM prepare_balance_history 
  GROUP BY account_id, nai
),

remove_duplicate_transfers_in_current_balance AS MATERIALIZED (
  SELECT 
    pbh.account_id,
    pbh.nai,
    pbh.source_op,
    pbh.source_op_block,
  -- choose the maximum balance for the case when has made transfer to itself
    MAX(pbh.balance) AS balance
  FROM prepare_newest_balance nw 
  JOIN prepare_balance_history pbh ON pbh.account_id = nw.account_id AND pbh.nai = nw.nai AND pbh.source_op = nw.source_op
  -- this group by is needed to avoid duplicate entries (account can make a transfer to itself)
  GROUP BY pbh.account_id, pbh.nai, pbh.source_op, pbh.source_op_block
),
insert_current_account_balances AS (
  INSERT INTO current_account_balances AS acc_balances
    (account, nai, source_op, source_op_block, balance)
  SELECT 
    rd.account_id,
    rd.nai,
    rd.source_op,
    rd.source_op_block,
    rd.balance
  FROM remove_duplicate_transfers_in_current_balance rd 
  ON CONFLICT ON CONSTRAINT pk_current_account_balances DO
  UPDATE SET 
    balance = EXCLUDED.balance,
    source_op = EXCLUDED.source_op,
    source_op_block = EXCLUDED.source_op_block
  RETURNING (xmax = 0) as is_new_entry, acc_balances.account
),

remove_latest_stored_balance_record AS MATERIALIZED (
  SELECT 
    pbh.account_id,
    pbh.nai,
    pbh.source_op,
    pbh.source_op_block,
    pbh.balance
  FROM prepare_balance_history pbh
  -- remove with prepared previous balance
  WHERE pbh.source_op > 0
  ORDER BY pbh.source_op
),
insert_account_balance_history AS (
  INSERT INTO account_balance_history AS acc_history
    (account, nai, source_op, source_op_block, balance)
  SELECT 
    pbh.account_id,
    pbh.nai,
    pbh.source_op,
    pbh.source_op_block,
    pbh.balance
  FROM remove_latest_stored_balance_record pbh
  RETURNING (xmax = 0) as is_new_entry, acc_history.account
)

SELECT
  (SELECT count(*) FROM insert_account_balance_history) as balance_history,
  (SELECT count(*) FROM insert_current_account_balances) AS current_balances
INTO __balance_history, __current_balances;

END
$$;

CREATE OR REPLACE FUNCTION btracker_block_range_data_b(IN _from INT, IN _to INT, IN _report_step INT = 1000)
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
  ov.body_binary::jsonb AS body,
  ov.id AS source_op,
  ov.block_num as source_op_block,
  ov.op_type_id 
FROM operations_view ov
WHERE 
 ov.op_type_id IN (40,41,62,32,33,34,59,39,4,20,56,60,52,53,77,70,68,51,63,55) AND 
 ov.block_num BETWEEN _from AND _to
),
filter_ops AS 
(
SELECT 
  ov.body,
  ov.source_op,
  ov.source_op_block,
  ov.op_type_id 
FROM process_block_range_data_b ov
WHERE 
  (ov.op_type_id IN (40,41,62,32,33,34,59,39,4,20,56,60,52,53,77,70,68) or
  (ov.op_type_id = 55 and (ov.body->'value'->>'is_saved_into_hbd_balance')::BOOLEAN = false)  or
  (ov.op_type_id IN (51,63) and (ov.body->'value'->>'payout_must_be_claimed')::BOOLEAN = true))
),
insert_balance AS MATERIALIZED 
(
SELECT 
  pbr.source_op,
  pbr.source_op_block,
  (CASE 
  WHEN pbr.op_type_id = 40 THEN
    process_delegate_vesting_shares_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 41 THEN
    process_account_create_with_delegation_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 62 THEN
    process_return_vesting_delegation_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 32 THEN
    process_transfer_to_savings_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 33 THEN
    process_transfer_from_savings_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 34 THEN
    process_cancel_transfer_from_savings_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 59 THEN
    process_fill_transfer_from_savings_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 55 THEN
    process_interest_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 39 THEN
    process_claim_reward_balance_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 51 THEN
    process_author_reward_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 52 THEN
    process_curation_reward_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 63 THEN
    process_comment_benefactor_reward_operation(pbr.body, pbr.source_op, pbr.source_op_block)

  WHEN pbr.op_type_id = 4 THEN
    process_withdraw_vesting_operation(pbr.body, (SELECT withdraw_rate FROM btracker_app_status))

  WHEN pbr.op_type_id = 20 THEN
    process_set_withdraw_vesting_route_operation(pbr.body)

  WHEN pbr.op_type_id = 56 THEN
    process_fill_vesting_withdraw_operation(pbr.body, (SELECT start_delayed_vests FROM btracker_app_status))

  WHEN pbr.op_type_id = 53 THEN
    process_comment_reward_operation(pbr.body)

  WHEN pbr.op_type_id = 77 AND (SELECT start_delayed_vests FROM btracker_app_status) = TRUE THEN
    process_transfer_to_vesting_completed_operation(pbr.body)

  WHEN pbr.op_type_id = 70 AND (SELECT start_delayed_vests FROM btracker_app_status) = TRUE THEN
    process_delayed_voting_operation(pbr.body)

  WHEN pbr.op_type_id = 68 THEN
    process_hardfork_hive_operation(pbr.body)

  WHEN pbr.op_type_id = 60 THEN
    process_hardfork(((pbr.body)->'value'->>'hardfork_id')::INT)
  END)
FROM filter_ops pbr
ORDER BY pbr.source_op_block, pbr.source_op
)

SELECT COUNT(*) INTO _result 
FROM insert_balance;

END
$$;

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

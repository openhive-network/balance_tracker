SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION process_block_range_withdrawals(IN _from INT, IN _to INT, IN _report_step INT = 1000)
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
 ov.op_type_id IN (4,20,56,60,77,70,68) AND 
 ov.block_num BETWEEN _from AND _to
),
insert_balance AS MATERIALIZED 
(
SELECT 
  pbr.source_op,
  pbr.source_op_block,
  (CASE 
  WHEN pbr.op_type_id = 4 THEN
    process_withdraw_vesting_operation(pbr.body, (SELECT withdraw_rate FROM btracker_app_status))

  WHEN pbr.op_type_id = 20 THEN
    process_set_withdraw_vesting_route_operation(pbr.body)

  WHEN pbr.op_type_id = 56 THEN
    process_fill_vesting_withdraw_operation(pbr.body, (SELECT start_delayed_vests FROM btracker_app_status))

  WHEN pbr.op_type_id = 77 AND (SELECT start_delayed_vests FROM btracker_app_status) = TRUE THEN
    process_transfer_to_vesting_completed_operation(pbr.body)

  WHEN pbr.op_type_id = 70 AND (SELECT start_delayed_vests FROM btracker_app_status) = TRUE THEN
    process_delayed_voting_operation(pbr.body)

  WHEN pbr.op_type_id = 68 THEN
    process_hardfork_hive_operation(pbr.body)

  WHEN pbr.op_type_id = 60 THEN
    process_hardfork(((pbr.body)->'value'->>'hardfork_id')::INT)
  END)
FROM process_block_range_data_b pbr
ORDER BY pbr.source_op_block, pbr.source_op
)

SELECT COUNT(*) INTO _result 
FROM insert_balance;

END
$$;

RESET ROLE;

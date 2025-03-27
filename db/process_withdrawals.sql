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
  __insert_routes INT;
  __delete_routes INT;
  __insert_sum_of_routes INT;
  __delete_hf23_routes_count INT;
  __delete_hf23_routes INT;
  __insert_not_yet_filled_withdrawals INT;
  __reset_filled_withdrawals  INT;
BEGIN
-----------------------------------------WITHDRAWALS---------------------------------------------
WITH process_block_range_data_b AS MATERIALIZED
(
  SELECT 
    impacted_withdraws.account_name,
    impacted_withdraws.withdrawn,
    impacted_withdraws.vesting_withdraw_rate,
    impacted_withdraws.to_withdraw,
    ov.id AS source_op
  FROM operations_view ov
  CROSS JOIN btracker_backend.get_impacted_withdraws(ov.body, ov.op_type_id, ov.block_num) AS impacted_withdraws
  WHERE 
    ov.op_type_id IN (4,68) AND 
    ov.block_num BETWEEN _from AND _to
),
group_by_account AS (
  SELECT 
    account_name,
    withdrawn,
    vesting_withdraw_rate,
    to_withdraw,
    source_op,
    ROW_NUMBER() OVER (PARTITION BY account_name ORDER BY source_op DESC) AS row_num
  FROM process_block_range_data_b
),
join_latest_withdraw AS (
  SELECT 
    (SELECT av.id FROM accounts_view av WHERE av.name = account_name) AS account_id,
    withdrawn,
    vesting_withdraw_rate,
    to_withdraw,
    source_op
  FROM group_by_account
  WHERE row_num = 1
)
INSERT INTO account_withdraws
  (account, vesting_withdraw_rate, to_withdraw, withdrawn, source_op)
SELECT 
  jlw.account_id,
  jlw.vesting_withdraw_rate,
  jlw.to_withdraw,
  jlw.withdrawn,
  jlw.source_op
FROM join_latest_withdraw jlw
ON CONFLICT ON CONSTRAINT pk_account_withdraws
DO UPDATE SET
  vesting_withdraw_rate = EXCLUDED.vesting_withdraw_rate,
  to_withdraw = EXCLUDED.to_withdraw,
  withdrawn = EXCLUDED.withdrawn,
  source_op = EXCLUDED.source_op;

-----------------------------------------WITHDRAWAL ROUTES---------------------------------------------
WITH process_block_range_data_b AS (
  SELECT 
    withdraw_vesting_route.from_account,
    withdraw_vesting_route.to_account,
    withdraw_vesting_route.percent,
    ov.id AS source_op
  FROM operations_view ov
  CROSS JOIN btracker_backend.process_set_withdraw_vesting_route_operation(ov.body) AS withdraw_vesting_route
  WHERE 
    ov.op_type_id = 20 AND 
    ov.block_num BETWEEN _from AND _to
),
group_by_account AS (
  SELECT 
    from_account,
    to_account,
    percent,
    source_op,
    ROW_NUMBER() OVER (PARTITION BY from_account, to_account ORDER BY source_op DESC) AS row_num
  FROM process_block_range_data_b
),
join_latest_withdraw_routes AS (
  SELECT 
    (SELECT av.id FROM accounts_view av WHERE av.name = from_account) AS from_account,
    (SELECT av.id FROM accounts_view av WHERE av.name = to_account) AS to_account,
    percent,
    source_op
  FROM group_by_account 
  WHERE row_num = 1
),
join_prev_route AS (
  SELECT 
    cp.from_account,
    cp.to_account,
    cp.percent,
    cr.percent AS prev_percent,
    cp.source_op
  FROM join_latest_withdraw_routes cp
  LEFT JOIN account_routes cr ON cr.account = cp.from_account AND cr.to_account = cp.to_account
),
add_routes AS MATERIALIZED (
  SELECT 
    from_account,
    to_account,
    percent,
    (
      CASE 
        WHEN prev_percent IS NULL AND percent != 0 THEN 
          1
        WHEN prev_percent IS NOT NULL AND percent != 0 THEN
          0
        WHEN prev_percent IS NOT NULL AND percent = 0 THEN 
          -1
        ELSE 
          0
      END
    ) AS withdraw_routes,
    source_op
  FROM join_prev_route
),
sum_routes AS (
  SELECT 
    from_account,
    SUM(withdraw_routes) AS withdraw_routes
  FROM add_routes
  GROUP BY from_account
),
insert_routes AS (
  INSERT INTO account_routes
    (account, to_account, percent, source_op)
  SELECT 
    ar.from_account,
    ar.to_account,
    ar.percent,
    ar.source_op
  FROM add_routes ar
  WHERE ar.percent != 0
  ON CONFLICT ON CONSTRAINT pk_account_routes
  DO UPDATE SET
    percent = EXCLUDED.percent,
    source_op = EXCLUDED.source_op
  RETURNING account
),
delete_routes AS (
  DELETE FROM account_routes ar
  USING add_routes ar2
  WHERE 
    ar.account = ar2.from_account AND 
    ar.to_account = ar2.to_account AND 
    ar2.percent = 0
  RETURNING ar.account
),
insert_sum_of_routes AS (
  INSERT INTO account_withdraws
    (account, withdraw_routes)
  SELECT 
    sr.from_account,
    sr.withdraw_routes
  FROM sum_routes sr
  ON CONFLICT ON CONSTRAINT pk_account_withdraws
  DO UPDATE SET
    withdraw_routes = account_withdraws.withdraw_routes + EXCLUDED.withdraw_routes
  RETURNING account
)
SELECT
  (SELECT count(*) FROM insert_routes) AS insert_routes,
  (SELECT count(*) FROM delete_routes) AS delete_routes,
  (SELECT count(*) FROM insert_sum_of_routes) AS insert_sum_of_routes
INTO __insert_routes, __delete_routes, __insert_sum_of_routes;


-- delete routes for hf23 accounts (it will be triggered only once - in hf23 block range)
WITH process_block_range_data_b AS (
  SELECT 
    (SELECT av.id FROM accounts_view av WHERE av.name = (ov.body->'value'->>'account')) AS account_id,
    ov.id AS source_op
  FROM operations_view ov
  WHERE 
    ov.op_type_id = 68 AND 
    ov.block_num BETWEEN _from AND _to
),
join_current_routes_for_hf23_accounts AS MATERIALIZED (
  SELECT 
    ar.account,
    ar.to_account
  FROM account_routes ar
  JOIN process_block_range_data_b gi ON ar.account = gi.account_id
  WHERE gi.source_op > ar.source_op
),
count_deleted_routes AS (
  SELECT
    account,
    COUNT(*) AS count
  FROM join_current_routes_for_hf23_accounts
  GROUP BY account
),
delete_hf23_routes_count AS (
  INSERT INTO account_withdraws
    (account, withdraw_routes)
  SELECT 
    cd.account,
    cd.count
  FROM count_deleted_routes cd
  ON CONFLICT ON CONSTRAINT pk_account_withdraws
  DO UPDATE SET
    withdraw_routes = account_withdraws.withdraw_routes - EXCLUDED.withdraw_routes
  RETURNING account
),
delete_hf23_routes AS (
  DELETE FROM account_routes ar
  USING join_current_routes_for_hf23_accounts jcr
  WHERE 
    ar.account = jcr.account AND 
    ar.to_account = jcr.to_account
  RETURNING ar.account
)
SELECT
  (SELECT count(*) FROM delete_hf23_routes_count) AS delete_hf23_routes_count,
  (SELECT count(*) FROM delete_hf23_routes) AS delete_hf23_routes
INTO __delete_hf23_routes_count, __delete_hf23_routes;

-----------------------------------------FILL WITHDRAWS---------------------------------------------

WITH process_block_range_data_b AS MATERIALIZED
(
  SELECT 
    (SELECT av.id FROM accounts_view av WHERE av.name = fill_vesting_withdraw_operation.account_name) AS account_id,
    fill_vesting_withdraw_operation.withdrawn,
    ov.id AS source_op
  FROM operations_view ov
  JOIN hafd.applied_hardforks ah ON ah.hardfork_num = 1
  CROSS JOIN btracker_backend.process_fill_vesting_withdraw_operation(ov.body, ov.block_num > ah.block_num) AS fill_vesting_withdraw_operation
  WHERE 
    ov.op_type_id IN (56) AND 
    ov.block_num BETWEEN _from AND _to
),
--------------------------------------------------------------------------------------
group_by_account AS (
  SELECT DISTINCT account_id
  FROM process_block_range_data_b
),
join_current_withdraw AS (
  SELECT 
    aw.account,
    aw.withdrawn,
    aw.to_withdraw,
    aw.source_op
  FROM account_withdraws aw
  JOIN group_by_account gba ON aw.account = gba.account_id
),
--------------------------------------------------------------------------------------
get_fills_conserning_current_withdrawal AS MATERIALIZED (
  SELECT 
    cp.account_id,
    SUM(cp.withdrawn) + aw.withdrawn AS withdrawn,
    MAX(aw.to_withdraw) AS to_withdraw
  FROM process_block_range_data_b cp
  JOIN join_current_withdraw aw ON aw.account = cp.account_id
  WHERE cp.source_op > aw.source_op
  GROUP BY cp.account_id, aw.withdrawn
),
insert_not_yet_filled_withdrawals AS (
  INSERT INTO account_withdraws
    (account, withdrawn)
  SELECT 
    gf.account_id,
    gf.withdrawn
  FROM get_fills_conserning_current_withdrawal gf
  WHERE gf.to_withdraw > gf.withdrawn
  ON CONFLICT ON CONSTRAINT pk_account_withdraws
  DO UPDATE SET
    withdrawn = EXCLUDED.withdrawn
  RETURNING account
),
reset_filled_withdrawals AS (
  INSERT INTO account_withdraws
    (account, vesting_withdraw_rate, to_withdraw, withdrawn)
  SELECT 
    gf.account_id,
    0,
    0,
    0
  FROM get_fills_conserning_current_withdrawal gf
  WHERE gf.to_withdraw <= gf.withdrawn
  ON CONFLICT ON CONSTRAINT pk_account_withdraws
  DO UPDATE SET
    vesting_withdraw_rate = EXCLUDED.vesting_withdraw_rate,
    to_withdraw = EXCLUDED.to_withdraw,
    withdrawn = EXCLUDED.withdrawn
  RETURNING account
)
SELECT
  (SELECT count(*) FROM insert_not_yet_filled_withdrawals) AS insert_not_yet_filled_withdrawals,
  (SELECT count(*) FROM reset_filled_withdrawals) AS reset_filled_withdrawals
INTO __insert_not_yet_filled_withdrawals, __reset_filled_withdrawals;

-----------------------------------------DELAYS---------------------------------------------

WITH process_block_range_data_b AS MATERIALIZED
(
  SELECT 
    (SELECT av.id FROM accounts_view av WHERE av.name = get_impacted_delayed_balances.from_account) AS from_account,
    (SELECT av.id FROM accounts_view av WHERE av.name = get_impacted_delayed_balances.to_account) AS to_account,
    get_impacted_delayed_balances.withdrawn,
    get_impacted_delayed_balances.deposited,
    ov.id AS source_op
  FROM operations_view ov
  -- start calculations from the first block after the 24 hardfork
  JOIN hafd.applied_hardforks ah ON ah.hardfork_num = 24 AND ah.block_num < ov.block_num
  CROSS JOIN btracker_backend.get_impacted_delayed_balances(ov.body, ov.op_type_id) AS get_impacted_delayed_balances
  WHERE 
    ov.op_type_id IN (56,77,70) AND 
    ov.block_num BETWEEN _from AND _to
),
--------------------------------------------------------------------------------------
-- prepare and sum all delayed vests
union_delays AS MATERIALIZED (
  SELECT 
    from_account AS account_id,
    withdrawn AS balance,
    source_op
  FROM process_block_range_data_b

  UNION ALL

  SELECT 
    to_account AS account_id,
    deposited AS balance,
    source_op
  FROM process_block_range_data_b
  WHERE to_account IS NOT NULL
),
group_by_account AS (
  SELECT DISTINCT account_id 
  FROM union_delays
),
join_prev_delays AS (
  SELECT 
    gb.account_id,
    COALESCE(aw.delayed_vests,0) AS balance,
    0 AS source_op
  FROM group_by_account gb
  LEFT JOIN account_withdraws aw ON aw.account = gb.account_id
),
union_prev_delays AS (
  SELECT 
    account_id,
    balance,
    source_op
  FROM join_prev_delays

  UNION ALL

  SELECT 
    account_id,
    balance,
    source_op
  FROM union_delays
),
add_row_number AS (
  SELECT 
    account_id,
    balance,
    source_op,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY source_op) AS row_num
  FROM union_prev_delays
),
recursive_delays AS MATERIALIZED (
  WITH RECURSIVE calculated_delays AS (
    SELECT 
      cp.account_id,
      cp.balance,
      cp.source_op,
      cp.row_num
    FROM add_row_number cp
    WHERE cp.row_num = 1

    UNION ALL

    SELECT 
      next_cp.account_id,
      GREATEST(next_cp.balance + prev.balance, 0) AS balance,
      next_cp.source_op,
      next_cp.row_num
    FROM calculated_delays prev
    JOIN add_row_number next_cp ON prev.account_id = next_cp.account_id AND next_cp.row_num = prev.row_num + 1
  )
  SELECT * FROM calculated_delays
),
latest_rows AS (
  SELECT
    account_id,
    balance,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY source_op DESC) AS rn
  FROM
    recursive_delays
)
--------------------------------------------------------------------------------------
INSERT INTO account_withdraws
  (account, delayed_vests)
SELECT 
  account_id,
  balance
FROM latest_rows
WHERE rn = 1
ON CONFLICT ON CONSTRAINT pk_account_withdraws
DO UPDATE SET
  delayed_vests = EXCLUDED.delayed_vests;

END
$$;

RESET ROLE;

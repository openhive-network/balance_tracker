SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION process_block_range_balances(
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
 __balance_history_by_day INT;
 __balance_history_by_month INT;
BEGIN
--RAISE NOTICE 'Processing balances';

-- get all operations that can impact balances
WITH balance_impacting_ops AS MATERIALIZED
(
  SELECT ot.id
  FROM hafd.operation_types ot
  WHERE ot.name IN (SELECT * FROM hive.get_balance_impacting_operations())
),
ops_in_range AS MATERIALIZED 
(
  SELECT 
    (SELECT av.id FROM accounts_view av WHERE av.name = get_impacted_balances.account_name) AS account_id,
    get_impacted_balances.asset_symbol_nai AS nai,
    get_impacted_balances.amount AS balance,
    ho.id AS source_op,
    ho.block_num AS source_op_block,
    ho.body_binary
  FROM operations_view ho --- APP specific view must be used, to correctly handle reversible part of the data.
  JOIN hafd.applied_hardforks ah ON ah.hardfork_num = 1
  CROSS JOIN hive.get_impacted_balances(
    ho.body_binary, 
    ho.block_num > ah.block_num
  ) AS get_impacted_balances
  WHERE 
    ho.op_type_id IN (SELECT id FROM balance_impacting_ops) AND 
    ho.block_num BETWEEN _from AND _to
),
-- prepare accounts that were impacted by the operations
group_by_account_nai AS (
  SELECT 
    cp.account_id,
    cp.nai
  FROM ops_in_range cp
  GROUP BY cp.account_id, cp.nai
),
get_latest_balance AS (
  SELECT 
    gan.account_id,
    gan.nai,
    COALESCE(cab.balance, 0) as balance,
    COALESCE(cab.balance_change_count, 0) as balance_seq_no,
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
    1 AS balance_seq_no,
    cp.source_op,
    cp.source_op_block
  FROM ops_in_range cp

  UNION ALL

-- latest stored balance is needed to replecate balance history
  SELECT 
    glb.account_id,
    glb.nai,
    glb.balance,
    glb.balance_seq_no,
    glb.source_op,
    glb.source_op_block
  FROM get_latest_balance glb
),

/*
whole table is inserted into history (without id = 0) and the newest record is used to update current balance table

 id  | balance | sum-balance
  2       10     10 + 5 + 1
  1       5        5 + 1
  0       1          1

*/

-- Combine both window function calculations in a single pass to avoid redundant sorting.
-- The row number must be calculated after the sum since operations like escrow_rejected_operation
-- can trigger multiple balance changes for the same asset, leading to multiple rows with the
-- same source_op and balance.
prepare_balance_history AS MATERIALIZED (
  SELECT
    ulb.account_id,
    ulb.nai,
    SUM(ulb.balance) OVER w_asc AS balance,
    SUM(ulb.balance_seq_no) OVER w_asc AS balance_seq_no,
    ulb.source_op,
    ulb.source_op_block,
    ROW_NUMBER() OVER w_desc AS rn
  FROM union_latest_balance_with_impacted_balances ulb
  WINDOW
    w_asc AS (PARTITION BY ulb.account_id, ulb.nai ORDER BY ulb.source_op, ulb.balance ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    w_desc AS (PARTITION BY ulb.account_id, ulb.nai ORDER BY ulb.source_op DESC, ulb.balance DESC)
),

insert_current_account_balances AS (
  INSERT INTO current_account_balances AS acc_balances
    (account, nai, balance_change_count, source_op, balance)
  SELECT 
    rd.account_id,
    rd.nai,
    rd.balance_seq_no,
    rd.source_op,
    rd.balance
  FROM prepare_balance_history rd 
  WHERE rd.rn = 1
  ON CONFLICT ON CONSTRAINT pk_current_account_balances DO
  UPDATE SET 
    balance = EXCLUDED.balance,
    balance_change_count = EXCLUDED.balance_change_count,
    source_op = EXCLUDED.source_op
  RETURNING (xmax = 0) as is_new_entry, acc_balances.account
),

remove_latest_stored_balance_record AS MATERIALIZED (
  SELECT 
    pbh.account_id,
    pbh.nai,
    pbh.balance_seq_no,
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
    (account, nai, balance_seq_no, source_op, balance)
  SELECT 
    pbh.account_id,
    pbh.nai,
    pbh.balance_seq_no,
    pbh.source_op,
    pbh.balance
  FROM remove_latest_stored_balance_record pbh
  RETURNING (xmax = 0) as is_new_entry, acc_history.account
),

-- aggregated balance history by day and month
-- Join block timestamps once and compute all aggregations in a single pass
join_created_at_to_balance_history AS MATERIALIZED (
  SELECT
    rls.account_id,
    rls.nai,
    rls.source_op,
    rls.source_op_block,
    rls.balance,
    date_trunc('day', bv.created_at) AS by_day,
    date_trunc('month', bv.created_at) AS by_month
  FROM remove_latest_stored_balance_record rls
  JOIN hive.blocks_view bv ON bv.num = rls.source_op_block
),
-- Combine all window functions and aggregations into single pass using WINDOW clause
-- This avoids multiple scans of join_created_at_to_balance_history
aggregated_balance_history AS MATERIALIZED (
  SELECT
    account_id,
    nai,
    source_op,
    balance,
    by_day,
    by_month,
    ROW_NUMBER() OVER w_day_desc AS rn_by_day,
    ROW_NUMBER() OVER w_month_desc AS rn_by_month,
    MIN(balance) OVER w_day_all AS min_balance_day,
    MAX(balance) OVER w_day_all AS max_balance_day,
    MIN(balance) OVER w_month_all AS min_balance_month,
    MAX(balance) OVER w_month_all AS max_balance_month
  FROM join_created_at_to_balance_history
  WINDOW
    w_day_desc AS (PARTITION BY account_id, nai, by_day ORDER BY source_op DESC),
    w_month_desc AS (PARTITION BY account_id, nai, by_month ORDER BY source_op DESC),
    w_day_all AS (PARTITION BY account_id, nai, by_day),
    w_month_all AS (PARTITION BY account_id, nai, by_month)
),

-- insert aggregated balance history
-- Now using the combined aggregated_balance_history CTE which has all data in one scan
insert_account_balance_history_by_day AS (
  INSERT INTO balance_history_by_day AS acc_history
    (account, nai, source_op, updated_at, balance, min_balance, max_balance)
  SELECT
    abh.account_id,
    abh.nai,
    abh.source_op,
    abh.by_day,
    abh.balance,
    abh.min_balance_day,
    abh.max_balance_day
  FROM aggregated_balance_history abh
  WHERE abh.rn_by_day = 1
  ON CONFLICT ON CONSTRAINT pk_balance_history_by_day DO
  UPDATE SET
    source_op = EXCLUDED.source_op,
    balance = EXCLUDED.balance,
    min_balance = LEAST(EXCLUDED.min_balance, acc_history.min_balance),
    max_balance = GREATEST(EXCLUDED.max_balance, acc_history.max_balance)
  RETURNING (xmax = 0) as is_new_entry, acc_history.account
),
insert_account_balance_history_by_month AS (
  INSERT INTO balance_history_by_month AS acc_history
    (account, nai, source_op, updated_at, balance, min_balance, max_balance)
  SELECT
    abh.account_id,
    abh.nai,
    abh.source_op,
    abh.by_month,
    abh.balance,
    abh.min_balance_month,
    abh.max_balance_month
  FROM aggregated_balance_history abh
  WHERE abh.rn_by_month = 1
  ON CONFLICT ON CONSTRAINT pk_balance_history_by_month DO
  UPDATE SET
    source_op = EXCLUDED.source_op,
    balance = EXCLUDED.balance,
    min_balance = LEAST(EXCLUDED.min_balance, acc_history.min_balance),
    max_balance = GREATEST(EXCLUDED.max_balance, acc_history.max_balance)
  RETURNING (xmax = 0) as is_new_entry, acc_history.account
)

SELECT
  (SELECT count(*) FROM insert_account_balance_history) as balance_history,
  (SELECT count(*) FROM insert_current_account_balances) AS current_balances,
  (SELECT count(*) FROM insert_account_balance_history_by_day) as balance_history_by_day,
  (SELECT count(*) FROM insert_account_balance_history_by_month) AS balance_history_by_month
INTO __balance_history, __current_balances, __balance_history_by_day,__balance_history_by_month;

END
$$;

RESET ROLE;

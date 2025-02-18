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
  FROM ops_in_range cp

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
whole table is inserted into history (without id = 0) and the newest record is used to update current balance table

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

RESET ROLE;

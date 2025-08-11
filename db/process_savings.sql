SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION process_block_range_savings(IN _from INT, IN _to INT, IN _report_step INT = 1000)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  __delete_transfer_requests INT;
  __insert_transfer_requests INT;
  __upsert_transfers INT;
  __savings_history INT;
  __savings_history_by_day INT;
  __savings_history_by_month INT;
BEGIN
WITH process_block_range_data_b AS  
(
  SELECT 
    ov.body,
    ov.id AS source_op,
    ov.block_num as source_op_block,
    ov.op_type_id 
  FROM operations_view ov
  WHERE 
    ov.op_type_id IN (32,33,34,59,55) AND 
    ov.block_num BETWEEN _from AND _to
),
filter_interest_ops AS 
(
  SELECT 
    (SELECT av.id FROM accounts_view av WHERE av.name = get_impacted_saving_balances.account_name) AS account_id,
    get_impacted_saving_balances.asset_symbol_nai AS nai,
    get_impacted_saving_balances.amount AS balance,
    get_impacted_saving_balances.savings_withdraw_request,
    get_impacted_saving_balances.request_id,  
    ov.source_op,
    ov.source_op_block,
    ov.op_type_id 
  FROM process_block_range_data_b ov
  CROSS JOIN btracker_backend.get_impacted_saving_balances(ov.body, ov.op_type_id) AS get_impacted_saving_balances
  WHERE 
    ov.op_type_id IN (32,33,34,59) OR
    (ov.op_type_id = 55 AND (ov.body->'value'->>'is_saved_into_hbd_balance')::BOOLEAN = false)
),
---------------------------------------------------------------------------------------
-- views for specific operation types
cancel_and_fill_transfers AS (
  SELECT  
    account_id,
    nai,
    balance,
    savings_withdraw_request,
    request_id,  
    op_type_id,
    source_op,
    source_op_block
  FROM filter_interest_ops
  WHERE op_type_id in (34,59)
),
transfers_from AS (
  SELECT  
    account_id,
    nai,
    balance,
    savings_withdraw_request,
    request_id,  
    op_type_id,
    source_op,
    source_op_block
  FROM filter_interest_ops
  WHERE op_type_id = 33
),
income_transfers AS (
  SELECT  
    account_id,
    nai,
    balance,
    savings_withdraw_request,
    request_id,  
    op_type_id,
    source_op,
    source_op_block
  FROM filter_interest_ops
  WHERE op_type_id IN (32,55)
),
---------------------------------------------------------------------------------------
-- find if transfer from savings happened in same batch of blocks as cancel or fill
join_canceled_transfers_in_query AS (
  SELECT  
    cpo.account_id,
    cpo.nai,
    cpo.balance,
    cpo.savings_withdraw_request,
    cpo.request_id,  
    cpo.op_type_id,
    cpo.source_op,
    cpo.source_op_block,
	  (SELECT tf.source_op FROM transfers_from tf WHERE tf.account_id = cpo.account_id AND tf.request_id = cpo.request_id AND tf.source_op < cpo.source_op ORDER BY tf.source_op DESC LIMIT 1) AS last_transfer_id
  FROM cancel_and_fill_transfers cpo
),
prepare_canceled_transfers_in_query AS MATERIALIZED (
  SELECT  
    jct.account_id,
    (CASE WHEN jct.op_type_id = 34 THEN tf.nai ELSE jct.nai END) AS nai,
    -- we change sign of balance for cancel operation - we take the value from transfer_from_savings operation which is negative
    -- so we need to change it to positive (we want to give it back to savings)
    (CASE WHEN jct.op_type_id = 34 THEN (- tf.balance) ELSE jct.balance END) AS balance,
    jct.savings_withdraw_request,
    jct.request_id,  
    jct.op_type_id,
    jct.source_op,
    jct.source_op_block,
	  jct.last_transfer_id,
-- if transfer wasn't found in join_canceled_transfers_in_query, it has to be removed from transfer_saving_id table
	  (CASE WHEN jct.last_transfer_id IS NULL THEN TRUE ELSE FALSE END) AS delete_transfer_id_from_table,
	  FALSE AS insert_transfer_id_to_table
  FROM join_canceled_transfers_in_query jct
  LEFT JOIN transfers_from tf ON tf.source_op = jct.last_transfer_id
),
----------------------------------------
-- 1/2 cancels (found in same batch of blocks as transfers - request_id doesn't require removing from transfer_saving_id table)
canceled_transfers_in_query AS (
  SELECT  
    account_id,
    nai,
    balance,
    savings_withdraw_request,
    request_id,  
    op_type_id,
    source_op,
    source_op_block,
	  last_transfer_id,
	  delete_transfer_id_from_table,
	  insert_transfer_id_to_table
  FROM prepare_canceled_transfers_in_query 
  WHERE NOT delete_transfer_id_from_table
),
----------------------------------------
prepare_canceled_transfers_already_inserted AS (
  SELECT  
    account_id,
    nai,
    balance,
    savings_withdraw_request,
    request_id,  
    op_type_id,
    source_op,
    source_op_block,
	  delete_transfer_id_from_table,
	  insert_transfer_id_to_table
  FROM prepare_canceled_transfers_in_query 
  WHERE delete_transfer_id_from_table
),
-- 2/2 cancels (transfer happened in diffrent batch of blocks - request_id must be removed from transfer_saving_id table)
canceled_transfers_already_inserted AS (
  SELECT  
    pct.account_id,
    (CASE WHEN pct.op_type_id = 34 THEN tsi.nai ELSE pct.nai END) AS nai,
    -- we change sign of balance for cancel operation - we take the value from transfer_saving_id table which is negative
    -- so we need to change it to positive (we want to give it back to savings)
    (CASE WHEN pct.op_type_id = 34 THEN (- tsi.balance) ELSE pct.balance END) AS balance,
    pct.savings_withdraw_request,
    pct.request_id,  
    pct.op_type_id,
    pct.source_op,
    pct.source_op_block,
    pct.delete_transfer_id_from_table,
    pct.insert_transfer_id_to_table
  FROM prepare_canceled_transfers_already_inserted pct
  JOIN transfer_saving_id tsi ON tsi.request_id = pct.request_id AND tsi.account = pct.account_id
),
----------------------------------------
---------------------------------------------------------------------------------------
transfers_from_canceled_in_query AS (
  SELECT  
    cp.account_id,
    cp.nai,
    cp.balance,
    cp.savings_withdraw_request,
    cp.request_id,  
    cp.op_type_id,
    cp.source_op,
    cp.source_op_block,
	  FALSE AS delete_transfer_id_from_table,
-- if transfer was cancelled in the same batch of block it doen't require to be inserted into transfer_saving_id table
	  (NOT EXISTS (SELECT 1 FROM canceled_transfers_in_query cti WHERE cti.last_transfer_id = cp.source_op)) AS insert_transfer_id_to_table
  FROM transfers_from cp
),
---------------------------------------------------------------------------------------
-- union all transfer operations 
union_operations AS MATERIALIZED (
-- transfers from savings
  SELECT  
    account_id,
    nai,
    balance,
    savings_withdraw_request,
    request_id,  
    op_type_id,
    source_op,
    source_op_block,
	  delete_transfer_id_from_table,
	  insert_transfer_id_to_table
  FROM transfers_from_canceled_in_query

  UNION ALL

-- transfers to savings
  SELECT  
    account_id,
    nai,
    balance,
    savings_withdraw_request,
    request_id,  
    op_type_id,
    source_op,
    source_op_block,
	  FALSE AS delete_transfer_id_from_table,
    FALSE AS insert_transfer_id_to_table
  FROM income_transfers

  UNION ALL

---------------------------------------------------------------------------------------
-- cancel / filled transfers
  SELECT  
    account_id,
    nai,
    balance,
    savings_withdraw_request,
    request_id,  
    op_type_id,
    source_op,
    source_op_block,
	  delete_transfer_id_from_table,
	  insert_transfer_id_to_table
  FROM canceled_transfers_in_query

  UNION ALL

  SELECT  
    account_id,
    nai,
    balance,
    savings_withdraw_request,
    request_id,  
    op_type_id,
    source_op,
    source_op_block,
	  delete_transfer_id_from_table,
	  insert_transfer_id_to_table
  FROM canceled_transfers_already_inserted
---------------------------------------------------------------------------------------
),
group_by_account_nai AS (
  SELECT 
    cp.account_id,
    cp.nai
  FROM union_operations cp
  GROUP BY cp.account_id, cp.nai
),
get_latest_balance AS (
  SELECT 
    gan.account_id,
    gan.nai,
    COALESCE(cab.balance, 0) as balance,
    COALESCE(cab.savings_withdraw_requests, 0) AS savings_withdraw_request,
    COALESCE(cab.balance_change_count, 0) as balance_seq_no,
    0 AS request_id,  
    0 AS op_type_id,
    0 AS source_op,
    0 AS source_op_block
  FROM group_by_account_nai gan
  LEFT JOIN account_savings cab ON cab.account = gan.account_id AND cab.nai = gan.nai
),
---------------------------------------------------------------------------------------
union_operations_with_latest_balance AS (
  SELECT  
    account_id,
    nai,
    balance,
    balance_seq_no,
    savings_withdraw_request,
    request_id,  
    op_type_id,
    source_op,
    source_op_block,
	  FALSE AS delete_transfer_id_from_table,
    FALSE AS insert_transfer_id_to_table
  FROM get_latest_balance

  UNION ALL

  SELECT  
    account_id,
    nai,
    balance,
    1 AS balance_seq_no,
    savings_withdraw_request,
    request_id,  
    op_type_id,
    source_op,
    source_op_block,
	  delete_transfer_id_from_table,
	  insert_transfer_id_to_table
  FROM union_operations
---------------------------------------------------------------------------------------
),
prepare_savings_history AS (
  SELECT 
    uo.account_id,
    uo.nai,
    SUM(uo.balance) OVER 
      (
        PARTITION BY uo.account_id, uo.nai 
        ORDER BY uo.source_op, uo.balance 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS balance,
    SUM(uo.savings_withdraw_request) OVER 
      ( 
        PARTITION BY uo.account_id, uo.nai 
        ORDER BY uo.source_op, uo.balance 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS savings_withdraw_request,
    SUM(uo.balance_seq_no) OVER 
      (
        PARTITION BY uo.account_id, uo.nai 
        ORDER BY uo.source_op, uo.balance 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS balance_seq_no,
    uo.source_op,
    uo.source_op_block,
    ROW_NUMBER() OVER (PARTITION BY uo.account_id, uo.nai ORDER BY uo.source_op DESC, uo.balance DESC) AS rn
  FROM union_operations_with_latest_balance uo
),
---------------------------------------------------------------------------------------
delete_all_canceled_or_filled_transfers AS (
  DELETE FROM transfer_saving_id tsi
  USING union_operations_with_latest_balance uo
  WHERE 
    tsi.request_id = uo.request_id AND 
    tsi.account = uo.account_id AND
    uo.delete_transfer_id_from_table
  RETURNING tsi.account AS cleaned_account_id
),
insert_all_new_registered_transfers_from_savings AS (
  INSERT INTO transfer_saving_id AS tsi
    (account, nai, balance, request_id) 
  SELECT
    uo.account_id,
    uo.nai,
    uo.balance,
    uo.request_id
  FROM union_operations_with_latest_balance uo
  WHERE uo.insert_transfer_id_to_table
  RETURNING tsi.account AS new_transfer
),
remove_latest_stored_balance_record AS MATERIALIZED (
  SELECT 
    pbh.account_id,
    pbh.nai,
    pbh.balance_seq_no,
    pbh.balance,
    pbh.source_op,
    pbh.source_op_block,
    pbh.savings_withdraw_request,
    pbh.rn
  FROM prepare_savings_history pbh
  -- remove with prepared previous balance
  WHERE pbh.source_op > 0
  ORDER BY pbh.source_op
),
insert_sum_of_transfers AS (
  INSERT INTO account_savings AS acc_history
    (account, nai, balance_change_count, balance, source_op, savings_withdraw_requests)
  SELECT
    ps.account_id,
    ps.nai,
    ps.balance_seq_no,
    ps.balance,
    ps.source_op,
    ps.savings_withdraw_request
  FROM remove_latest_stored_balance_record ps
  WHERE rn = 1
  ON CONFLICT ON CONSTRAINT pk_account_savings
  DO UPDATE SET
      balance = EXCLUDED.balance,
      balance_change_count = EXCLUDED.balance_change_count,
      source_op = EXCLUDED.source_op,
      savings_withdraw_requests = EXCLUDED.savings_withdraw_requests
  RETURNING acc_history.account
),
insert_saving_balance_history AS (
  INSERT INTO account_savings_history AS acc_history
    (account, nai, balance_seq_no, source_op, balance)
  SELECT 
    pbh.account_id,
    pbh.nai,
    pbh.balance_seq_no,
    pbh.source_op,
    pbh.balance
  FROM remove_latest_stored_balance_record pbh
  RETURNING acc_history.account
),
join_created_at_to_balance_history AS (
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
get_latest_updates AS (
  SELECT 
    account_id,
    nai,
    source_op,
    source_op_block,
    balance,
    by_day,
    by_month,
    ROW_NUMBER() OVER (PARTITION BY account_id, nai, by_day ORDER BY source_op DESC) AS rn_by_day,
    ROW_NUMBER() OVER (PARTITION BY account_id, nai, by_month ORDER BY source_op DESC) AS rn_by_month
  FROM join_created_at_to_balance_history 
),
get_min_max_balances_by_day AS (
  SELECT 
    account_id,
    nai,
    by_day,
    MAX(balance) AS max_balance,
    MIN(balance) AS min_balance
  FROM join_created_at_to_balance_history
  GROUP BY account_id, nai, by_day
),
get_min_max_balances_by_month AS (
  SELECT 
    account_id,
    nai,
    by_month,
    MAX(balance) AS max_balance,
    MIN(balance) AS min_balance
  FROM join_created_at_to_balance_history
  GROUP BY account_id, nai, by_month
),
insert_saving_balance_history_by_day AS (
  INSERT INTO saving_history_by_day AS acc_history
    (account, nai, source_op, updated_at, balance, min_balance, max_balance)
  SELECT 
    gl.account_id,
    gl.nai,
    gl.source_op,
    gl.by_day,
    gl.balance,
    gm.min_balance,
    gm.max_balance
  FROM get_latest_updates gl
  JOIN get_min_max_balances_by_day gm ON 
    gm.account_id = gl.account_id AND 
    gm.nai = gl.nai AND 
    gm.by_day = gl.by_day
  WHERE gl.rn_by_day = 1
  ON CONFLICT ON CONSTRAINT pk_saving_history_by_day DO 
  UPDATE SET 
    source_op = EXCLUDED.source_op,
    balance = EXCLUDED.balance,
    min_balance = LEAST(EXCLUDED.min_balance, acc_history.min_balance),
    max_balance = GREATEST(EXCLUDED.max_balance, acc_history.max_balance)
  RETURNING (xmax = 0) as is_new_entry, acc_history.account
),
insert_saving_balance_history_by_month AS (
  INSERT INTO saving_history_by_month AS acc_history
    (account, nai, source_op, updated_at, balance, min_balance, max_balance)
  SELECT 
    gl.account_id,
    gl.nai,
    gl.source_op,
    gl.by_month,
    gl.balance,
    gm.min_balance,
    gm.max_balance
  FROM get_latest_updates gl
  JOIN get_min_max_balances_by_month gm ON 
    gm.account_id = gl.account_id AND 
    gm.nai = gl.nai AND 
    gm.by_month = gl.by_month
  WHERE rn_by_month = 1
  ON CONFLICT ON CONSTRAINT pk_saving_history_by_month DO 
  UPDATE SET
    source_op = EXCLUDED.source_op,
    balance = EXCLUDED.balance,
    min_balance = LEAST(EXCLUDED.min_balance, acc_history.min_balance),
    max_balance = GREATEST(EXCLUDED.max_balance, acc_history.max_balance)
  RETURNING (xmax = 0) as is_new_entry, acc_history.account
)

SELECT
  (SELECT count(*) FROM delete_all_canceled_or_filled_transfers) AS delete_transfer_requests,
  (SELECT count(*) FROM insert_all_new_registered_transfers_from_savings) AS insert_transfer_requests,
  (SELECT count(*) FROM insert_sum_of_transfers) AS upsert_transfers,
  (SELECT count(*) FROM insert_saving_balance_history) as savings_history,
  (SELECT count(*) FROM insert_saving_balance_history_by_day) as savings_history_by_day,
  (SELECT count(*) FROM insert_saving_balance_history_by_month) AS savings_history_by_month
INTO __delete_transfer_requests, __insert_transfer_requests, __upsert_transfers, 
  __savings_history, __savings_history_by_day, __savings_history_by_month;

END
$$;

RESET ROLE;

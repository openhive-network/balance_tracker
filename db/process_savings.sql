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
union_operations AS (

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
sum_all_transfers AS (
  SELECT 
    uo.account_id,
    uo.nai,
    SUM(balance) AS balance,
    SUM(savings_withdraw_request) AS savings_withdraw_request,
    MAX(source_op) AS source_op,
    MAX(source_op_block) AS source_op_block
  FROM union_operations uo
  GROUP BY uo.account_id, uo.nai
),
---------------------------------------------------------------------------------------
delete_all_canceled_or_filled_transfers AS (
  DELETE FROM transfer_saving_id tsi
  USING union_operations uo
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
  FROM union_operations uo
  WHERE uo.insert_transfer_id_to_table
  RETURNING tsi.account AS new_transfer
),
insert_sum_of_transfers AS (
  INSERT INTO account_savings
    (account, nai, saving_balance, source_op, source_op_block, savings_withdraw_requests)
  SELECT
    sat.account_id,
    sat.nai,
    sat.balance,
    sat.source_op,
    sat.source_op_block,
    sat.savings_withdraw_request
  FROM sum_all_transfers sat
  ON CONFLICT ON CONSTRAINT pk_account_savings
  DO UPDATE SET
      saving_balance = account_savings.saving_balance + EXCLUDED.saving_balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block,
      savings_withdraw_requests = account_savings.savings_withdraw_requests + EXCLUDED.savings_withdraw_requests
  RETURNING account AS new_updated_acccounts
)

SELECT
  (SELECT count(*) FROM delete_all_canceled_or_filled_transfers) AS delete_transfer_requests,
  (SELECT count(*) FROM insert_all_new_registered_transfers_from_savings) AS insert_transfer_requests,
  (SELECT count(*) FROM insert_sum_of_transfers) AS upsert_transfers
INTO __delete_transfer_requests, __insert_transfer_requests, __upsert_transfers;

END
$$;

RESET ROLE;

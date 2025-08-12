SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION process_block_range_delegations(IN _from INT, IN _to INT, IN _report_step INT = 1000)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  __insert_current_delegations INT;
  __delete_canceled_delegations INT;
  __insert_delegations INT;
BEGIN
WITH process_block_range_data_b AS MATERIALIZED
(
  SELECT 
    (SELECT av.id FROM accounts_view av WHERE av.name = get_impacted_delegation_balances.delegator) AS delegator,
    (SELECT av.id FROM accounts_view av WHERE av.name = get_impacted_delegation_balances.delegatee) AS delegatee,
    get_impacted_delegation_balances.amount AS balance,
    ov.id AS source_op,
    ov.block_num as source_op_block,
    ov.op_type_id 
  FROM operations_view ov
  CROSS JOIN btracker_backend.get_impacted_delegation_balances(ov.body, ov.op_type_id) AS get_impacted_delegation_balances
  WHERE 
    ov.op_type_id IN (40,41,62,68) AND 
    ov.block_num BETWEEN _from AND _to
),
---------------------------------------------------------------------------------------
-- prepare hf23 data
-- contains all delegation RESETS that were made by hf23 (already in the db)
join_prev_balance_to_hf23_accounts AS MATERIALIZED (
  SELECT 
    prev.delegator,
    prev.delegatee,
    cpfd.balance, -- reset all delegations for accounts that were impacted by hf23 (balance is 0)
    cpfd.source_op,
    cpfd.source_op_block
  FROM current_accounts_delegations prev
  JOIN process_block_range_data_b cpfd ON prev.delegator = cpfd.delegator AND cpfd.op_type_id = 68
),

-- contains all delegation RESETS that were made by hf23 (in query)
-- we need to simulate reset like in above CTE
find_delegations_of_hf23_account_in_query AS (
  SELECT 
    cp.delegator,
    cp.delegatee,
    0 AS balance,
    MAX(cpfd.source_op) as source_op,
    MAX(cpfd.source_op_block) as source_op_block
  FROM process_block_range_data_b cp
  JOIN process_block_range_data_b cpfd ON cp.delegator = cpfd.delegator AND cpfd.op_type_id = 68 AND cp.source_op < cpfd.source_op
  WHERE 
    cp.op_type_id IN (40,41) AND
  --exclude already reset delegations
    NOT EXISTS (SELECT 1 FROM join_prev_balance_to_hf23_accounts jpb WHERE jpb.delegator = cp.delegator AND jpb.delegatee = cp.delegatee)
  GROUP BY cp.delegator, cp.delegatee
),
hf23_resets AS (
-- reset delegations for accounts that were impacted by hf23 (already in the db)
  SELECT 
    delegator,
    delegatee,
    balance,
    source_op,
    source_op_block
  FROM join_prev_balance_to_hf23_accounts

  UNION ALL

-- reset delegations for accounts that were impacted by hf23 (in query)
  SELECT 
    delegator,
    delegatee,
    balance,
    source_op,
    source_op_block
  FROM find_delegations_of_hf23_account_in_query
),
---------------------------------------------------------------------------------------
--list of delegations in block range
delegations_in_query AS MATERIALIZED (
-- delegations made in query
  SELECT 
    delegator,
    delegatee,
    balance,
    source_op,
    source_op_block
  FROM process_block_range_data_b 
  WHERE op_type_id IN (40,41)

  UNION ALL

-- hf23 delegation resets
  SELECT 
    delegator,
    delegatee,
    balance,
    source_op,
    source_op_block
  FROM hf23_resets
),
---------------------------------------------------------------------------------------
-- add prev delegation
group_by_delegator_delegatee AS MATERIALIZED (
  SELECT 
    delegator,
    delegatee,
    MAX(source_op) as source_op
  FROM delegations_in_query 
  GROUP BY delegator, delegatee
),
get_prev_delegation AS (
  SELECT 
    gb.delegator,
    gb.delegatee,
    COALESCE(cad.balance, 0) as balance,
    0 AS source_op,
    0 AS source_op_block
  FROM group_by_delegator_delegatee gb
  LEFT JOIN current_accounts_delegations cad ON cad.delegator = gb.delegator AND cad.delegatee = gb.delegatee
),
add_prev_delegation AS (
  SELECT 
    delegator, 
    delegatee,
    balance,
    source_op,
    source_op_block
  FROM delegations_in_query

  UNION ALL

  SELECT 
    delegator,
    delegatee,
    balance,
    source_op,
    source_op_block
  FROM get_prev_delegation 
),

/*
 id  | balance | balance_delta
  2       10       10 - 5
  1       5        5 - 1
  0       1          1
*/

delegation_delta AS MATERIALIZED (
  SELECT 
    delegator,
    delegatee,
    balance,
    balance - LAG(balance, 1, 0) OVER (PARTITION BY delegator, delegatee ORDER BY source_op) AS balance_delta,
    source_op,
    source_op_block
  FROM add_prev_delegation 
),
---------------------------------------------------------------------------------------
-- sum received and delegated vests
union_delegations AS (
  SELECT 
    dd.delegator AS account_id,
    0 AS received_vests,
    -- for every account found in hf23 - we need to lower its delegation by the amount that was delegated before hf23 (normally there is return operation for returned delegation)
    (
      CASE 
        WHEN NOT EXISTS (SELECT 1 FROM process_block_range_data_b gl WHERE gl.delegator = dd.delegator AND gl.op_type_id = 68 AND gl.source_op = dd.source_op) THEN
          GREATEST(dd.balance_delta, 0)
        ELSE
          dd.balance_delta
        END
    ) AS delegated_vests
  FROM delegation_delta dd
  WHERE dd.source_op > 0

  UNION ALL 

  SELECT 
    delegatee AS account_id,
    balance_delta AS received_vests,
    0
  FROM delegation_delta
  WHERE source_op > 0

  UNION ALL

  SELECT 
    delegator,
    0 AS received_vests,
    balance
  FROM process_block_range_data_b
  WHERE op_type_id = 62
),
sum_delegations AS (
  SELECT 
    ud.account_id,
    SUM(ud.received_vests) AS received_vests,
    SUM(ud.delegated_vests) AS delegated_vests
  FROM union_delegations ud
  GROUP BY ud.account_id
),
prepare_newest_delegation_pairs AS MATERIALIZED (
  SELECT 
    dd.delegator,
    dd.delegatee,
    dd.balance,
    dd.source_op,
    dd.source_op_block
  FROM delegation_delta dd 
  JOIN group_by_delegator_delegatee gb ON gb.delegator = dd.delegator AND gb.delegatee = dd.delegatee AND gb.source_op = dd.source_op
),
---------------------------------------------------------------------------------------
insert_current_delegations AS (
  INSERT INTO current_accounts_delegations AS cab
    (delegator, delegatee, balance, source_op)
  SELECT 
    delegator,
    delegatee,
    balance,
    source_op
  FROM prepare_newest_delegation_pairs
  WHERE balance > 0
  ON CONFLICT ON CONSTRAINT pk_current_accounts_delegations
  DO UPDATE SET
      balance = EXCLUDED.balance,
      source_op = EXCLUDED.source_op
  RETURNING cab.delegator AS delegator
),
---------------------------------------------------------------------------------------
-- delete canceled delegations
delete_canceled_delegations AS (
  DELETE FROM current_accounts_delegations cad
  USING prepare_newest_delegation_pairs pn
  WHERE 
    cad.delegator = pn.delegator AND 
    cad.delegatee = pn.delegatee AND pn.balance = 0
  RETURNING cad.delegator AS delegator
),
---------------------------------------------------------------------------------------
-- insert new delegations
insert_delegations AS (
  INSERT INTO account_delegations
    (account, received_vests, delegated_vests) 
  SELECT 
    sd.account_id,
    sd.received_vests,
    sd.delegated_vests
  FROM sum_delegations sd
  ON CONFLICT ON CONSTRAINT pk_temp_vests
  DO UPDATE SET
      received_vests = account_delegations.received_vests + EXCLUDED.received_vests,
      delegated_vests = account_delegations.delegated_vests + EXCLUDED.delegated_vests
  RETURNING account AS updated_account
)

SELECT
  (SELECT count(*) FROM insert_current_delegations) AS insert_current_delegations,
  (SELECT count(*) FROM delete_canceled_delegations) AS delete_canceled_delegations,
  (SELECT count(*) FROM insert_delegations) AS insert_delegations
INTO __insert_current_delegations, __delete_canceled_delegations, __insert_delegations;

END
$$;

RESET ROLE;

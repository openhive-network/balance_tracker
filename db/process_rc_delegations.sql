SET ROLE btracker_owner;

/**
 * process_block_range_rc_delegations - Process RC delegation operations for a block range
 *
 * OPERATIONS PROCESSED:
 *   - custom_json_operation with id='rc' containing delegate_rc_operation
 *
 * HARDFORK REQUIREMENT:
 *   RC delegations were formalized in HF26 (block ~68,676,505).
 *   This function skips processing for blocks before HF26.
 *
 * RC delegations are transmitted via custom_json_operation with:
 *   - id: "rc"
 *   - json: contains delegate_rc_operation structure
 *
 * The C++ parser function hive.parse_rc_delegation() extracts:
 *   - from_account TEXT: delegator account name
 *   - to_account TEXT: delegatee account name
 *   - max_rc BIGINT: amount of RC delegated (0 means remove delegation)
 *
 * This function follows the same "squashing" pattern as process_delegations.sql:
 *   1. Fetch all custom_json operations with id='rc' in block range
 *   2. Parse using hive.parse_rc_delegation()
 *   3. Resolve account names to IDs
 *   4. Calculate deltas using window functions
 *   5. UPSERT final state / DELETE zero delegations
 *   6. Update summary table
 */
CREATE OR REPLACE FUNCTION process_block_range_rc_delegations(IN _from INT, IN _to INT, IN _report_step INT = 1000)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  -- Operation type ID (looked up dynamically from hafd.operation_types)
  _op_custom_json INT := btracker_backend.op_custom_json();
  -- HF26 block (RC delegations were formalized in HF26)
  __hardfork_26_block INT;
  -- Counters
  __insert_current_rc_delegations INT;
  __delete_canceled_rc_delegations INT;
  __insert_rc_delegations INT;
BEGIN
------------------------------------------------------------------------------
-- HF26 CHECK: RC delegations were formalized in HF26
--
-- Skip processing if block range is entirely before HF26.
-- If range spans HF26, adjust _from to start at HF26 block.
------------------------------------------------------------------------------
SELECT block_num INTO __hardfork_26_block
FROM hafd.applied_hardforks
WHERE hardfork_num = btracker_backend.hf_rc_delegations();

-- If HF26 hasn't been applied yet (e.g., during early sync), skip entirely
IF __hardfork_26_block IS NULL OR _to < __hardfork_26_block THEN
  RETURN;
END IF;

-- Adjust _from if range starts before HF26
IF _from < __hardfork_26_block THEN
  _from := __hardfork_26_block;
END IF;
------------------------------------------------------------------------------
-- STEP 1: Fetch all custom_json operations with id='rc' in the block range
--
-- We filter on op_type_id for custom_json_operation and then check if the
-- operation body has id='rc' to identify RC delegation operations.
------------------------------------------------------------------------------
WITH ops AS MATERIALIZED (
  SELECT ov.body_value AS body, ov.id, ov.block_num
  FROM _btracker_ops_batch ov
  WHERE
    ov.op_type_id = _op_custom_json AND
    ov.block_num BETWEEN _from AND _to AND
    (ov.body_value->>'id') = 'rc'
),
------------------------------------------------------------------------------
-- STEP 2: Parse RC delegation operations using C++ parser
--
-- The hive.parse_rc_delegation function parses the JSON body and returns:
--   - from_account: delegator name
--   - to_account: delegatee name (can be multiple, parser returns one row per delegatee)
--   - max_rc: RC amount delegated
--
-- We filter out NULL results (invalid RC operations that the parser rejects).
------------------------------------------------------------------------------
parsed_rc_delegations AS MATERIALIZED (
  SELECT
    parsed.from_account,
    parsed.to_account,
    parsed.max_rc,
    o.id AS source_op,
    o.block_num AS source_op_block
  FROM ops o
  CROSS JOIN LATERAL hive.parse_rc_delegation((o.body->>'json')::text) AS parsed
  WHERE parsed.from_account IS NOT NULL
),
------------------------------------------------------------------------------
-- STEP 3: Resolve account names to IDs
--
-- Collect all unique account names and look up their IDs in hive.accounts_view.
-- This is done once for efficiency rather than per-row subqueries.
------------------------------------------------------------------------------
all_account_names AS (
  SELECT DISTINCT from_account AS account_name FROM parsed_rc_delegations
  UNION
  SELECT DISTINCT to_account AS account_name FROM parsed_rc_delegations
),
account_ids AS MATERIALIZED (
  SELECT av.name AS account_name, av.id AS account_id
  FROM hive.accounts_view av
  WHERE av.name IN (SELECT account_name FROM all_account_names)
),
------------------------------------------------------------------------------
-- STEP 4: Join parsed data with account IDs
------------------------------------------------------------------------------
rc_delegations_with_ids AS MATERIALIZED (
  SELECT
    delegator.account_id AS delegator,
    delegatee.account_id AS delegatee,
    p.max_rc,
    p.source_op,
    p.source_op_block
  FROM parsed_rc_delegations p
  JOIN account_ids delegator ON delegator.account_name = p.from_account
  JOIN account_ids delegatee ON delegatee.account_name = p.to_account
),
------------------------------------------------------------------------------
-- STEP 5: Get previous RC delegation balances for delta calculation
--
-- THE SQUASHING PATTERN:
-- We process many operations but only store the FINAL STATE.
-- To calculate correct deltas, we need the STARTING balance.
------------------------------------------------------------------------------
group_by_delegator_delegatee AS MATERIALIZED (
  SELECT
    delegator,
    delegatee,
    MAX(source_op) as source_op
  FROM rc_delegations_with_ids
  GROUP BY delegator, delegatee
),
------------------------------------------------------------------------------
-- Fetch previous RC delegation balance from current_rc_delegations table.
-- If no previous delegation exists (new delegation pair), default to 0.
-- source_op=0 marks this as a "synthetic" previous record.
------------------------------------------------------------------------------
get_prev_rc_delegation AS (
  SELECT
    gb.delegator,
    gb.delegatee,
    COALESCE(crd.max_rc, 0) as max_rc,
    0 AS source_op,
    0 AS source_op_block
  FROM group_by_delegator_delegatee gb
  LEFT JOIN current_rc_delegations crd ON crd.delegator = gb.delegator AND crd.delegatee = gb.delegatee
),
------------------------------------------------------------------------------
-- Combine current operations with previous balances.
-- This creates a complete timeline for each (delegator, delegatee) pair.
------------------------------------------------------------------------------
add_prev_rc_delegation AS (
  SELECT
    delegator,
    delegatee,
    max_rc,
    source_op,
    source_op_block
  FROM rc_delegations_with_ids

  UNION ALL

  SELECT
    delegator,
    delegatee,
    max_rc,
    source_op,
    source_op_block
  FROM get_prev_rc_delegation
),
------------------------------------------------------------------------------
-- STEP 6: Calculate deltas using window functions
--
-- LAG(max_rc, 1, 0): Get previous row's max_rc (default 0 for first row)
-- rc_delta = current_max_rc - previous_max_rc
--
-- LAST OPERATION WINS PATTERN:
-- ROW_NUMBER() OVER (ORDER BY source_op DESC) assigns rn=1 to the LATEST operation.
-- We only persist the final state (rn=1) to current_rc_delegations.
------------------------------------------------------------------------------
rc_delegation_delta AS MATERIALIZED (
  SELECT
    delegator,
    delegatee,
    max_rc,
    max_rc - LAG(max_rc, 1, 0) OVER w_asc AS rc_delta,
    source_op,
    source_op_block,
    ROW_NUMBER() OVER w_desc AS rn  -- rn=1 is the LATEST operation
  FROM add_prev_rc_delegation
  WINDOW
    w_asc AS (PARTITION BY delegator, delegatee ORDER BY source_op),
    w_desc AS (PARTITION BY delegator, delegatee ORDER BY source_op DESC)
),
------------------------------------------------------------------------------
-- STEP 7: Calculate aggregated RC changes per account
--
-- When alice delegates 1000 RC to bob:
--   - Alice's delegated_rc goes up by 1000
--   - Bob's received_rc goes up by 1000
--
-- The account_rc_delegations table stores CUMULATIVE totals.
------------------------------------------------------------------------------
union_rc_delegations AS (
  -- Delegator side (RC going OUT)
  SELECT
    dd.delegator AS account_id,
    0 AS received_rc,
    dd.rc_delta AS delegated_rc
  FROM rc_delegation_delta dd
  WHERE dd.source_op > 0  -- Exclude synthetic previous record

  UNION ALL

  -- Delegatee side (RC coming IN)
  SELECT
    delegatee AS account_id,
    rc_delta AS received_rc,
    0
  FROM rc_delegation_delta
  WHERE source_op > 0  -- Exclude synthetic previous record
),
------------------------------------------------------------------------------
-- Aggregate all RC delegation changes per account
------------------------------------------------------------------------------
sum_rc_delegations AS (
  SELECT
    ud.account_id,
    SUM(ud.received_rc) AS received_rc,
    SUM(ud.delegated_rc) AS delegated_rc
  FROM union_rc_delegations ud
  GROUP BY ud.account_id
),
------------------------------------------------------------------------------
-- STEP 8: Extract FINAL RC delegation state for each (delegator, delegatee) pair
--
-- SQUASHING RESULT: From all the operations processed, we only need the
-- LATEST state (rn=1) to store in current_rc_delegations.
------------------------------------------------------------------------------
prepare_newest_rc_delegation_pairs AS MATERIALIZED (
  SELECT
    dd.delegator,
    dd.delegatee,
    dd.max_rc,
    dd.source_op,
    dd.source_op_block
  FROM rc_delegation_delta dd
  WHERE dd.rn = 1 AND dd.source_op > 0
),
------------------------------------------------------------------------------
-- STEP 9: Persist final RC delegation states (UPSERT active delegations)
--
-- Only INSERT/UPDATE where max_rc > 0 (active delegations).
-- Delegations with max_rc = 0 are handled in delete_canceled_rc_delegations.
------------------------------------------------------------------------------
insert_current_rc_delegations AS (
  INSERT INTO current_rc_delegations AS crd
    (delegator, delegatee, max_rc, source_op)
  SELECT
    delegator,
    delegatee,
    max_rc,
    source_op
  FROM prepare_newest_rc_delegation_pairs
  WHERE max_rc > 0  -- Only active delegations
  ON CONFLICT ON CONSTRAINT pk_current_rc_delegations
  DO UPDATE SET
      max_rc = EXCLUDED.max_rc,
      source_op = EXCLUDED.source_op
  RETURNING crd.delegator AS delegator
),
------------------------------------------------------------------------------
-- STEP 10: Delete canceled RC delegations (max_rc = 0)
--
-- When an RC delegation is removed (max_rc set to 0), we DELETE the row from
-- current_rc_delegations rather than keeping a zero-balance record.
------------------------------------------------------------------------------
delete_canceled_rc_delegations AS (
  DELETE FROM current_rc_delegations crd
  USING prepare_newest_rc_delegation_pairs pn
  WHERE
    crd.delegator = pn.delegator AND
    crd.delegatee = pn.delegatee AND pn.max_rc = 0
  RETURNING crd.delegator AS delegator
),
------------------------------------------------------------------------------
-- STEP 11: Update account_rc_delegations summary table (UPSERT)
--
-- account_rc_delegations stores CUMULATIVE totals per account:
--   - received_rc: Total RC received from all delegators
--   - delegated_rc: Total RC delegated out to all delegatees
------------------------------------------------------------------------------
insert_rc_delegations AS (
  INSERT INTO account_rc_delegations
    (account, received_rc, delegated_rc)
  SELECT
    sd.account_id,
    sd.received_rc,
    sd.delegated_rc
  FROM sum_rc_delegations sd
  ON CONFLICT ON CONSTRAINT pk_account_rc_delegations
  DO UPDATE SET
      received_rc = account_rc_delegations.received_rc + EXCLUDED.received_rc,
      delegated_rc = account_rc_delegations.delegated_rc + EXCLUDED.delegated_rc
  RETURNING account AS updated_account
)

------------------------------------------------------------------------------
-- STEP 12: Return counts for logging/debugging
--
-- Execute all CTEs and capture counts from data-modifying CTEs.
------------------------------------------------------------------------------
SELECT
  (SELECT count(*) FROM insert_current_rc_delegations) AS insert_current_rc_delegations,
  (SELECT count(*) FROM delete_canceled_rc_delegations) AS delete_canceled_rc_delegations,
  (SELECT count(*) FROM insert_rc_delegations) AS insert_rc_delegations
INTO __insert_current_rc_delegations, __delete_canceled_rc_delegations, __insert_rc_delegations;

END
$$;

RESET ROLE;

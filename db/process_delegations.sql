SET ROLE btracker_owner;

/**
 * process_block_range_delegations - Process delegation operations for a block range
 *
 * OPERATIONS PROCESSED:
 *   - delegate_vesting_shares_operation: Create/update/remove delegation between accounts
 *   - account_create_with_delegation_operation: Account creation with initial delegation
 *   - return_vesting_delegation_operation: Return vests after delegation period expires
 *   - hardfork_hive_operation (HF23): Reset all delegations for affected accounts
 */
CREATE OR REPLACE FUNCTION process_block_range_delegations(IN _from INT, IN _to INT, IN _report_step INT = 1000)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  -- Operation type IDs (looked up dynamically from hafd.operation_types)
  _op_delegate_vesting_shares        INT := btracker_backend.op_delegate_vesting_shares();
  _op_account_create_with_delegation INT := btracker_backend.op_account_create_with_delegation();
  _op_return_vesting_delegation      INT := btracker_backend.op_return_vesting_delegation();
  _op_hardfork_hive                  INT := btracker_backend.op_hardfork_hive();
  -- Counters
  __insert_current_delegations       INT;
  __delete_canceled_delegations      INT;
  __insert_delegations               INT;
BEGIN
------------------------------------------------------------------------------
-- STEP 1: Fetch all delegation-related operations in the block range
--
-- MATERIALIZED forces PostgreSQL to execute this CTE first and store results
-- in a temporary structure. This is critical because:
--   1. The operations_view may be expensive to query multiple times
--   2. We reference these results in multiple downstream CTEs
--   3. Without MATERIALIZED, the planner might inline this subquery repeatedly
------------------------------------------------------------------------------
WITH ops AS MATERIALIZED (
  SELECT ov.body, ov.op_type_id, ov.id, ov.block_num
  FROM operations_view ov
  WHERE
    ov.op_type_id IN (_op_delegate_vesting_shares, _op_account_create_with_delegation, _op_return_vesting_delegation, _op_hardfork_hive) AND
    ov.block_num BETWEEN _from AND _to
),
------------------------------------------------------------------------------
-- STEP 2: Parse operation bodies into structured delegation data
--
-- CROSS JOIN LATERAL pattern: For each operation row, call exactly one helper
-- function based on op_type_id. The CASE expression selects the correct parser:
--
--   - delegate_vesting_shares: Returns (delegator, delegatee, new_amount)
--     Amount=0 means delegation removal, amount>0 means create/update
--
--   - account_create_with_delegation: Returns (creator, new_account, amount)
--     When creating account with initial delegation
--
--   - return_vesting_delegation: Returns (account, NULL, -amount)
--     Virtual operation emitted when delegation period expires; vests return
--     to the original delegator. Negative amount represents vests returning.
--
--   - hardfork_hive (HF23): Returns (affected_account, NULL, 0)
--     HF23 reset ALL delegations for certain accounts. The 0 balance is a
--     marker; actual reset logic is handled in subsequent CTEs.
--
-- MATERIALIZED ensures this expensive parsing happens exactly once.
------------------------------------------------------------------------------
process_block_range_data_b AS MATERIALIZED
(
  SELECT
    (SELECT av.id FROM accounts_view av WHERE av.name = result.delegator) AS delegator,
    (SELECT av.id FROM accounts_view av WHERE av.name = result.delegatee) AS delegatee,
    result.amount AS balance,
    o.id AS source_op,
    o.block_num as source_op_block,
    o.op_type_id
  FROM ops o
  CROSS JOIN LATERAL (
    SELECT (
      CASE
        WHEN o.op_type_id = _op_delegate_vesting_shares
          THEN btracker_backend.process_delegate_vesting_shares_operation(o.body)
        WHEN o.op_type_id = _op_account_create_with_delegation
          THEN btracker_backend.process_account_create_with_delegation_operation(o.body)
        WHEN o.op_type_id = _op_return_vesting_delegation
          THEN btracker_backend.process_return_vesting_shares_operation(o.body)
        WHEN o.op_type_id = _op_hardfork_hive
          THEN btracker_backend.process_hf23_acccounts(o.body)
      END
    ).*
  ) AS result
),
---------------------------------------------------------------------------------------
-- HARDFORK 23 HANDLING
--
-- HF23 (Hive hardfork) was a special event that RESET ALL DELEGATIONS for affected
-- accounts. This is different from normal delegation operations because:
--   1. A single hardfork_hive_operation affects ALL delegations from that account
--   2. There is no explicit "remove delegation" operation for each delegatee
--   3. We must synthesize delegation resets for every existing delegation pair
--
-- We handle this in two parts:
--   A. Delegations that exist in the DB (from previous block ranges)
--   B. Delegations created earlier in THIS query's block range
---------------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- STEP 3A: Find HF23 resets for delegations already stored in database
--
-- For each account affected by HF23, look up ALL their current delegations
-- and create reset records (balance=0) for each delegatee.
--
-- Example: If alice has delegations to bob and carol in the database, and
-- HF23 hits alice, we generate two reset records:
--   (alice, bob, 0, hf23_op_id)
--   (alice, carol, 0, hf23_op_id)
------------------------------------------------------------------------------
join_prev_balance_to_hf23_accounts AS MATERIALIZED (
  SELECT
    prev.delegator,
    prev.delegatee,
    cpfd.balance, -- This is 0 from process_hf23_acccounts, resetting the delegation
    cpfd.source_op,
    cpfd.source_op_block
  FROM current_accounts_delegations prev
  JOIN process_block_range_data_b cpfd ON prev.delegator = cpfd.delegator AND cpfd.op_type_id = _op_hardfork_hive
),

------------------------------------------------------------------------------
-- STEP 3B: Find HF23 resets for delegations created IN THIS QUERY
--
-- Edge case: What if alice delegates to dave at block 100, then HF23 hits
-- alice at block 150, all within the same block range we're processing?
--
-- The delegation to dave doesn't exist in current_accounts_delegations yet,
-- so join_prev_balance_to_hf23_accounts won't find it. We must scan
-- process_block_range_data_b to find delegations created BEFORE the HF23 op.
--
-- Conditions:
--   - cp.source_op < cpfd.source_op: The delegation was created before HF23
--   - NOT EXISTS check: Don't duplicate resets already found in 3A
--
-- We GROUP BY to handle cases where alice delegates to dave multiple times
-- before HF23 - we only need one reset record.
------------------------------------------------------------------------------
find_delegations_of_hf23_account_in_query AS (
  SELECT
    cp.delegator,
    cp.delegatee,
    0 AS balance,  -- Force to zero: HF23 resets ALL delegations
    MAX(cpfd.source_op) as source_op,
    MAX(cpfd.source_op_block) as source_op_block
  FROM process_block_range_data_b cp
  JOIN process_block_range_data_b cpfd ON cp.delegator = cpfd.delegator AND cpfd.op_type_id = _op_hardfork_hive AND cp.source_op < cpfd.source_op
  WHERE
    cp.op_type_id IN (_op_delegate_vesting_shares, _op_account_create_with_delegation) AND
  -- Exclude delegations already handled in join_prev_balance_to_hf23_accounts
    NOT EXISTS (SELECT 1 FROM join_prev_balance_to_hf23_accounts jpb WHERE jpb.delegator = cp.delegator AND jpb.delegatee = cp.delegatee)
  GROUP BY cp.delegator, cp.delegatee
),
------------------------------------------------------------------------------
-- STEP 3C: Combine all HF23 reset records from both sources
------------------------------------------------------------------------------
hf23_resets AS (
-- Reset delegations for accounts that were impacted by HF23 (already in the db)
  SELECT
    delegator,
    delegatee,
    balance,
    source_op,
    source_op_block
  FROM join_prev_balance_to_hf23_accounts

  UNION ALL

-- Reset delegations for accounts that were impacted by HF23 (in query)
  SELECT
    delegator,
    delegatee,
    balance,
    source_op,
    source_op_block
  FROM find_delegations_of_hf23_account_in_query
),
---------------------------------------------------------------------------------------
-- STEP 4: Combine all delegation events into a unified list
--
-- This includes:
--   1. Normal delegate_vesting_shares operations (create/update/remove)
--   2. account_create_with_delegation operations
--   3. Synthesized HF23 reset records
--
-- Note: return_vesting_delegation is NOT included here because it doesn't
-- affect the (delegator, delegatee) relationship - it only returns vests
-- to the delegator after the delegation was already removed.
--
-- MATERIALIZED ensures the unified list is computed once for all downstream CTEs.
---------------------------------------------------------------------------------------
delegations_in_query AS MATERIALIZED (
-- Delegations made in query (create/update/remove operations)
  SELECT
    delegator,
    delegatee,
    balance,
    source_op,
    source_op_block
  FROM process_block_range_data_b
  WHERE op_type_id IN (_op_delegate_vesting_shares, _op_account_create_with_delegation)

  UNION ALL

-- HF23 delegation resets (synthesized from hardfork_hive operations)
  SELECT
    delegator,
    delegatee,
    balance,
    source_op,
    source_op_block
  FROM hf23_resets
),
---------------------------------------------------------------------------------------
-- STEP 5: Get previous delegation balances for delta calculation
--
-- THE SQUASHING PATTERN:
-- We process many operations but only store the FINAL STATE. This enables fast sync
-- because we avoid intermediate writes. To calculate correct deltas (how much was
-- actually delegated/received), we need to know the STARTING balance.
--
-- Example: Alice has 1000 VESTS delegated to Bob (stored in DB)
--   Block 100: Alice changes delegation to 1500 VESTS
--   Block 150: Alice changes delegation to 800 VESTS
--   Block 200: Alice removes delegation (0 VESTS)
--
-- If we're processing blocks 100-200:
--   - Final state: 0 VESTS (we store this)
--   - Raw delta for Bob (received_vests): 1500-1000 + 800-1500 + 0-800 = -1000
--   - Note: Delegator side uses GREATEST(delta, 0) to only count increases.
--     Delegator reductions happen later via return_vesting_delegation operations.
--   - We need the previous balance (1000) to calculate this correctly
--
-- This CTE finds all unique (delegator, delegatee) pairs and gets their
-- previous balance from the database (or 0 if new).
---------------------------------------------------------------------------------------
group_by_delegator_delegatee AS MATERIALIZED (
  SELECT
    delegator,
    delegatee,
    MAX(source_op) as source_op
  FROM delegations_in_query
  GROUP BY delegator, delegatee
),
------------------------------------------------------------------------------
-- Fetch previous delegation balance from current_accounts_delegations table.
-- If no previous delegation exists (new delegation pair), default to 0.
-- source_op=0 marks this as a "synthetic" previous record, not a real operation.
------------------------------------------------------------------------------
get_prev_delegation AS (
  SELECT
    gb.delegator,
    gb.delegatee,
    COALESCE(cad.balance, 0) as balance,
    0 AS source_op,       -- Synthetic record: not a real operation
    0 AS source_op_block
  FROM group_by_delegator_delegatee gb
  LEFT JOIN current_accounts_delegations cad ON cad.delegator = gb.delegator AND cad.delegatee = gb.delegatee
),
------------------------------------------------------------------------------
-- Combine current operations with previous balances.
-- This creates a complete timeline for each (delegator, delegatee) pair.
------------------------------------------------------------------------------
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

---------------------------------------------------------------------------------------
-- STEP 6: Calculate deltas using window functions
--
-- NUMERICAL EXAMPLE (balance_delta calculation):
-- For alice->bob delegation with operations:
--   source_op=0: balance=1000 (previous, from DB)    -> delta = 1000 - 0 = 1000
--   source_op=100: balance=1500 (update)             -> delta = 1500 - 1000 = 500
--   source_op=150: balance=800 (update)              -> delta = 800 - 1500 = -700
--   source_op=200: balance=0 (remove)                -> delta = 0 - 800 = -800
--
-- LAG(balance, 1, 0): Get previous row's balance (default 0 for first row)
-- balance_delta = current_balance - previous_balance
--
-- LAST OPERATION WINS PATTERN:
-- ROW_NUMBER() OVER (ORDER BY source_op DESC) assigns rn=1 to the LATEST operation.
-- We only persist the final state (rn=1) to current_accounts_delegations.
--
-- WINDOW CLAUSE OPTIMIZATION:
-- Using named windows (w_asc, w_desc) allows PostgreSQL to compute both
-- calculations in a single pass over the data, avoiding duplicate sorting.
---------------------------------------------------------------------------------------
/*
 Example ordering for alice->bob pair (from narrative above):
 rn | source_op | balance | balance_delta
  4       0        1000       1000        <- synthetic previous (not stored)
  3       100      1500       500         <- intermediate (deltas used, row not stored)
  2       150      800        -700        <- intermediate (deltas used, row not stored)
  1       200      0          -800        <- FINAL STATE: stored in current_accounts_delegations
*/
delegation_delta AS MATERIALIZED (
  SELECT
    delegator,
    delegatee,
    balance,
    balance - LAG(balance, 1, 0) OVER w_asc AS balance_delta,
    source_op,
    source_op_block,
    ROW_NUMBER() OVER w_desc AS rn  -- rn=1 is the LATEST operation (last operation wins)
  FROM add_prev_delegation
  WINDOW
    w_asc AS (PARTITION BY delegator, delegatee ORDER BY source_op),      -- For LAG (chronological)
    w_desc AS (PARTITION BY delegator, delegatee ORDER BY source_op DESC) -- For ROW_NUMBER (reverse)
),
---------------------------------------------------------------------------------------
-- STEP 7: Calculate aggregated vesting share changes per account
--
-- DELEGATION VESTING SHARE MECHANICS:
-- When alice delegates 1000 VESTS to bob:
--   - Alice's effective voting power DECREASES by 1000 (delegated_vests goes up)
--   - Bob's effective voting power INCREASES by 1000 (received_vests goes up)
--
-- The account_delegations table stores CUMULATIVE totals:
--   - delegated_vests: Total VESTS this account has delegated OUT to others
--   - received_vests: Total VESTS this account has received FROM others
--
-- We aggregate all balance_deltas per account to get the net change.
---------------------------------------------------------------------------------------
union_delegations AS (
  ------------------------------------------------------------------------------
  -- PART A: Delegator side (vests going OUT)
  --
  -- HF23 SPECIAL HANDLING:
  -- For HF23 resets, balance_delta is negative (e.g., -1000 if alice had
  -- delegated 1000). Normally we'd use GREATEST(balance_delta, 0) to only
  -- count positive delegations, but HF23 operations MUST reduce delegated_vests.
  --
  -- The CASE expression checks if this operation IS the HF23 reset itself
  -- (matching delegator and source_op). If so, allow negative deltas.
  -- Otherwise, use GREATEST to ignore reductions (handled by return_vesting).
  ------------------------------------------------------------------------------
  SELECT
    dd.delegator AS account_id,
    0 AS received_vests,
    (
      CASE
        WHEN NOT EXISTS (SELECT 1 FROM process_block_range_data_b gl WHERE gl.delegator = dd.delegator AND gl.op_type_id = _op_hardfork_hive AND gl.source_op = dd.source_op) THEN
          -- Normal case: only count positive delegations (increases)
          -- Reductions happen via return_vesting_delegation later
          GREATEST(dd.balance_delta, 0)
        ELSE
          -- HF23 case: this IS the reset operation, allow negative delta
          dd.balance_delta
        END
    ) AS delegated_vests
  FROM delegation_delta dd
  WHERE dd.source_op > 0  -- Exclude synthetic previous record

  UNION ALL

  ------------------------------------------------------------------------------
  -- PART B: Delegatee side (vests coming IN)
  --
  -- Every delegation change affects the delegatee's received_vests.
  -- Unlike delegator side, we always use the full delta (positive or negative)
  -- because delegatees see immediate effect of delegation changes.
  ------------------------------------------------------------------------------
  SELECT
    delegatee AS account_id,
    balance_delta AS received_vests,
    0
  FROM delegation_delta
  WHERE source_op > 0  -- Exclude synthetic previous record

  UNION ALL

  ------------------------------------------------------------------------------
  -- PART C: Return vesting delegation (vests returning to delegator)
  --
  -- When a delegation is removed, the delegatee loses access immediately,
  -- but the delegator doesn't get the vests back for ~5 days (security measure).
  -- The return_vesting_delegation_operation is a virtual operation that marks
  -- when vests actually return.
  --
  -- The balance here is NEGATIVE (from process_return_vesting_shares_operation),
  -- representing vests being "returned" = reducing delegated_vests.
  ------------------------------------------------------------------------------
  SELECT
    delegator,
    0 AS received_vests,
    balance  -- Negative value: reduces delegated_vests
  FROM process_block_range_data_b
  WHERE op_type_id = _op_return_vesting_delegation
),
------------------------------------------------------------------------------
-- Aggregate all delegation changes per account
------------------------------------------------------------------------------
sum_delegations AS (
  SELECT
    ud.account_id,
    SUM(ud.received_vests) AS received_vests,
    SUM(ud.delegated_vests) AS delegated_vests
  FROM union_delegations ud
  GROUP BY ud.account_id
),
------------------------------------------------------------------------------
-- STEP 8: Extract FINAL delegation state for each (delegator, delegatee) pair
--
-- SQUASHING RESULT: From all the operations processed, we only need the
-- LATEST state (rn=1) to store in current_accounts_delegations.
--
-- Filter source_op > 0: Exclude synthetic "previous" records (they're not
-- real operations, just used for delta calculation).
--
-- MATERIALIZED ensures this is computed once for both insert and delete CTEs.
------------------------------------------------------------------------------
prepare_newest_delegation_pairs AS MATERIALIZED (
  SELECT
    dd.delegator,
    dd.delegatee,
    dd.balance,
    dd.source_op,
    dd.source_op_block
  FROM delegation_delta dd
  WHERE dd.rn = 1 AND dd.source_op > 0
),
---------------------------------------------------------------------------------------
-- STEP 9: Persist final delegation states (UPSERT active delegations)
--
-- ON CONFLICT (UPSERT) pattern:
-- - If (delegator, delegatee) exists: UPDATE balance and source_op
-- - If (delegator, delegatee) doesn't exist: INSERT new row
--
-- Only INSERT/UPDATE where balance > 0 (active delegations).
-- Delegations with balance = 0 are handled in delete_canceled_delegations.
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
  WHERE balance > 0  -- Only active delegations
  ON CONFLICT ON CONSTRAINT pk_current_accounts_delegations
  DO UPDATE SET
      balance = EXCLUDED.balance,
      source_op = EXCLUDED.source_op
  RETURNING cab.delegator AS delegator
),
---------------------------------------------------------------------------------------
-- STEP 10: Delete canceled delegations (balance = 0)
--
-- When a delegation is removed (balance set to 0), we DELETE the row from
-- current_accounts_delegations rather than keeping a zero-balance record.
-- This keeps the table lean and queries fast.
--
-- Note: The delegation history is still tracked via balance_delta calculations;
-- we just don't keep zero-balance rows in the "current state" table.
---------------------------------------------------------------------------------------
delete_canceled_delegations AS (
  DELETE FROM current_accounts_delegations cad
  USING prepare_newest_delegation_pairs pn
  WHERE
    cad.delegator = pn.delegator AND
    cad.delegatee = pn.delegatee AND pn.balance = 0
  RETURNING cad.delegator AS delegator
),
---------------------------------------------------------------------------------------
-- STEP 11: Update account_delegations summary table (UPSERT)
--
-- account_delegations stores CUMULATIVE totals per account:
--   - received_vests: Total VESTS received from all delegators
--   - delegated_vests: Total VESTS delegated out to all delegatees
--
-- ON CONFLICT pattern adds the new delta to existing values:
--   new_value = old_value + delta
--
-- This is an aggregate/summary table for fast lookups without
-- scanning all individual delegation records.
---------------------------------------------------------------------------------------
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

------------------------------------------------------------------------------
-- STEP 12: Return counts for logging/debugging
--
-- Execute all CTEs and capture counts from data-modifying CTEs.
-- The SELECT triggers execution of the entire CTE chain.
------------------------------------------------------------------------------
SELECT
  (SELECT count(*) FROM insert_current_delegations) AS insert_current_delegations,
  (SELECT count(*) FROM delete_canceled_delegations) AS delete_canceled_delegations,
  (SELECT count(*) FROM insert_delegations) AS insert_delegations
INTO __insert_current_delegations, __delete_canceled_delegations, __insert_delegations;

END
$$;

RESET ROLE;

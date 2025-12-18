SET ROLE btracker_owner;

/**
 * process_block_range_withdrawals - Squash withdrawal operations into final state for fast sync.
 * Processes: withdraw_vesting, set_withdraw_vesting_route, fill_vesting_withdraw,
 *            hardfork_hive (HF23 reset), transfer_to_vesting_completed, delayed_voting (HF24+)
 */
CREATE OR REPLACE FUNCTION process_block_range_withdrawals(IN _from INT, IN _to INT, IN _report_step INT = 1000)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  -- Operation type IDs (looked up dynamically from hafd.operation_types)
  _op_withdraw_vesting                INT := btracker_backend.op_withdraw_vesting();
  _op_set_withdraw_vesting_route      INT := btracker_backend.op_set_withdraw_vesting_route();
  _op_fill_vesting_withdraw           INT := btracker_backend.op_fill_vesting_withdraw();
  _op_transfer_to_vesting_completed   INT := btracker_backend.op_transfer_to_vesting_completed();
  _op_delayed_voting                  INT := btracker_backend.op_delayed_voting();
  _op_hardfork_hive                   INT := btracker_backend.op_hardfork_hive();
  -- Hardfork numbers (protocol constants)
  _hf_vests_precision                 INT := btracker_backend.hf_vests_precision();
  _hf_withdraw_rate                   INT := btracker_backend.hf_withdraw_rate();
  _hf_delayed_voting                  INT := btracker_backend.hf_delayed_voting();
  -- Hardfork blocks (looked up from hafd.applied_hardforks)
  _hf_vests_precision_block           INT;
  _hf_withdraw_rate_block             INT;
  _hf_delayed_voting_block            INT;
  -- Counter variables (prefixed with __) are for debugging/logging purposes.
  -- They capture row counts from various operations to help diagnose sync issues.
  __insert_routes                     INT;
  __delete_routes                     INT;
  __insert_sum_of_routes              INT;
  __delete_hf23_routes_count          INT;
  __delete_hf23_routes                INT;
  __insert_not_yet_filled_withdrawals INT;
  __reset_filled_withdrawals          INT;
BEGIN
-- Look up hardfork blocks once at the start
SELECT
  MAX(CASE WHEN hardfork_num = _hf_vests_precision THEN block_num END),
  MAX(CASE WHEN hardfork_num = _hf_withdraw_rate THEN block_num END),
  MAX(CASE WHEN hardfork_num = _hf_delayed_voting THEN block_num END)
INTO _hf_vests_precision_block, _hf_withdraw_rate_block, _hf_delayed_voting_block
FROM hafd.applied_hardforks
WHERE hardfork_num IN (_hf_vests_precision, _hf_withdraw_rate, _hf_delayed_voting);

-- Validate that all required hardfork blocks were found
IF _hf_vests_precision_block IS NULL OR _hf_withdraw_rate_block IS NULL OR _hf_delayed_voting_block IS NULL THEN
  RAISE EXCEPTION 'Required hardfork block not found. HF%=%, HF%=%, HF%=%',
    _hf_vests_precision, _hf_vests_precision_block,
    _hf_withdraw_rate, _hf_withdraw_rate_block,
    _hf_delayed_voting, _hf_delayed_voting_block;
END IF;

------------------------------------------------------------------------------
-- SECTION 1: WITHDRAWALS (withdraw_vesting_operation, hardfork_hive_operation)
------------------------------------------------------------------------------
WITH ops_withdraw AS MATERIALIZED (
  -- MATERIALIZED: prevents re-scanning operations_view when referenced later.
  -- Single scan extracts both operation types - more efficient than two separate queries.
  SELECT ov.body, ov.op_type_id, ov.id, ov.block_num
  FROM operations_view ov
  WHERE
    ov.op_type_id IN (_op_withdraw_vesting, _op_hardfork_hive) AND
    ov.block_num BETWEEN _from AND _to
),
process_block_range_data_b AS MATERIALIZED
(
  -- LATERAL + CASE pattern: Parse each operation with appropriate helper based on op_type_id.
  -- Alternative would be separate queries per op_type, requiring multiple table scans.
  -- Hardfork boolean flags passed to helper - determines VESTS precision multiplier and
  -- withdrawal rate (104 weeks pre-HF16, 13 weeks post-HF16).
  SELECT
    result.account_name,
    result.withdrawn,
    result.vesting_withdraw_rate,
    result.to_withdraw,
    o.id AS source_op
  FROM ops_withdraw o
  CROSS JOIN LATERAL (
    SELECT (
      CASE
        WHEN o.op_type_id = _op_withdraw_vesting
          THEN btracker_backend.process_withdraw_vesting_operation(
            o.body,
            o.block_num > _hf_vests_precision_block,  -- HF1: VESTS precision change
            o.block_num > _hf_withdraw_rate_block     -- HF16: 104->13 week withdrawal
          )
        WHEN o.op_type_id = _op_hardfork_hive
          -- HF23: Reset withdrawal state to 0 for affected accounts (Steemit Inc accounts)
          THEN btracker_backend.process_reset_withdraw_hf23(o.body)
      END
    ).*
  ) AS result
),
group_by_account AS MATERIALIZED (
  -- "LAST OPERATION WINS" pattern: If account has multiple withdrawals in block range,
  -- only the final state matters. ROW_NUMBER() DESC by source_op gives us row_num=1 for latest.
  -- WHY? withdraw_vesting is a FULL STATE REPLACEMENT - it sets to_withdraw/rate/withdrawn
  -- from scratch. Intermediate operations are completely superseded by later ones.
  -- Example: Account sets 1000 VESTS withdrawal, then changes to 500 VESTS - we only store 500.
  SELECT
    account_name,
    withdrawn,
    vesting_withdraw_rate,
    to_withdraw,
    source_op,
    ROW_NUMBER() OVER w_account_desc AS row_num
  FROM process_block_range_data_b
  WINDOW w_account_desc AS (PARTITION BY account_name ORDER BY source_op DESC)
),
account_names_withdrawal AS (
  -- Extract unique account names BEFORE joining to accounts_view.
  -- This allows batch lookup instead of correlated subquery per row.
  SELECT DISTINCT account_name FROM group_by_account WHERE row_num = 1
),
account_ids_withdrawal AS MATERIALIZED (
  -- MATERIALIZED + batch lookup: Convert account names to IDs in single scan of accounts_view.
  -- Without MATERIALIZED, planner might inline this and execute lookup per-row.
  SELECT av.name AS account_name, av.id AS account_id
  FROM accounts_view av
  WHERE av.name IN (SELECT account_name FROM account_names_withdrawal)
),
join_latest_withdraw AS (
  SELECT
    ai.account_id,
    gba.withdrawn,
    gba.vesting_withdraw_rate,
    gba.to_withdraw,
    gba.source_op
  FROM group_by_account gba
  JOIN account_ids_withdrawal ai ON ai.account_name = gba.account_name
  WHERE gba.row_num = 1
)
-- UPSERT pattern: INSERT new accounts, UPDATE existing with new withdrawal state.
-- ON CONFLICT replaces all withdrawal fields - new withdraw_vesting resets the entire state.
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

------------------------------------------------------------------------------
-- SECTION 2: WITHDRAWAL ROUTES (set_withdraw_vesting_route_operation)
------------------------------------------------------------------------------
WITH ops_routes AS MATERIALIZED (
  SELECT ov.body, ov.id
  FROM operations_view ov
  WHERE
    ov.op_type_id = _op_set_withdraw_vesting_route AND
    ov.block_num BETWEEN _from AND _to
),
process_block_range_data_b AS (
  SELECT
    result.from_account,
    result.to_account,
    result.percent,
    o.id AS source_op
  FROM ops_routes o
  CROSS JOIN LATERAL (
    SELECT (btracker_backend.process_set_withdraw_vesting_route_operation(o.body)).*
  ) AS result
),
group_by_account AS MATERIALIZED (
  -- Routes are keyed by (from_account, to_account) pair. Same user can set multiple
  -- routes to same destination in one block range - only latest matters.
  SELECT
    from_account,
    to_account,
    percent,
    source_op,
    ROW_NUMBER() OVER w_accounts_desc AS row_num
  FROM process_block_range_data_b
  WINDOW w_accounts_desc AS (PARTITION BY from_account, to_account ORDER BY source_op DESC)
),
account_names_routes AS (
  -- Both from_account AND to_account need ID lookup - collect all names first.
  SELECT DISTINCT from_account AS account_name FROM group_by_account WHERE row_num = 1
  UNION
  SELECT DISTINCT to_account AS account_name FROM group_by_account WHERE row_num = 1
),
account_ids_routes AS MATERIALIZED (
  SELECT av.name AS account_name, av.id AS account_id
  FROM accounts_view av
  WHERE av.name IN (SELECT account_name FROM account_names_routes)
),
join_latest_withdraw_routes AS (
  SELECT
    ai_from.account_id AS from_account,
    ai_to.account_id AS to_account,
    gba.percent,
    gba.source_op
  FROM group_by_account gba
  JOIN account_ids_routes ai_from ON ai_from.account_name = gba.from_account
  JOIN account_ids_routes ai_to ON ai_to.account_name = gba.to_account
  WHERE gba.row_num = 1
),
join_prev_route AS (
  -- LEFT JOIN to existing routes: need to know if this is NEW route, UPDATE, or DELETE.
  -- prev_percent NULL = route didn't exist before (new route)
  -- prev_percent NOT NULL + percent != 0 = updating existing route
  -- prev_percent NOT NULL + percent = 0 = deleting existing route
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
  -- Calculate withdraw_routes DELTA for account_withdraws.withdraw_routes counter.
  -- API displays "number of active routes" - we track count changes here:
  --   +1 = new route added
  --    0 = existing route updated (percent changed but route still exists)
  --   -1 = route deleted (percent set to 0)
  -- Edge case: prev_percent NULL AND percent = 0 means user tried to delete non-existent route - no change.
  SELECT
    from_account,
    to_account,
    percent,
    (
      CASE
        WHEN prev_percent IS NULL AND percent != 0 THEN
          1   -- NEW: route didn't exist, now it does
        WHEN prev_percent IS NOT NULL AND percent != 0 THEN
          0   -- UPDATE: route existed, still exists (just different percent)
        WHEN prev_percent IS NOT NULL AND percent = 0 THEN
          -1  -- DELETE: route existed, now being removed
        ELSE
          0   -- NOOP: tried to delete non-existent route
      END
    ) AS withdraw_routes,
    source_op
  FROM join_prev_route
),
sum_routes AS (
  -- Aggregate delta per from_account. Account might add 2 routes and delete 1 in same block range.
  -- Net effect: +1 to withdraw_routes counter.
  SELECT
    from_account,
    SUM(withdraw_routes) AS withdraw_routes
  FROM add_routes
  GROUP BY from_account
),
insert_routes AS (
  -- UPSERT active routes (percent != 0). Percent=0 means delete, handled separately.
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
  -- Delete routes where percent=0 (user explicitly removes routing rule).
  DELETE FROM account_routes ar
  USING add_routes ar2
  WHERE
    ar.account = ar2.from_account AND
    ar.to_account = ar2.to_account AND
    ar2.percent = 0
  RETURNING ar.account
),
insert_sum_of_routes AS (
  -- Update withdraw_routes counter in account_withdraws.
  -- Uses += delta (not absolute): we're adding the NET CHANGE from this block range
  -- to the existing counter value from previous block ranges.
  -- Example: Counter was 3 from prev sync. This range: +2 adds, -1 delete = +1 delta. New total: 4.
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


------------------------------------------------------------------------------
-- SECTION 2b: HF23 ROUTE CLEANUP (hardfork_hive_operation)
-- HF23 was the Steem->Hive fork. Certain accounts (Steemit Inc) had ALL routes deleted.
-- This section runs only during the HF23 block range and cleans up those routes.
------------------------------------------------------------------------------
WITH ops_hf23 AS MATERIALIZED (
  -- Will be empty for all block ranges except the one containing HF23.
  -- No performance impact when empty - PostgreSQL optimizes empty CTE chains.
  SELECT ov.body, ov.id
  FROM operations_view ov
  WHERE
    ov.op_type_id = _op_hardfork_hive AND
    ov.block_num BETWEEN _from AND _to
),
process_block_range_data_b AS (
  SELECT
    (o.body->'value'->>'account') AS account_name,
    o.id AS source_op
  FROM ops_hf23 o
),
account_names_hf23 AS (
  SELECT DISTINCT account_name FROM process_block_range_data_b
),
account_ids_hf23 AS MATERIALIZED (
  SELECT av.name AS account_name, av.id AS account_id
  FROM accounts_view av
  WHERE av.name IN (SELECT account_name FROM account_names_hf23)
),
process_block_range_data_with_ids AS (
  SELECT
    ai.account_id,
    pb.source_op
  FROM process_block_range_data_b pb
  JOIN account_ids_hf23 ai ON ai.account_name = pb.account_name
),
join_current_routes_for_hf23_accounts AS MATERIALIZED (
  -- Find ALL existing routes for HF23-affected accounts that were created BEFORE the HF23 op.
  -- source_op comparison ensures we only delete routes that existed pre-HF23.
  -- Routes created AFTER HF23 (in same block range) should NOT be deleted.
  SELECT
    ar.account,
    ar.to_account
  FROM account_routes ar
  JOIN process_block_range_data_with_ids gi ON ar.account = gi.account_id
  WHERE gi.source_op > ar.source_op
),
count_deleted_routes AS (
  -- Count routes being deleted per account - needed to update withdraw_routes counter.
  SELECT
    account,
    COUNT(*) AS count
  FROM join_current_routes_for_hf23_accounts
  GROUP BY account
),
delete_hf23_routes_count AS (
  -- Decrement withdraw_routes counter by number of deleted routes.
  -- INSERT uses the positive count value, but ON CONFLICT SUBTRACTS it from
  -- the existing withdraw_routes counter (account_withdraws.withdraw_routes - EXCLUDED).
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
  -- Actually delete the route records.
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

------------------------------------------------------------------------------
-- SECTION 3: FILL WITHDRAWS (fill_vesting_withdraw_operation)
-- Virtual operation emitted weekly when withdrawal portion is paid out.
-- Tracks progress: withdrawn += fill_amount. When withdrawn >= to_withdraw, withdrawal completes.
------------------------------------------------------------------------------
WITH ops_fill AS MATERIALIZED (
  SELECT ov.body, ov.id, ov.block_num
  FROM operations_view ov
  WHERE
    ov.op_type_id = _op_fill_vesting_withdraw AND
    ov.block_num BETWEEN _from AND _to
),
process_block_range_data_raw AS MATERIALIZED
(
  -- HF1 flag determines VESTS precision multiplier (pre-HF1 values need *1000000).
  SELECT
    result.account_name,
    result.withdrawn,
    o.id AS source_op
  FROM ops_fill o
  CROSS JOIN btracker_backend.process_fill_vesting_withdraw_operation_for_withdrawals(o.body,o.block_num > _hf_vests_precision_block) AS result
),
account_names_fill AS (
  SELECT DISTINCT account_name FROM process_block_range_data_raw
),
account_ids_fill AS MATERIALIZED (
  SELECT av.name AS account_name, av.id AS account_id
  FROM accounts_view av
  WHERE av.name IN (SELECT account_name FROM account_names_fill)
),
process_block_range_data_b AS (
  SELECT
    ai.account_id,
    pr.withdrawn,
    pr.source_op
  FROM process_block_range_data_raw pr
  JOIN account_ids_fill ai ON ai.account_name = pr.account_name
),
--------------------------------------------------------------------------------------
group_by_account AS MATERIALIZED (
  SELECT DISTINCT account_id
  FROM process_block_range_data_b
),
join_current_withdraw AS (
  -- Fetch CURRENT withdrawal state from account_withdraws.
  -- Need to_withdraw to know when withdrawal completes, and source_op to filter fills.
  SELECT
    aw.account,
    aw.withdrawn,
    aw.to_withdraw,
    aw.source_op
  FROM account_withdraws aw
  JOIN group_by_account gba ON aw.account = gba.account_id
),
--------------------------------------------------------------------------------------
get_fills_concerning_current_withdrawal AS MATERIALIZED (
  -- CRITICAL: Only count fills that occurred AFTER the current withdrawal started.
  -- source_op > aw.source_op filters out fills from PREVIOUS withdrawals.
  --
  -- WHY this is critical: Operations have monotonically increasing IDs (source_op).
  -- A fill belongs to the withdrawal that has the highest source_op BELOW the fill's source_op.
  -- Without this filter, fills from a completed previous withdrawal would inflate the
  -- current withdrawal's progress, potentially marking it complete prematurely.
  --
  -- Edge case: Account completes withdrawal, starts new one, gets fills in same block range.
  -- The new withdrawal has higher source_op, so old fills (lower source_op) are correctly excluded.
  --
  -- SUM(cp.withdrawn) = total filled in this block range
  -- + aw.withdrawn = previously filled amount (from earlier block range syncs)
  -- = new total withdrawn
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
  -- INCOMPLETE withdrawals: withdrawn < to_withdraw. Just update the withdrawn counter.
  INSERT INTO account_withdraws
    (account, withdrawn)
  SELECT
    gf.account_id,
    gf.withdrawn
  FROM get_fills_concerning_current_withdrawal gf
  WHERE gf.to_withdraw > gf.withdrawn
  ON CONFLICT ON CONSTRAINT pk_account_withdraws
  DO UPDATE SET
    withdrawn = EXCLUDED.withdrawn
  RETURNING account
),
reset_filled_withdrawals AS (
  -- COMPLETE withdrawals: withdrawn >= to_withdraw. Reset withdrawal state to 0.
  -- This signals "no active withdrawal" and prepares for next withdraw_vesting_operation.
  -- NOTE: Only resets vesting_withdraw_rate, to_withdraw, withdrawn. Other columns
  -- (withdraw_routes, delayed_vests) are NOT touched - routing rules persist after completion.
  INSERT INTO account_withdraws
    (account, vesting_withdraw_rate, to_withdraw, withdrawn)
  SELECT
    gf.account_id,
    0,
    0,
    0
  FROM get_fills_concerning_current_withdrawal gf
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

------------------------------------------------------------------------------
-- SECTION 4: DELAYED VOTING (HF24+)
-- Introduced in HF24: Power-ups have 30-day delay before gaining full voting power.
-- Tracks delayed_vests balance that affects governance voting weight.
------------------------------------------------------------------------------
WITH ops_delays AS MATERIALIZED (
  SELECT ov.body, ov.op_type_id, ov.id, ov.block_num
  FROM operations_view ov
  WHERE
    ov.op_type_id IN (_op_fill_vesting_withdraw, _op_transfer_to_vesting_completed, _op_delayed_voting) AND
    ov.block_num BETWEEN _from AND _to
),
process_block_range_data_raw AS MATERIALIZED
(
  -- Only process ops AFTER HF24 - delayed voting didn't exist before.
  -- Operations affect delayed_vests differently:
  --   fill_vesting_withdraw: NEGATIVE (withdrawals reduce delayed vests proportionally)
  --   transfer_to_vesting_completed: POSITIVE (power-up adds to delayed vests)
  --   delayed_voting: NEGATIVE (delay period ends, vests become "active")
  SELECT
    result.from_account AS from_account_name,
    result.to_account AS to_account_name,
    result.withdrawn,
    result.deposited,
    o.id AS source_op
  FROM ops_delays o
  CROSS JOIN LATERAL (
    SELECT (
      CASE
        WHEN o.op_type_id = _op_fill_vesting_withdraw
          THEN btracker_backend.process_fill_vesting_withdraw_operation(o.body)
        WHEN o.op_type_id = _op_transfer_to_vesting_completed
          THEN btracker_backend.process_transfer_to_vesting_completed_operation(o.body)
        WHEN o.op_type_id = _op_delayed_voting
          THEN btracker_backend.process_delayed_voting_operation(o.body)
      END
    ).*
  ) AS result
  WHERE o.block_num > _hf_delayed_voting_block  -- HF24+ only: pre-HF24 block ranges will produce 0 rows here
),
-- NOTE: For block ranges entirely before HF24, process_block_range_data_raw will be empty
-- and all subsequent CTEs in this section will process 0 rows. This is expected behavior.
account_names_delays AS (
  -- Some operations affect two accounts (power-up from A to B).
  -- from_account always exists, to_account may be NULL (self power-up).
  SELECT DISTINCT from_account_name AS account_name FROM process_block_range_data_raw
  UNION
  SELECT DISTINCT to_account_name AS account_name FROM process_block_range_data_raw WHERE to_account_name IS NOT NULL
),
account_ids_delays AS MATERIALIZED (
  SELECT av.name AS account_name, av.id AS account_id
  FROM accounts_view av
  WHERE av.name IN (SELECT account_name FROM account_names_delays)
),
process_block_range_data_b AS (
  -- LEFT JOIN for to_account: NULL means self power-up (from_account = to_account).
  SELECT
    ai_from.account_id AS from_account,
    ai_to.account_id AS to_account,
    pr.withdrawn,
    pr.deposited,
    pr.source_op
  FROM process_block_range_data_raw pr
  JOIN account_ids_delays ai_from ON ai_from.account_name = pr.from_account_name
  LEFT JOIN account_ids_delays ai_to ON ai_to.account_name = pr.to_account_name
),
--------------------------------------------------------------------------------------
-- Flatten operations into (account_id, balance_delta) rows.
-- One operation may produce TWO rows: one for from_account (withdrawn), one for to_account (deposited).
union_delays AS MATERIALIZED (
  SELECT
    from_account AS account_id,
    withdrawn AS balance,  -- typically negative (reducing delayed vests)
    source_op
  FROM process_block_range_data_b

  UNION ALL

  SELECT
    to_account AS account_id,
    deposited AS balance,  -- typically positive (adding delayed vests)
    source_op
  FROM process_block_range_data_b
  WHERE to_account IS NOT NULL
),
group_by_account AS MATERIALIZED (
  SELECT DISTINCT account_id
  FROM union_delays
),
join_prev_delays AS (
  -- Fetch PREVIOUS delayed_vests balance from account_withdraws table.
  -- source_op=0 is a sentinel value: real operations have source_op >= 1 (monotonic IDs).
  -- When we ORDER BY source_op, this row sorts FIRST and becomes row_num=1.
  -- This makes the previous balance the BASE CASE for the recursive CTE.
  SELECT
    gb.account_id,
    COALESCE(aw.delayed_vests,0) AS balance,
    0 AS source_op
  FROM group_by_account gb
  LEFT JOIN account_withdraws aw ON aw.account = gb.account_id
),
union_prev_delays AS (
  -- Combine previous balance (row 0) with current operations (rows 1..N).
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
  -- Number rows per account, ordered by source_op.
  -- row_num=1 is the previous balance, row_num=2+ are operations.
  SELECT
    account_id,
    balance,
    source_op,
    ROW_NUMBER() OVER w_account_asc AS row_num
  FROM union_prev_delays
  WINDOW w_account_asc AS (PARTITION BY account_id ORDER BY source_op)
),
--------------------------------------------------------------------------------------
-- WHY RECURSIVE CTE?
-- delayed_vests has a constraint: balance >= 0 (can't be negative).
-- Simple SUM would allow negative values. We need GREATEST(..., 0) at each step.
--
-- Example without recursion (WRONG):
--   prev_balance=100, op1=-50, op2=-60, op3=+20
--   SUM = 100 + (-50) + (-60) + 20 = 10  ← But intermediate value 100-50-60 = -10 is invalid!
--
-- With recursion (CORRECT):
--   row1: 100 (prev balance)
--   row2: GREATEST(100 + (-50), 0) = 50
--   row3: GREATEST(50 + (-60), 0) = 0  ← Can't go below 0
--   row4: GREATEST(0 + 20, 0) = 20    ← Final balance is 20
--
-- Each row depends on the CLAMPED result of the previous row, not the raw sum.
-- This is why we can't use window function SUM - it doesn't support recursive clamping.
--------------------------------------------------------------------------------------
recursive_delays AS MATERIALIZED (
  WITH RECURSIVE calculated_delays AS (
    -- Base case: Previous balance (row_num=1) - already clamped (stored value >= 0).
    SELECT
      cp.account_id,
      cp.balance,
      cp.source_op,
      cp.row_num
    FROM add_row_number cp
    WHERE cp.row_num = 1

    UNION ALL

    -- Recursive case: Add delta, clamp to 0 minimum.
    -- GREATEST ensures delayed_vests never goes negative.
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
  -- Extract final balance per account (highest row_num = last operation processed).
  SELECT
    account_id,
    balance,
    ROW_NUMBER() OVER w_account_desc AS rn
  FROM
    recursive_delays
  WINDOW w_account_desc AS (PARTITION BY account_id ORDER BY source_op DESC)
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

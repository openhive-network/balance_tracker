SET ROLE btracker_owner;

/**
 * Processes recurrent transfer operations for a block range using the "squashing" pattern.
 * Handles: recurrent_transfer (setup/modify), fill_recurrent_transfer (execution),
 *          failed_recurrent_transfer (failure tracking/cancellation).
 */

CREATE OR REPLACE FUNCTION process_block_range_recurrent_transfers(IN _from INT, IN _to INT, IN _report_step INT = 1000)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  -- Operation type IDs (looked up dynamically from hafd.operation_types)
  _op_recurrent_transfer        INT := btracker_backend.op_recurrent_transfer();
  _op_fill_recurrent_transfer   INT := btracker_backend.op_fill_recurrent_transfer();
  _op_failed_recurrent_transfer INT := btracker_backend.op_failed_recurrent_transfer();
  -- Counters
  __insert_transfers            INT;
  __delete_canceled_transfers   INT;
BEGIN

  /*
   * =====================================================================================
   * SQUASHING PATTERN OVERVIEW
   * =====================================================================================
   * During fast sync, we process thousands of blocks at once (e.g., blocks 1000-2000).
   * A single recurrent transfer may have multiple operations in this range:
   *
   *   Example: Transfer from alice->bob (transfer_id=0) in blocks 1000-2000:
   *     - Block 1050: recurrent_transfer (setup: 10 HIVE, 24h recurrence, 5 executions)
   *     - Block 1200: fill_recurrent_transfer (executed, remaining=4)
   *     - Block 1400: failed_recurrent_transfer (insufficient balance, consecutive_failures=1)
   *     - Block 1600: fill_recurrent_transfer (executed, remaining=3, consecutive_failures reset to 0)
   *
   * Instead of updating the database 4 times (expensive I/O), we "squash" these operations:
   *   1. Collect all operations in the block range
   *   2. Use recursive CTE to apply them in sequence (in memory)
   *   3. Write only the FINAL state to the database (single INSERT/UPDATE)
   *
   * This reduces database writes from O(operations) to O(unique transfers).
   * =====================================================================================
   */

  WITH ops AS MATERIALIZED (
    /*
     * Step 1: Fetch all relevant operations from the block range.
     *
     * MATERIALIZED keyword forces PostgreSQL to execute this CTE first and store results
     * in a temporary structure. This is critical because:
     *   - Without MATERIALIZED, the planner might inline this CTE into each reference
     *   - That would cause multiple scans of operations_view (expensive)
     *   - With MATERIALIZED, we scan once and reuse the result set
     *
     * We fetch three operation types that affect recurrent transfers:
     *   - recurrent_transfer: User creates/modifies/cancels a scheduled transfer
     *   - fill_recurrent_transfer: System executes a scheduled transfer successfully
     *   - failed_recurrent_transfer: System failed to execute (e.g., insufficient funds)
     */
    SELECT ov.body, ov.op_type_id, ov.id
    FROM operations_view ov
    WHERE
      ov.op_type_id IN (_op_recurrent_transfer, _op_fill_recurrent_transfer, _op_failed_recurrent_transfer) AND
      ov.block_num BETWEEN _from AND _to
  ),

  process_block_range_data_b AS (
    /*
     * Step 2: Parse each operation's JSON body into structured fields.
     *
     * CROSS JOIN LATERAL allows us to call a different parsing function based on op_type_id.
     * Each parser extracts relevant fields and normalizes them into a common structure:
     *
     * Lifecycle of a recurrent transfer:
     *   - recurrent_transfer_operation: User initiates or modifies transfer
     *       Sets: from, to, amount, nai, recurrence (hours), executions count, memo
     *       If amount=0: This is a CANCELLATION request (delete_transfer=true)
     *
     *   - fill_recurrent_transfer_operation: System executed the transfer
     *       Updates: remaining_executions (decremented), consecutive_failures reset to 0
     *       If remaining_executions=0: Transfer complete (delete_transfer=true)
     *       Note: Does NOT include amount/nai/recurrence/memo (inherited from setup)
     *
     *   - failed_recurrent_transfer_operation: System failed to execute
     *       Updates: consecutive_failures (incremented), remaining_executions
     *       If consecutive_failures=10 OR deleted=true: Transfer cancelled (delete_transfer=true)
     *       Note: Does NOT include amount/nai/recurrence/memo (inherited from setup)
     *
     * Edge case - pair_id (transfer_id):
     *   HF28 introduced pair_id allowing multiple recurrent transfers between same accounts.
     *   Example: alice->bob can have transfer_id=0 for "rent" and transfer_id=1 for "salary"
     *   If extensions is empty, pair_id defaults to 0 (backward compatibility).
     */
    SELECT
      result.from_account AS from_account,
      result.to_account AS to_account,
      result.transfer_id AS transfer_id,
      result.nai AS nai,
      result.amount AS amount,
      result.consecutive_failures AS consecutive_failures,
      result.remaining_executions AS remaining_executions,
      result.recurrence AS recurrence,
      result.memo AS memo,
      result.delete_transfer AS delete_transfer,
      o.id AS source_op,
      o.op_type_id
    FROM ops o
    CROSS JOIN LATERAL (
      SELECT (
        CASE
          WHEN o.op_type_id = _op_recurrent_transfer
            THEN btracker_backend.process_recurrent_transfer_operations(o.body)
          WHEN o.op_type_id = _op_fill_recurrent_transfer
            THEN btracker_backend.process_fill_recurrent_transfer_operations(o.body)
          WHEN o.op_type_id = _op_failed_recurrent_transfer
            THEN btracker_backend.process_fail_recurrent_transfer_operations(o.body)
        END
      ).*
    ) AS result
  ),

  ---------------------------------------------------------------------------------------
  -- Optimization 1: WINDOW clause for combined ROW_NUMBER - compute both in single pass
  ---------------------------------------------------------------------------------------
  latest_rec_transfers AS MATERIALIZED (
    /*
     * Step 3: Assign sequence numbers for chronological ordering within each transfer.
     *
     * For the squashing pattern, we need to process operations in order. We compute
     * TWO row numbers in a SINGLE PASS over the data (performance optimization):
     *
     *   rn_per_transfer_desc: 1 = most recent operation (used to find final state)
     *   rn_per_transfer_asc:  1 = earliest operation (used as starting point for recursion)
     *
     * Example for alice->bob transfer_id=0:
     *   source_op=1050 (setup):   rn_asc=1, rn_desc=4
     *   source_op=1200 (fill):    rn_asc=2, rn_desc=3
     *   source_op=1400 (fail):    rn_asc=3, rn_desc=2
     *   source_op=1600 (fill):    rn_asc=4, rn_desc=1  <-- final state (rn_desc=1)
     *
     * The WINDOW clause allows defining the partition/order once and reusing it,
     * which is more efficient than two separate ROW_NUMBER() calls.
     *
     * MATERIALIZED ensures this computation happens once and results are stored,
     * since multiple downstream CTEs reference this data.
     */
    SELECT
      ar.from_account,
      ar.to_account,
      ar.transfer_id,
      ar.nai,
      ar.amount,
      ar.consecutive_failures,
      ar.remaining_executions,
      ar.recurrence,
      ar.memo,
      ar.delete_transfer,
      ar.source_op,
      ar.op_type_id,
      ROW_NUMBER() OVER w_desc AS rn_per_transfer_desc,
      ROW_NUMBER() OVER w_asc AS rn_per_transfer_asc
    FROM process_block_range_data_b ar
    WINDOW
      w_desc AS (PARTITION BY ar.from_account, ar.to_account, ar.transfer_id ORDER BY ar.source_op DESC),
      w_asc AS (PARTITION BY ar.from_account, ar.to_account, ar.transfer_id ORDER BY ar.source_op)
  ),

  ---------------------------------------------------------------------------------------
  -- Optimization 2: Batch account ID lookups instead of correlated subqueries
  ---------------------------------------------------------------------------------------
  all_account_names AS (
    /*
     * Step 4a: Collect all unique account names that appear in this batch.
     *
     * We need to convert account names (text) to account IDs (int) for storage.
     * Instead of doing N correlated subqueries (one per row), we:
     *   1. Collect all unique names first
     *   2. Do a single batch lookup
     *   3. JOIN the results
     *
     * This transforms O(N) individual lookups into O(1) batch lookup.
     */
    SELECT DISTINCT from_account AS account_name FROM latest_rec_transfers
    UNION
    SELECT DISTINCT to_account AS account_name FROM latest_rec_transfers
  ),
  account_ids AS MATERIALIZED (
    /*
     * Step 4b: Batch lookup of account name -> account ID mapping.
     *
     * MATERIALIZED because this result is JOINed multiple times below.
     * Single scan of hive.accounts_view is much faster than per-row lookups.
     */
    SELECT av.name AS account_name, av.id AS account_id
    FROM hive.accounts_view av
    WHERE av.name IN (SELECT account_name FROM all_account_names)
  ),

  ---------------------------------------------------------------------------------------
  delete_transfer AS MATERIALIZED (
    /*
     * Step 5: Identify transfers that should be DELETED from our tracking table.
     *
     * A transfer is marked for deletion (delete_transfer=TRUE) when:
     *   - User cancels it (recurrent_transfer with amount=0)
     *   - All executions completed (remaining_executions=0 after fill)
     *   - Too many consecutive failures (consecutive_failures=10)
     *   - Explicitly deleted by system (failed_recurrent_transfer with deleted=true)
     *
     * We only check the FINAL state (rn_per_transfer_desc=1) because:
     *   - A transfer might be cancelled then recreated in same block range
     *   - Only the final state matters for what we store
     *
     * Example edge case:
     *   Block 1000: recurrent_transfer alice->bob (setup)
     *   Block 1100: recurrent_transfer alice->bob amount=0 (cancel)
     *   Block 1200: recurrent_transfer alice->bob (new setup)
     *
     *   Here the final state is NOT a deletion, even though block 1100 was a cancel.
     */
    SELECT
      lrt.from_account,
      lrt.to_account,
      from_acc.account_id AS from_account_id,
      to_acc.account_id AS to_account_id,
      lrt.transfer_id
    FROM latest_rec_transfers lrt
    JOIN account_ids from_acc ON from_acc.account_name = lrt.from_account
    JOIN account_ids to_acc ON to_acc.account_name = lrt.to_account
    WHERE lrt.delete_transfer = TRUE AND lrt.rn_per_transfer_desc = 1
  ),

  ---------------------------------------------------------------------------------------
  -- Optimization 3: LEFT JOIN anti-join pattern instead of NOT EXISTS
  -- Optimization 5: Use pre-computed rn_per_transfer_asc from latest_rec_transfers
  ---------------------------------------------------------------------------------------
  excluded_deleted_transfers AS MATERIALIZED (
    /*
     * Step 6: Filter out transfers that will be deleted.
     *
     * For transfers marked for deletion, we don't need to compute their final state
     * via the recursive CTE - we'll just delete them. This optimization skips
     * unnecessary work for cancelled/completed transfers.
     *
     * The LEFT JOIN + WHERE ... IS NULL pattern (anti-join) is often faster than NOT EXISTS
     * because it allows the planner more flexibility in join ordering.
     *
     * Pattern explanation:
     *   LEFT JOIN delete_transfer dt ON (matching keys)
     *   WHERE dt.from_account IS NULL
     *
     *   This keeps only rows that DON'T have a match in delete_transfer.
     */
    SELECT
      lrt.from_account,
      lrt.to_account,
      from_acc.account_id AS from_account_id,
      to_acc.account_id AS to_account_id,
      lrt.transfer_id,
      lrt.nai,
      lrt.amount,
      lrt.consecutive_failures,
      lrt.remaining_executions,
      lrt.recurrence,
      lrt.memo,
      lrt.delete_transfer,
      lrt.source_op,
      lrt.op_type_id,
      lrt.rn_per_transfer_desc,
      lrt.rn_per_transfer_asc
    FROM latest_rec_transfers lrt
    JOIN account_ids from_acc ON from_acc.account_name = lrt.from_account
    JOIN account_ids to_acc ON to_acc.account_name = lrt.to_account
    LEFT JOIN delete_transfer dt ON
      dt.from_account = lrt.from_account AND
      dt.to_account = lrt.to_account AND
      dt.transfer_id = lrt.transfer_id
    WHERE dt.from_account IS NULL
  ),

  latest_transfers_in_table AS (
    /*
     * Step 7: Fetch existing state from the database for transfers being updated.
     *
     * For the recursive squashing to work correctly, we need the STARTING state:
     *   - If transfer exists in DB: Use DB values as base, apply new ops on top
     *   - If transfer is new: First operation in range provides the base
     *
     * This CTE fetches the current DB state for transfers that have new operations.
     * The rn_per_transfer_desc=1 filter ensures we only do one lookup per transfer
     * (not one per operation).
     *
     * The rn_per_transfer_asc=0 and rn_per_transfer_desc=0 values indicate this
     * is the "base" row from the database, not from the operations stream.
     * This allows the recursive CTE to distinguish between DB state and new ops.
     *
     * Edge case - new transfer:
     *   If LEFT JOIN finds no match (rt.* is NULL), this row will have NULL values.
     *   The recursive CTE handles this by starting from the first operation instead.
     */
    SELECT
      edt.from_account_id,
      edt.to_account_id,
      edt.transfer_id,
      rt.nai,
      rt.amount,
      rt.consecutive_failures,
      rt.remaining_executions,
      rt.recurrence,
      rt.memo,
      FALSE AS delete_transfer,
      rt.source_op,
      _op_recurrent_transfer AS op_type_id,
      0 as rn_per_transfer_desc,
      0 AS rn_per_transfer_asc
    FROM excluded_deleted_transfers edt
    LEFT JOIN recurrent_transfers rt ON
      rt.from_account = edt.from_account_id AND
      rt.to_account = edt.to_account_id AND
      rt.transfer_id = edt.transfer_id
    WHERE edt.rn_per_transfer_desc = 1
  ),

  union_total_transfers AS MATERIALIZED (
    /*
     * Step 8: Combine database state with new operations for recursive processing.
     *
     * This creates a unified dataset containing:
     *   - Row with rn_per_transfer_asc=0: Existing DB state (if any)
     *   - Rows with rn_per_transfer_asc=1,2,3...: New operations in chronological order
     *
     * The recursive CTE will iterate through these in order of rn_per_transfer_asc,
     * applying each operation to compute the cumulative state.
     *
     * MATERIALIZED ensures this combined dataset is computed once before recursion.
     */
    SELECT
      ltin.from_account_id,
      ltin.to_account_id,
      ltin.transfer_id,
      ltin.nai,
      ltin.amount,
      ltin.consecutive_failures,
      ltin.remaining_executions,
      ltin.recurrence,
      ltin.memo,
      ltin.delete_transfer,
      ltin.source_op,
      ltin.op_type_id,
      ltin.rn_per_transfer_desc,
      ltin.rn_per_transfer_asc
    FROM latest_transfers_in_table ltin

    UNION ALL

    SELECT
      edt.from_account_id,
      edt.to_account_id,
      edt.transfer_id,
      edt.nai,
      edt.amount,
      edt.consecutive_failures,
      edt.remaining_executions,
      edt.recurrence,
      edt.memo,
      edt.delete_transfer,
      edt.source_op,
      edt.op_type_id,
      edt.rn_per_transfer_desc,
      edt.rn_per_transfer_asc
    FROM excluded_deleted_transfers edt
  ),

  recursive_transfers AS MATERIALIZED (
    /*
     * Step 9: THE SQUASHING ENGINE - Recursive CTE to compute final state.
     *
     * This is the core of the squashing pattern. For each transfer, we:
     *   1. Start with base state (rn_per_transfer_asc=0, from DB or NULL)
     *   2. Apply each subsequent operation in order
     *   3. Output only the final state (rn_per_transfer_desc=1)
     *
     * The recursion follows this pattern:
     *   Base case: SELECT ... WHERE rn_per_transfer_asc = 0  (starting state)
     *   Recursive case: JOIN on rn_per_transfer_asc = prev + 1 (next operation)
     *
     * Field update logic per operation type:
     *
     *   recurrent_transfer (user action - setup/modify):
     *     - Updates: nai, amount, recurrence, memo (user can change these)
     *     - consecutive_failures: Value from operation (usually 0 for new setup)
     *     - remaining_executions: Value from operation
     *     - Special: If recurrence unchanged AND not a fresh start, keep old source_op
     *       (This preserves the trigger date calculation for modified-but-not-rescheduled transfers)
     *
     *   fill_recurrent_transfer (system action - successful execution):
     *     - Preserves: nai, amount, recurrence, memo (not in this op type)
     *     - Updates: remaining_executions (decremented by system)
     *     - Resets: consecutive_failures to 0 (success clears failure count)
     *
     *   failed_recurrent_transfer (system action - execution failed):
     *     - Preserves: nai, amount, recurrence, memo (not in this op type)
     *     - Updates: remaining_executions, consecutive_failures from operation
     *
     * Edge case - source_op preservation:
     *   When a user modifies a transfer WITHOUT changing recurrence, we preserve
     *   the original source_op. This is because source_op is used to calculate
     *   the next execution time. If user just changes amount but keeps same
     *   schedule, we shouldn't reset the execution timer.
     *
     *   Example:
     *     Block 1000: Setup transfer, recurrence=24h, source_op=1000
     *     Block 1500: Modify amount only, recurrence still 24h
     *     Result: source_op stays 1000 (next execution still based on original schedule)
     *
     *   But if recurrence changes:
     *     Block 1000: Setup transfer, recurrence=24h, source_op=1000
     *     Block 1500: Modify recurrence to 12h
     *     Result: source_op becomes 1500 (schedule reset to new recurrence)
     */
    WITH RECURSIVE calculated_transfers AS (
      -- Base case: Start with DB state or first operation (rn_per_transfer_asc=0)
      SELECT
        ed.from_account_id,
        ed.to_account_id,
        ed.transfer_id,
        ed.nai,
        ed.amount,
        ed.consecutive_failures,
        ed.remaining_executions,
        ed.recurrence,
        ed.memo,
        ed.delete_transfer,
        ed.source_op,
        ed.rn_per_transfer_asc,
        ed.rn_per_transfer_desc
      FROM union_total_transfers ed
      WHERE ed.rn_per_transfer_asc = 0

      UNION ALL

      -- Recursive case: Apply each subsequent operation
      SELECT
        prev.from_account_id,
        prev.to_account_id,
        prev.transfer_id,
        -- nai/amount/recurrence/memo: Only recurrent_transfer op provides these, others inherit
        (CASE WHEN next_cp.op_type_id = _op_recurrent_transfer THEN next_cp.nai ELSE prev.nai END) AS nai,
        (CASE WHEN next_cp.op_type_id = _op_recurrent_transfer THEN next_cp.amount ELSE prev.amount END) AS amount,
        -- consecutive_failures/remaining_executions: All op types provide these
        next_cp.consecutive_failures,
        next_cp.remaining_executions,
        (CASE WHEN next_cp.op_type_id = _op_recurrent_transfer THEN next_cp.recurrence ELSE prev.recurrence END) AS recurrence,
        (CASE WHEN next_cp.op_type_id = _op_recurrent_transfer THEN next_cp.memo ELSE prev.memo END) AS memo,
        next_cp.delete_transfer,
        -- source_op: Complex logic to preserve schedule timing when appropriate
        -- if the transfer is an update of previous transfer
        -- and between them there is no deletion of the transfer
        -- the block number that is used to update trigger date must not be changed
        -- so we need to use the block number from the previous transfer
        (
          CASE
            WHEN next_cp.op_type_id = _op_recurrent_transfer AND next_cp.recurrence = prev.recurrence AND NOT prev.delete_transfer THEN
              prev.source_op
            ELSE
              next_cp.source_op
          END
        ) AS source_op,
        next_cp.rn_per_transfer_asc,
        next_cp.rn_per_transfer_desc
      FROM calculated_transfers prev
      JOIN union_total_transfers next_cp ON
        prev.from_account_id          = next_cp.from_account_id AND
        prev.to_account_id            = next_cp.to_account_id AND
        prev.transfer_id              = next_cp.transfer_id AND
        next_cp.rn_per_transfer_asc   = prev.rn_per_transfer_asc + 1
    )
    -- Output only the final computed state (rn_per_transfer_desc=1)
    SELECT * FROM calculated_transfers
    WHERE rn_per_transfer_desc = 1
  ),

  ---------------------------------------------------------------------------------------
  -- Step 10: Write final states to database
  ---------------------------------------------------------------------------------------
  insert_transfers AS (
    /*
     * UPSERT pattern: Insert new transfers or update existing ones.
     *
     * ON CONFLICT ... DO UPDATE is PostgreSQL's upsert mechanism:
     *   - If no row exists with this (from_account, to_account, transfer_id): INSERT
     *   - If row exists: UPDATE the existing row with new values
     *
     * This handles both new transfers and modifications in a single statement,
     * which is more efficient than separate INSERT and UPDATE queries.
     *
     * The EXCLUDED pseudo-table contains the values that would have been inserted,
     * allowing us to use them in the UPDATE clause.
     *
     * Why ON CONFLICT ON CONSTRAINT pk_recurrent_transfers:
     *   The primary key (from_account, to_account, transfer_id) uniquely identifies
     *   a recurrent transfer. Two transfers with same from/to but different transfer_id
     *   are distinct (HF28 pair_id feature).
     */
    INSERT INTO recurrent_transfers AS rt
      (
        from_account,
        to_account,
        transfer_id,
        nai,
        amount,
        consecutive_failures,
        remaining_executions,
        recurrence,
        memo,
        source_op
      )
    SELECT
      from_account_id,
      to_account_id,
      transfer_id,
      nai,
      amount,
      consecutive_failures,
      remaining_executions,
      recurrence,
      memo,
      source_op
    FROM recursive_transfers
    ON CONFLICT ON CONSTRAINT pk_recurrent_transfers
    DO UPDATE SET
        nai = EXCLUDED.nai,
        amount = EXCLUDED.amount,
        consecutive_failures = EXCLUDED.consecutive_failures,
        remaining_executions = EXCLUDED.remaining_executions,
        recurrence = EXCLUDED.recurrence,
        memo = EXCLUDED.memo,
        source_op = EXCLUDED.source_op
    RETURNING rt.from_account AS from_account
  ),

  ---------------------------------------------------------------------------------------
  -- Step 11: Remove completed/cancelled transfers from tracking table
  ---------------------------------------------------------------------------------------
  delete_canceled_transfers AS (
    /*
     * Delete transfers that are no longer active.
     *
     * Transfers are deleted when:
     *   - User cancelled (amount=0 in recurrent_transfer)
     *   - All executions completed (remaining_executions=0)
     *   - Too many failures (consecutive_failures=10, auto-cancelled by system)
     *
     * We only track ACTIVE recurrent transfers. Once completed or cancelled,
     * the transfer is removed. Historical data is preserved in the operations
     * themselves (in hafd.operations), not in our tracking table.
     *
     * USING clause allows us to join the delete_transfer CTE, deleting only
     * rows that match the identified-for-deletion set.
     */
    DELETE FROM recurrent_transfers rt
    USING delete_transfer dt
    WHERE
      rt.from_account = dt.from_account_id AND
      rt.to_account = dt.to_account_id AND
      rt.transfer_id = dt.transfer_id
    RETURNING rt.transfer_id AS transfer_id
  )

  /*
   * Final aggregation: Count operations for logging/diagnostics.
   * This executes all the above CTEs and captures row counts.
   */
  SELECT
    (SELECT count(*) FROM insert_transfers) AS insert_transfers,
    (SELECT count(*) FROM delete_canceled_transfers) AS delete_canceled_transfers
  INTO __insert_transfers, __delete_canceled_transfers;

END
$$;

RESET ROLE;

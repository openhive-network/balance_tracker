SET ROLE btracker_owner;

/*
 * Processes HBD->HIVE conversion operations within a block range.
 * Maintains convert_state table tracking pending conversions and their remaining amounts.
 * Operations: convert_operation (initiate), fill_convert_request_operation (fulfill).
 */
CREATE OR REPLACE FUNCTION process_block_range_converts(
    IN _from INT,
    IN _to   INT,
    IN _report_step INT DEFAULT 1000
)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
/*
 * Planner settings optimized for complex CTE queries:
 *   from_collapse_limit/join_collapse_limit = 16: Allow planner to consider more join
 *     orderings. Default (8) is too restrictive for queries with 15+ CTEs, causing
 *     suboptimal plans. Value 16 balances planning time vs plan quality.
 *   jit = OFF: JIT compilation adds startup overhead (~50-100ms) that exceeds savings
 *     for this query pattern. These queries run frequently with small result sets
 *     where JIT's benefits don't materialize.
 */
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS $func$
DECLARE
    /*
     * Cache operation type IDs to avoid repeated lookups during processing.
     * These map to hafd.operation_types entries:
     *   - convert_operation: User initiates HBD->HIVE conversion (3.5 day delay)
     *   - fill_convert_request_operation: System completes the conversion
     *
     * Note: collateralized_convert_operation (HIVE->HBD) is NOT handled here.
     * While it also has a 3.5 day delay, it uses different mechanics: user receives
     * HBD immediately while HIVE collateral is held for settlement.
     */
    _op_convert  INT := btracker_backend.op_convert();
    _op_fillconv INT := btracker_backend.op_fill_convert_request();
    /* Diagnostic counters for tracking processing results */
    __upd_pre    INT := 0;  -- Pre-existing conversions updated
    __ins_new    INT := 0;  -- New conversions inserted/upserted
    __del_any    INT := 0;  -- Conversions deleted (fully filled)
BEGIN
    WITH ops_in_range AS MATERIALIZED (
        /*
         * Fetch all conversion-related operations in this block range.
         *
         * MATERIALIZED forces PostgreSQL to compute this CTE once and store results,
         * preventing repeated scans of operations_view. Critical for performance because
         * operations_view is typically large and this CTE is referenced multiple times.
         *
         * Without MATERIALIZED, PostgreSQL might inline the CTE and scan the view
         * separately for each reference, dramatically increasing I/O.
         *
         * Operations captured:
         *   - convert_operation: User requests to convert HBD to HIVE
         *     Example: {"owner":"alice","requestid":1,"amount":{"amount":"1000","nai":"@@000000013"}}
         *     Creates a pending conversion that completes after 3.5 days
         *
         *   - fill_convert_request_operation: System fulfills a pending conversion
         *     Example: {"owner":"alice","requestid":1,"amount_in":{"amount":"1000","nai":"@@000000013"}}
         *     Reduces or completes the pending conversion
         */
        SELECT ov.id AS op_id, ov.block_num, ov.op_type_id, ov.body
        FROM operations_view ov
        WHERE ov.block_num BETWEEN _from AND _to
          AND ov.op_type_id IN (_op_convert, _op_fillconv)
    ),
    events_raw AS MATERIALIZED (
        /*
         * Transform raw operations into normalized conversion events.
         *
         * Why CROSS JOIN LATERAL + CASE pattern?
         *   - Single pass through ops_in_range (vs multiple UNIONs scanning it N times)
         *   - CASE dispatches to appropriate parser based on operation type
         *   - LATERAL allows accessing outer row (o.body, o.op_type_id) in subquery
         *   - .* expansion unpacks the composite type into individual columns
         *
         * Alternative approaches and why they're worse:
         *   - UNION of separate queries: Scans ops_in_range twice, 2x I/O
         *   - Single function with polymorphism: More complex, harder to debug
         *
         * The convert_event type structure (from backend/convert_requests.sql):
         *   - owner: account name initiating the conversion (e.g., 'alice')
         *   - request_id: per-owner unique identifier (e.g., 1, 2, 3...)
         *   - nai: asset type being converted FROM (13=HBD for regular convert)
         *   - amount_in: satoshis being converted (amount * 1000 for HBD/HIVE)
         *
         * Example flow:
         *   Block 1000: Alice's convert op for 100 HBD
         *     -> event: {owner:'alice', request_id:1, nai:13, amount_in:100000}
         *   Block 1500: System fills Alice's conversion
         *     -> event: {owner:'alice', request_id:1, nai:13, amount_in:100000}
         */
        SELECT
            e.owner,
            e.request_id,
            e.nai,
            e.amount_in,
            o.op_type_id,
            o.op_id,
            o.block_num,
            o.body
        FROM ops_in_range o
        CROSS JOIN LATERAL (
            SELECT (
                CASE
                    WHEN o.op_type_id = _op_convert
                        THEN btracker_backend.get_convert_request_event(o.body)
                    WHEN o.op_type_id = _op_fillconv
                        THEN btracker_backend.get_fill_convert_event(o.body)
                END
            ).*
        ) AS e
    ),
    account_names AS (
        /*
         * Extract unique account names for efficient bulk lookup.
         *
         * Collecting distinct names first allows a single JOIN against accounts_view
         * rather than per-row correlated subqueries, which would be O(n) lookups
         * vs O(1) hash join.
         */
        SELECT DISTINCT owner FROM events_raw
    ),
    account_ids AS MATERIALIZED (
        /*
         * Resolve account names to numeric IDs (owner_id).
         *
         * convert_state uses integer IDs as foreign keys because:
         *   - Storage efficiency: INT (4 bytes) vs TEXT (variable, typically 10-20 bytes)
         *   - Join performance: Integer comparisons are faster than string comparisons
         *   - Index efficiency: B-tree indexes on integers are more compact
         *
         * MATERIALIZED ensures this lookup happens once, not per-reference.
         */
        SELECT av.name AS account_name, av.id AS account_id
        FROM accounts_view av
        WHERE av.name IN (SELECT owner FROM account_names)
    ),
    events AS MATERIALIZED (
        /*
         * Final normalized events with owner_id resolved.
         *
         * This is the canonical dataset that all subsequent CTEs work from.
         * MATERIALIZED because it's referenced by both 'creates' and 'fills' CTEs.
         */
        SELECT
            ai.account_id AS owner_id,
            er.owner,
            er.request_id,
            er.nai,
            er.amount_in,
            er.op_type_id,
            er.op_id,
            er.block_num,
            er.body
        FROM events_raw er
        JOIN account_ids ai ON ai.account_name = er.owner
    ),

    /*
     * =========================================================================
     * PHASE 1: Partition events into creates (convert ops) and fills
     * =========================================================================
     *
     * Conversion lifecycle:
     *   1. User submits convert_operation for X HBD
     *   2. After 3.5 days, system issues fill_convert_request_operation
     *   3. Fill can be partial or complete
     *   4. When remaining reaches 0, conversion is complete
     */
    creates AS (
        /*
         * All conversion initiation events in this block range.
         *
         * create_amount is the initial amount the user wants to convert.
         * create_op_id and create_block track when/where the conversion started.
         *
         * Edge case: Same (owner_id, request_id, nai) can appear multiple times
         * if a user completes a conversion and reuses the same request_id for a
         * new conversion within the same block range. Handled later in
         * latest_creates by keeping only the most recent one.
         */
        SELECT
            owner_id,
            request_id,
            nai,
            e.amount_in AS create_amount,
            op_id AS create_op_id,
            block_num AS create_block
        FROM events e
        WHERE op_type_id = _op_convert
    ),
    fills AS (
        /*
         * All conversion fill events in this block range.
         *
         * fill_amount is how much of the pending conversion was fulfilled.
         * In practice, a conversion fills completely in a single operation after
         * the 3.5 day waiting period. The code handles multiple fills defensively.
         */
        SELECT
            owner_id,
            request_id,
            nai,
            e.amount_in AS fill_amount,
            op_id AS fill_op_id,
            block_num AS fill_block
        FROM events e
        WHERE op_type_id = _op_fillconv
    ),

    /*
     * =========================================================================
     * PHASE 2: Handle PRE-EXISTING conversions (created BEFORE this block range)
     * =========================================================================
     *
     * "Pre-existing" conversions already exist in convert_state from previous
     * block range processing. We detect them by finding fills that have NO
     * matching create in the current block range.
     *
     * Example scenario:
     *   Block 1000: Alice creates conversion #5 (processed in previous run, stored in convert_state)
     *   Block 2000: Alice's conversion #5 gets filled (current range)
     *   -> Conversion #5 is "pre-existing" because create was in earlier range
     *
     * This separation is critical for "squashing" optimization: we handle
     * pre-existing records differently from new ones to minimize database writes.
     */
    create_keys AS (
        /*
         * Set of (owner_id, request_id, nai) tuples created in THIS block range.
         * Used to distinguish "new" conversions from "pre-existing" ones.
         *
         * NAI is included in the key because the same request_id could theoretically
         * be used for different asset types (though this is rare in practice).
         */
        SELECT DISTINCT owner_id, request_id, nai
        FROM creates
    ),
    pre_fills AS (
        /*
         * Fills for PRE-EXISTING conversions (conversion was created before this range).
         *
         * Detection logic: LEFT JOIN to create_keys, WHERE create_keys IS NULL
         * This gives us fills that have no corresponding create in current range,
         * meaning the conversion must have been created in a previous range.
         *
         * Aggregation: SUM all fill amounts per conversion because we need total
         * amount filled in this range to calculate new remaining balance.
         *
         * Example:
         *   Conversion #5 (created in block 1000) has remaining=500 in convert_state
         *   Current range (blocks 2000-3000) has fills: 200 + 150 = 350
         *   -> sum_fill = 350
         */
        SELECT
            f.owner_id,
            f.request_id,
            f.nai,
            SUM(f.fill_amount)::bigint AS sum_fill
        FROM fills f
        LEFT JOIN create_keys k USING (owner_id, request_id, nai)
        WHERE k.owner_id IS NULL  -- No create found in this range = pre-existing conversion
        GROUP BY f.owner_id, f.request_id, f.nai
    ),
    pre_calc AS MATERIALIZED (
        /*
         * Calculate new remaining amounts for pre-existing conversions after fills.
         *
         * Formula: new_remaining = current_remaining - sum_of_fills
         * GREATEST(..., 0) prevents negative values (defensive programming against
         * data inconsistencies or blockchain bugs).
         *
         * Filters applied for efficiency:
         *   - sum_fill <> 0: Skip if no actual fill amount (shouldn't happen but defensive)
         *   - remaining <> new_remaining: Skip if no change (avoids unnecessary updates)
         *
         * Example:
         *   Conversion has remaining=500, fills in this range sum to 200
         *   -> new_remaining = 500 - 200 = 300
         *
         * Edge case: new_remaining could be 0 if conversion is exactly fulfilled.
         * These records are identified in pre_zero for deletion.
         */
        SELECT
            s.owner_id,
            s.request_id,
            s.nai,
            s.remaining,
            pf.sum_fill,
            GREATEST(s.remaining - pf.sum_fill, 0)::bigint AS new_remaining
        FROM convert_state s
        JOIN pre_fills pf
          ON (s.owner_id, s.request_id, s.nai) = (pf.owner_id, pf.request_id, pf.nai)
        WHERE pf.sum_fill <> 0
          AND s.remaining <> GREATEST(s.remaining - pf.sum_fill, 0)::bigint
    ),
    upd_pre AS (
        /*
         * Update remaining amounts for pre-existing conversions that were PARTIALLY filled.
         *
         * This handles conversions that:
         *   - Existed before this block range (pre-existing)
         *   - Got partially filled (new_remaining > 0)
         *   - Value actually changed (s.remaining <> pc.new_remaining)
         *
         * Conversions that are FULLY filled (new_remaining <= 0) are handled by
         * del_any instead - they get deleted, not updated.
         *
         * RETURNING 1 allows counting affected rows for diagnostics.
         */
        UPDATE convert_state s
           SET remaining = pc.new_remaining
          FROM pre_calc pc
         WHERE (s.owner_id, s.request_id, s.nai) = (pc.owner_id, pc.request_id, pc.nai)
           AND s.remaining <> pc.new_remaining
           AND pc.new_remaining > 0
        RETURNING 1
    ),
    pre_zero AS (
        /*
         * Pre-existing conversions that are now FULLY FILLED (remaining <= 0).
         * These conversions should be deleted from convert_state.
         *
         * Example: Conversion had remaining=100, got filled for 100+ -> delete it
         */
        SELECT owner_id, request_id, nai
        FROM pre_calc
        WHERE new_remaining <= 0
    ),

    /*
     * =========================================================================
     * PHASE 3: Handle NEW conversions (created IN this block range)
     * =========================================================================
     *
     * "Squashing" pattern: Within a single block range, a conversion might be:
     *   created -> partially filled -> filled again -> fully completed
     *
     * Instead of:
     *   1. INSERT new conversion
     *   2. UPDATE with first fill
     *   3. UPDATE with second fill
     *   4. DELETE when fully filled
     *
     * We compute the FINAL STATE at end of range and do a single operation:
     *   - If remaining > 0: INSERT/UPSERT the final state
     *   - If remaining <= 0: Don't insert at all (conversion completed in same range)
     *
     * This "squashing" reduces database writes from O(n) to O(1) per conversion,
     * critical for fast sync where block ranges can span thousands of blocks.
     */
    latest_creates AS MATERIALIZED (
        /*
         * For each (owner_id, request_id, nai), keep only the LATEST create.
         *
         * Why might there be multiple creates for same conversion key?
         * If a conversion completes and the user reuses the same request_id
         * for a new conversion within the same block range, we keep only
         * the most recent one (highest op_id).
         *
         * DISTINCT ON + ORDER BY create_op_id DESC keeps highest op_id (latest).
         *
         * MATERIALIZED because this CTE is referenced by fills_after_latest and
         * remaining_calc.
         */
        SELECT DISTINCT ON (owner_id, request_id, nai)
               owner_id,
               request_id,
               nai,
               create_amount,
               create_block,
               create_op_id
        FROM creates
        ORDER BY owner_id, request_id, nai, create_op_id DESC
    ),
    fills_after_latest AS (
        /*
         * Sum fills that occurred AFTER the latest create for each conversion.
         *
         * Critical: f.fill_op_id > c.create_op_id ensures we only count fills
         * that happened after the conversion was created, not fills that might
         * belong to a previous incarnation of the same request_id.
         *
         * Example scenario (rare but possible):
         *   op_id 100: Alice creates conversion #5 for 1000 HBD
         *   op_id 150: Conversion #5 fills 1000 HBD (completes)
         *   op_id 200: Alice creates NEW conversion #5 for 500 HBD (reusing ID)
         *   op_id 250: Conversion #5 fills 500 HBD (completes)
         *
         * For latest create (op_id 200), we only count fill at op_id 250.
         * Fill at op_id 150 is ignored because it belongs to the old conversion #5.
         *
         * LEFT JOIN with COALESCE handles conversions with no fills (common case:
         * conversion created, not yet filled within the same block range).
         */
        SELECT
            c.owner_id,
            c.request_id,
            c.nai,
            COALESCE(SUM(f.fill_amount), 0)::bigint AS sum_fill_after
        FROM latest_creates c
        LEFT JOIN fills f
          ON (f.owner_id, f.request_id, f.nai) = (c.owner_id, c.request_id, c.nai)
         AND f.fill_op_id > c.create_op_id
        GROUP BY c.owner_id, c.request_id, c.nai
    ),
    remaining_calc AS MATERIALIZED (
        /*
         * Calculate remaining amount for each newly created conversion.
         *
         * Formula: remaining = create_amount - sum_of_fills_after_create
         *
         * Example:
         *   Created with 1000 HBD, filled 300 + 200 = 500 in same range
         *   -> remaining = 1000 - 500 = 500 HBD still pending
         *
         * request_block is preserved for the convert_state record to track
         * when the conversion was initiated.
         */
        SELECT
            c.owner_id,
            c.request_id,
            c.nai,
            (c.create_amount - fal.sum_fill_after)::bigint AS remaining,
            c.create_block AS request_block
        FROM latest_creates c
        JOIN fills_after_latest fal
          ON (fal.owner_id, fal.request_id, fal.nai) = (c.owner_id, c.request_id, c.nai)
    ),

    /*
     * =========================================================================
     * PHASE 4: Determine SURVIVORS (conversions that should exist in final state)
     * =========================================================================
     */
    survivors AS (
        /*
         * Conversions from this range that should remain PENDING after processing.
         *
         * Survival criteria: remaining > 0 (not fully filled)
         *
         * Unlike orders (which can be cancelled), conversions can only be completed
         * through fills - there's no cancel operation. So we only filter by remaining.
         *
         * Conversions with remaining <= 0 are "squashed out" - they were created
         * and fully filled within the same block range, so they never need to
         * exist in convert_state.
         */
        SELECT owner_id, request_id, nai, remaining, request_block
        FROM remaining_calc
        WHERE remaining > 0
    ),

    /*
     * =========================================================================
     * PHASE 5: Apply changes to convert_state table
     * =========================================================================
     */
    ins_new AS (
        /*
         * UPSERT survivors into convert_state.
         *
         * ON CONFLICT pattern handles two scenarios:
         *   1. New conversion: INSERT succeeds, conversion is tracked
         *   2. Existing conversion (edge case): UPDATE with latest values
         *
         * The edge case occurs when a conversion with same (owner_id, request_id, nai)
         * already exists, which could happen if:
         *   - Processing overlapping block ranges (recovery scenario)
         *   - User reuses request_id for new conversion after previous completed
         *
         * IS DISTINCT FROM optimization: Only update if values actually changed.
         * This prevents unnecessary WAL writes and index updates when re-processing
         * the same block range (idempotency).
         *
         * Why upsert instead of separate insert/update?
         *   - Simpler logic: one statement handles all cases
         *   - Atomic: no race conditions between existence check and modification
         *   - Idempotent: safe to re-run on same block range
         */
        INSERT INTO convert_state (owner_id, request_id, nai, remaining, request_block)
        SELECT owner_id, request_id, nai, remaining, request_block
        FROM survivors
        ON CONFLICT (owner_id, request_id, nai) DO UPDATE
          SET remaining     = EXCLUDED.remaining,
              request_block = EXCLUDED.request_block
          WHERE (convert_state.remaining, convert_state.request_block)
                IS DISTINCT FROM (EXCLUDED.remaining, EXCLUDED.request_block)
        RETURNING 1
    ),
    keys_to_delete_raw AS (
        /*
         * Conversions that should be deleted: pre-existing ones fully filled.
         *
         * These are conversions that:
         *   - Existed in convert_state before this block range
         *   - Were fully filled during this range (new_remaining <= 0)
         *
         * New conversions that complete within the same range are simply not
         * inserted (squashed out), so they don't appear here.
         */
        SELECT owner_id, request_id, nai FROM pre_zero
    ),
    to_delete AS (
        /*
         * Final deletion list, excluding survivors.
         *
         * Why exclude survivors? Edge case protection:
         * If somehow the same key appears in both delete candidates and survivors
         * (e.g., due to unusual operation ordering), the survivor takes precedence.
         *
         * LEFT JOIN + WHERE s.owner_id IS NULL ensures we don't delete
         * conversions that should actually remain.
         *
         * DISTINCT handles any duplicate keys from different deletion sources.
         */
        SELECT DISTINCT d.owner_id, d.request_id, d.nai
        FROM keys_to_delete_raw d
        LEFT JOIN survivors s USING (owner_id, request_id, nai)
        WHERE s.owner_id IS NULL
    ),
    del_any AS (
        /*
         * Execute deletions for fully-filled conversions.
         *
         * USING clause efficiently joins to_delete with convert_state for
         * bulk deletion (vs correlated subquery which would be row-by-row).
         *
         * RETURNING 1 allows counting affected rows for diagnostics.
         */
        DELETE FROM convert_state s
        USING to_delete d
        WHERE (s.owner_id, s.request_id, s.nai) = (d.owner_id, d.request_id, d.nai)
        RETURNING 1
    )
    /*
     * Collect row counts for diagnostics/logging.
     *
     * These counters track processing results:
     *   - __upd_pre: Pre-existing conversions that were partially filled (updated)
     *   - __ins_new: New conversions inserted or existing ones upserted
     *   - __del_any: Conversions deleted (fully filled)
     *
     * Currently these are computed but not returned (function returns VOID).
     * They could be used for logging by uncommenting RAISE NOTICE statements.
     */
    SELECT
        COALESCE((SELECT COUNT(*) FROM upd_pre), 0),
        COALESCE((SELECT COUNT(*) FROM ins_new), 0),
        COALESCE((SELECT COUNT(*) FROM del_any), 0)
    INTO __upd_pre, __ins_new, __del_any;
END;
$func$;

RESET ROLE;

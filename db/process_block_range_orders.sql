SET ROLE btracker_owner;

/*
 * Processes limit order events (create, fill, cancel) within a block range.
 * Maintains order_state table with current open orders and their remaining amounts.
 * Handles order lifecycle: create -> partial fills -> fully filled or cancelled.
 */
CREATE OR REPLACE FUNCTION process_block_range_orders(
    IN _from INT,
    IN _to INT,
    IN _report_step INT DEFAULT 1000
)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
-- Planner settings: higher limits allow more join reordering for optimal plans with many CTEs.
-- JIT disabled as it adds overhead for this query pattern.
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS $func$
DECLARE
    _op_create1   INT := btracker_backend.op_limit_order_create();
    _op_create2   INT := btracker_backend.op_limit_order_create2();
    _op_fill      INT := btracker_backend.op_fill_order();
    _op_cancel    INT := btracker_backend.op_limit_order_cancel();
    _op_cancelled INT := btracker_backend.op_limit_order_cancelled();
    __ins_new     INT := 0;
    __del_any     INT := 0;
    __upd_pre     INT := 0;
BEGIN
    WITH ops_in_range AS MATERIALIZED (
        /*
         * Fetch all order-related operations in this block range.
         * MATERIALIZED hint forces PostgreSQL to compute this once and reuse,
         * preventing repeated scans of the operations_view.
         *
         * Operations captured:
         *   - limit_order_create / limit_order_create2: User places a new order
         *   - fill_order: Market match fills (partially or fully) an order
         *   - limit_order_cancel: User manually cancels their order
         *   - limit_order_cancelled: System auto-cancels (e.g., expired order)
         */
        SELECT ov.id AS op_id, ov.block_num, ov.op_type_id, (ov.body)::jsonb AS body
        FROM operations_view ov
        WHERE ov.block_num BETWEEN _from AND _to
          AND ov.op_type_id IN (_op_create1, _op_create2, _op_fill, _op_cancel, _op_cancelled)
    ),
    events_raw AS MATERIALIZED (
        /*
         * Transform raw operations into normalized order events using LATERAL + CASE pattern.
         *
         * Why LATERAL + CASE instead of multiple UNION queries?
         *   - Single pass through ops_in_range (avoids scanning it 3 times)
         *   - CASE dispatches to appropriate parser based on op_type_id
         *   - LATERAL allows row-by-row evaluation with access to outer row columns
         *
         * Why two branches with UNION ALL?
         *   - Create/cancel operations return exactly 1 row -> use CASE with .* expansion
         *   - Fill operations return 0-2 rows (one per side of trade) -> use SETOF function
         *
         * Example: fill_order affects TWO orders (one for each side of the trade),
         * so get_limit_order_fill_events() returns up to 2 rows per fill operation.
         *
         * Event structure (limit_order_event type):
         *   - owner: account name (e.g., 'alice')
         *   - order_id: per-owner unique ID (e.g., 12345)
         *   - nai: asset type being sold (13=HBD, 21=HIVE)
         *   - amount: satoshis (for creates: initial amount, for fills: amount consumed)
         */
        SELECT
            e.owner,
            e.order_id,
            e.nai,
            e.amount,
            o.op_id,
            o.block_num,
            o.op_type_id
        FROM ops_in_range o
        CROSS JOIN LATERAL (
            SELECT (
                CASE
                    WHEN o.op_type_id IN (_op_create1, _op_create2)
                        THEN btracker_backend.get_limit_order_create_event(o.body)
                    WHEN o.op_type_id IN (_op_cancel, _op_cancelled)
                        THEN btracker_backend.get_limit_order_cancel_event(o.body)
                END
            ).* WHERE o.op_type_id IN (_op_create1, _op_create2, _op_cancel, _op_cancelled)
            UNION ALL
            SELECT * FROM btracker_backend.get_limit_order_fill_events(o.body) WHERE o.op_type_id = _op_fill
        ) AS e
    ),
    account_names AS (
        /*
         * Extract unique account names for efficient bulk lookup.
         * Collecting names first allows a single JOIN against accounts_view
         * instead of per-row lookups.
         */
        SELECT DISTINCT owner FROM events_raw
    ),
    account_ids AS MATERIALIZED (
        /*
         * Resolve account names to numeric IDs (owner_id).
         * order_state uses integer IDs as foreign keys for storage efficiency
         * and faster joins compared to text-based account names.
         */
        SELECT av.name AS account_name, av.id AS account_id
        FROM accounts_view av
        WHERE av.name IN (SELECT owner FROM account_names)
    ),
    events AS MATERIALIZED (
        /*
         * Final normalized events with owner_id resolved.
         * This is the base dataset all subsequent CTEs work from.
         */
        SELECT
            ai.account_id AS owner_id,
            er.owner,
            er.order_id,
            er.nai,
            er.amount,
            er.op_id,
            er.block_num,
            er.op_type_id
        FROM events_raw er
        JOIN account_ids ai ON ai.account_name = er.owner
    ),

    /*
     * =========================================================================
     * PHASE 1: Partition events by type (creates, fills, cancels)
     * =========================================================================
     */
    creates AS MATERIALIZED (
        /*
         * All order creation events in this block range.
         * create_amount is the initial amount the user wants to sell.
         *
         * Edge case: Same (owner_id, order_id) can appear multiple times if user
         * cancels and recreates with same ID. Handled later in latest_creates.
         */
        SELECT
            owner_id,
            order_id,
            nai,
            (amount)::bigint AS create_amount,
            op_id AS create_op_id,
            block_num AS create_block
        FROM events
        WHERE op_type_id IN (_op_create1, _op_create2)
    ),
    fills AS MATERIALIZED (
        /*
         * All order fill events in this block range.
         * fill_amount is how much of the order was consumed by this match.
         *
         * An order can have many partial fills before being fully consumed.
         * Example: Order to sell 1000 HIVE might fill as: 300 + 400 + 300 = 1000
         */
        SELECT
            owner_id,
            order_id,
            nai,
            (amount)::bigint AS fill_amount,
            op_id AS fill_op_id,
            block_num AS fill_block
        FROM events
        WHERE op_type_id = _op_fill
    ),
    cancels AS MATERIALIZED (
        /*
         * All cancellation events in this block range.
         * Includes both user-initiated cancels and system auto-cancellations.
         * A cancel removes the order regardless of remaining amount.
         */
        SELECT
            owner_id,
            order_id,
            op_id AS cancel_op_id,
            block_num AS cancel_block
        FROM events
        WHERE op_type_id IN (_op_cancel, _op_cancelled)
    ),

    /*
     * =========================================================================
     * PHASE 2: Handle PRE-EXISTING orders (created BEFORE this block range)
     * =========================================================================
     *
     * Pre-existing orders already exist in order_state from previous processing.
     * We detect them by finding fills/cancels that have NO matching create in
     * this block range - meaning the create happened in an earlier range.
     *
     * Example scenario:
     *   Block 1000: Alice creates order #5 (processed in previous run)
     *   Block 2000: Alice's order #5 gets partially filled (current range)
     *   -> Order #5 is "pre-existing" because create was in earlier range
     */
    create_keys AS (
        /*
         * Set of (owner_id, order_id) pairs that were CREATED in this range.
         * Used to distinguish "new" orders from "pre-existing" ones.
         */
        SELECT DISTINCT owner_id, order_id
        FROM creates
    ),
    pre_fills AS (
        /*
         * Fills for PRE-EXISTING orders (order was created before this range).
         *
         * Detection: LEFT JOIN to create_keys, WHERE create_keys IS NULL
         * This gives us fills that have no corresponding create in this range.
         *
         * We aggregate all fills per order because we need total amount filled
         * to calculate new remaining balance.
         */
        SELECT
            f.owner_id,
            f.order_id,
            f.nai,
            SUM(f.fill_amount)::bigint AS sum_fill
        FROM fills f
        LEFT JOIN create_keys k USING (owner_id, order_id)
        WHERE k.owner_id IS NULL  -- No create found = pre-existing order
        GROUP BY f.owner_id, f.order_id, f.nai
    ),
    pre_calc AS MATERIALIZED (
        /*
         * Calculate new remaining amounts for pre-existing orders after fills.
         *
         * Formula: new_remaining = current_remaining - sum_of_fills
         * GREATEST(..., 0) prevents negative values (defensive, shouldn't happen)
         *
         * Filters:
         *   - sum_fill <> 0: Skip if no actual fill amount
         *   - remaining <> new_remaining: Skip if no change (optimization)
         *
         * Example:
         *   Order has remaining=500, fills sum to 200
         *   -> new_remaining = 500 - 200 = 300
         */
        SELECT
            s.owner_id,
            s.order_id,
            s.nai,
            s.remaining,
            pf.sum_fill,
            GREATEST(s.remaining - pf.sum_fill, 0)::bigint AS new_remaining
        FROM order_state s
        JOIN pre_fills pf
          ON (s.owner_id, s.order_id, s.nai) = (pf.owner_id, pf.order_id, pf.nai)
        WHERE pf.sum_fill <> 0
          AND s.remaining <> GREATEST(s.remaining - pf.sum_fill, 0)::bigint
    ),
    del_pre AS (
        /*
         * Pre-existing orders that are now FULLY FILLED (remaining <= 0).
         * These orders should be deleted from order_state.
         *
         * Example: Order had remaining=100, got filled for 100 -> delete it
         */
        SELECT owner_id, order_id
        FROM pre_calc
        WHERE new_remaining <= 0
    ),
    pre_canceled AS (
        /*
         * Pre-existing orders that were CANCELLED in this block range.
         *
         * Detection: Cancel event exists but no create in this range.
         * These should be deleted from order_state regardless of remaining amount.
         */
        SELECT DISTINCT c.owner_id, c.order_id
        FROM cancels c
        LEFT JOIN create_keys k USING (owner_id, order_id)
        WHERE k.owner_id IS NULL  -- No create found = pre-existing order
    ),

    /*
     * =========================================================================
     * PHASE 3: Handle NEW orders (created IN this block range)
     * =========================================================================
     *
     * "Squashing" concept: Within a single block range, an order might be:
     *   created -> partially filled -> filled again -> cancelled
     *
     * We need to compute the FINAL STATE at end of range, not intermediate states.
     * This is done by finding the latest create and applying only subsequent events.
     */
    latest_creates AS MATERIALIZED (
        /*
         * For each (owner_id, order_id, nai), keep only the LATEST create.
         *
         * Why might there be multiple creates for same order_id?
         * User can: create order #5 -> cancel it -> create new order #5
         * The old create is obsolete; we only care about the most recent one.
         *
         * ORDER BY create_op_id DESC + DISTINCT ON keeps highest op_id (latest).
         */
        SELECT DISTINCT ON (owner_id, order_id, nai)
               owner_id,
               order_id,
               nai,
               create_amount,
               create_block,
               create_op_id
        FROM creates
        ORDER BY owner_id, order_id, nai, create_op_id DESC
    ),
    fills_after_latest AS (
        /*
         * Sum fills that occurred AFTER the latest create for each order.
         *
         * Critical: f.fill_op_id > c.create_op_id ensures we only count fills
         * that happened after the order was created, not fills from a previous
         * incarnation of the same order_id.
         *
         * Example scenario in same block range:
         *   op_id 100: Alice creates order #5 for 1000 HIVE
         *   op_id 101: Order #5 fills 200 HIVE
         *   op_id 102: Alice cancels order #5
         *   op_id 103: Alice creates order #5 for 500 HIVE (reusing ID)
         *   op_id 104: Order #5 fills 100 HIVE
         *
         * For latest create (op_id 103), we only count fill at op_id 104 (100 HIVE).
         * Fills at op_id 101 are ignored because they belong to old order #5.
         */
        SELECT
            c.owner_id,
            c.order_id,
            c.nai,
            COALESCE(SUM(f.fill_amount), 0)::bigint AS sum_fill_after
        FROM latest_creates c
        LEFT JOIN fills f
          ON (f.owner_id, f.order_id, f.nai) = (c.owner_id, c.order_id, c.nai)
         AND f.fill_op_id > c.create_op_id
        GROUP BY c.owner_id, c.order_id, c.nai
    ),
    canceled_after_latest AS (
        /*
         * Orders that were cancelled AFTER their latest create.
         *
         * Same logic as fills_after_latest: only count cancels that occurred
         * after the most recent create (k.cancel_op_id > c.create_op_id).
         *
         * These orders should not appear in final state regardless of remaining.
         */
        SELECT DISTINCT c.owner_id, c.order_id
        FROM latest_creates c
        JOIN cancels k
          ON (k.owner_id, k.order_id) = (c.owner_id, c.order_id)
         AND k.cancel_op_id > c.create_op_id
    ),
    remaining_calc AS MATERIALIZED (
        /*
         * Calculate remaining amount for each newly created order.
         *
         * Formula: remaining = create_amount - sum_of_fills_after_create
         *
         * Example:
         *   Created with 1000 HIVE, filled 300 + 200 = 500
         *   -> remaining = 1000 - 500 = 500 HIVE
         */
        SELECT
            c.owner_id,
            c.order_id,
            c.nai,
            (c.create_amount - fal.sum_fill_after)::bigint AS remaining,
            c.create_block AS block_created
        FROM latest_creates c
        JOIN fills_after_latest fal
          ON (fal.owner_id, fal.order_id, fal.nai) = (c.owner_id, c.order_id, c.nai)
    ),

    /*
     * =========================================================================
     * PHASE 4: Determine SURVIVORS (orders that should exist in final state)
     * =========================================================================
     */
    survivors AS (
        /*
         * Orders from this range that should remain OPEN after processing.
         *
         * Survival criteria:
         *   1. remaining > 0 (not fully filled)
         *   2. NOT in canceled_after_latest (not cancelled)
         *
         * DISTINCT ON (owner_id, order_id) is defensive programming against potential
         * data inconsistencies (an order should only ever have one nai value).
         * ORDER BY block_created DESC, nai DESC keeps most recent version.
         */
        SELECT DISTINCT ON (rc.owner_id, rc.order_id)
               rc.owner_id,
               rc.order_id,
               rc.nai,
               rc.remaining,
               rc.block_created
        FROM remaining_calc rc
        LEFT JOIN canceled_after_latest cal ON (cal.owner_id, cal.order_id) = (rc.owner_id, rc.order_id)
        WHERE rc.remaining > 0 AND cal.owner_id IS NULL
        ORDER BY rc.owner_id, rc.order_id, rc.block_created DESC, rc.nai DESC
    ),

    /*
     * =========================================================================
     * PHASE 5: Apply changes to order_state table
     * =========================================================================
     */
    ins_new AS (
        /*
         * UPSERT survivors into order_state.
         *
         * ON CONFLICT handles orders that already exist in order_state from
         * previous block range processing runs.
         * Uses IS DISTINCT FROM to avoid unnecessary writes when values unchanged.
         *
         * Why upsert instead of separate insert/update?
         *   - Simpler logic: one statement handles both new orders and updates
         *   - Atomic: no race conditions between check and modify
         *   - Efficient: PostgreSQL optimizes this pattern well
         */
        INSERT INTO order_state (owner_id, order_id, nai, remaining, block_created)
        SELECT owner_id, order_id, nai, remaining, block_created
        FROM survivors
        ON CONFLICT (owner_id, order_id) DO UPDATE
          SET nai = EXCLUDED.nai,
              remaining = EXCLUDED.remaining,
              block_created = EXCLUDED.block_created
          WHERE (order_state.nai,
                 order_state.remaining,
                 order_state.block_created)
                IS DISTINCT FROM (EXCLUDED.nai,
                                   EXCLUDED.remaining,
                                   EXCLUDED.block_created)
        RETURNING 1
    ),
    del_keys AS (
        /*
         * Collect ALL orders that should be DELETED from order_state.
         *
         * Four sources of deletions:
         *   1. del_pre: Pre-existing orders fully filled (remaining <= 0)
         *   2. pre_canceled: Pre-existing orders that were cancelled
         *   3. canceled_after_latest: New orders created then cancelled in same range
         *   4. remaining_calc WHERE remaining <= 0: New orders fully filled in same range
         *
         * Example of #4: Order created for 100 HIVE, immediately filled for 100 HIVE
         * -> Created and fully consumed in same block range, never stored.
         */
        SELECT owner_id, order_id FROM del_pre
        UNION ALL
        SELECT owner_id, order_id FROM pre_canceled
        UNION ALL
        SELECT owner_id, order_id FROM canceled_after_latest
        UNION ALL
        SELECT owner_id, order_id FROM remaining_calc WHERE remaining <= 0
    ),
    to_delete AS (
        /*
         * Final deletion list, excluding survivors.
         *
         * Why exclude survivors? An order might appear in both:
         *   - del_keys (e.g., from an old cancelled order #5)
         *   - survivors (from a NEW order #5 created after the cancel)
         *
         * The LEFT JOIN + WHERE s.owner_id IS NULL ensures we don't delete
         * orders that should actually remain (the new incarnation).
         */
        SELECT DISTINCT d.owner_id, d.order_id
        FROM del_keys d
        LEFT JOIN survivors s USING (owner_id, order_id)
        WHERE s.owner_id IS NULL
    ),
    del_any AS (
        /*
         * Execute deletions for orders no longer active.
         * RETURNING 1 allows counting affected rows for diagnostics.
         */
        DELETE FROM order_state s
        USING to_delete d
        WHERE (s.owner_id, s.order_id) = (d.owner_id, d.order_id)
        RETURNING 1
    ),
    upd_pre AS (
        /*
         * Update remaining amounts for pre-existing orders that were PARTIALLY filled.
         *
         * This handles orders that:
         *   - Existed before this block range (pre-existing)
         *   - Got partially filled (new_remaining > 0)
         *   - Value actually changed (s.remaining <> pc.new_remaining)
         *
         * Orders fully filled (new_remaining <= 0) are handled by del_any instead.
         */
        UPDATE order_state s
           SET remaining = pc.new_remaining
          FROM pre_calc pc
         WHERE (s.owner_id, s.order_id, s.nai) = (pc.owner_id, pc.order_id, pc.nai)
           AND pc.new_remaining > 0
           AND s.remaining <> pc.new_remaining
        RETURNING 1
    )
    /*
     * Collect row counts for diagnostics/logging.
     * These counters track: inserts/updates of new orders, deletions, pre-existing updates.
     */
    SELECT
        COALESCE((SELECT COUNT(*) FROM ins_new), 0),
        COALESCE((SELECT COUNT(*) FROM del_any), 0),
        COALESCE((SELECT COUNT(*) FROM upd_pre), 0)
    INTO __ins_new, __del_any, __upd_pre;
END;
$func$;

RESET ROLE;

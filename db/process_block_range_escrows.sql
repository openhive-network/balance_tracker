SET ROLE btracker_owner;

/*
 * Processes escrow operations within a block range, maintaining escrow_state and escrow_fees tables.
 * Escrow lifecycle: transfer (create) -> approve/dispute -> release (partial/full) or reject.
 * Uses "squashing" pattern to compute final state efficiently during fast sync.
 */
CREATE OR REPLACE FUNCTION process_block_range_escrows(
    IN _from INT,
    IN _to   INT,
    IN _report_step INT DEFAULT 1000
)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
/*
 * Planner settings for complex CTE queries:
 *   - from_collapse_limit/join_collapse_limit = 16: Allow planner to reorder up to 16 tables
 *     for optimal join order. Default (8) is too low for this many CTEs.
 *   - jit = OFF: JIT compilation adds startup overhead that exceeds benefits for this
 *     pattern of many small CTEs with simple operations.
 */
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$func$
DECLARE
  /*
   * Cache operation type IDs to avoid repeated function calls.
   * These map to hive operation types:
   *   - escrow_transfer: Creates a new escrow with HIVE/HBD amounts and agent fee
   *   - escrow_release: Releases funds (partial or full) from escrow to recipient
   *   - escrow_approved: Agent or receiver approves the escrow (fee is consumed)
   *   - escrow_rejected: Agent or receiver rejects, returning funds to sender
   *   - escrow_dispute: Receiver or sender raises a dispute, requiring agent intervention
   */
  __op_transfer INT := btracker_backend.op_escrow_transfer();
  __op_release  INT := btracker_backend.op_escrow_release();
  __op_approved INT := btracker_backend.op_escrow_approved();
  __op_rejected INT := btracker_backend.op_escrow_rejected();
  __op_dispute  INT := btracker_backend.op_escrow_dispute();
  __ins_escrows INT := 0;
  __del_escrows INT := 0;
  __ins_fee     INT := 0;
  __del_fee     INT := 0;
BEGIN
  /* Process escrow operations in the specified block range
    - escrow_transfer_operation - creates an escrow and fee
    - escrow_approved_operation - deletes the fee, sets to_approved = TRUE
    - escrow_dispute_operation  - sets disputed = TRUE
    - escrow_release_operation  - deletes or reduces the escrow amounts
    - escrow_rejected_operation - deletes the escrow completely
  */

  WITH ops_in_range AS (
    /*
     * Fetch all escrow-related operations in this block range.
     * NOT MATERIALIZED here because it's a simple filter that benefits from
     * predicate pushdown into operations_view.
     *
     * Escrow operations captured:
     *   - escrow_transfer: User creates escrow (sender, receiver, agent, amounts, fee)
     *   - escrow_release: Funds released from escrow (can be partial)
     *   - escrow_approved: Agent/receiver approves, activating the escrow
     *   - escrow_rejected: Agent/receiver rejects before approval deadline
     *   - escrow_dispute: Party raises dispute after escrow is active
     */
    SELECT ov.id AS op_id, ov.op_type_id, ov.body
    FROM operations_view ov
    WHERE
      ov.block_num BETWEEN _from AND _to AND
      ov.op_type_id IN (__op_transfer, __op_release, __op_approved, __op_rejected, __op_dispute)
  ),
  --------------------- BATCH ACCOUNT LOOKUPS ---------------------
  /*
   * Extract unique account names for efficient bulk ID lookup.
   * Escrows are keyed by (from_id, escrow_id) where from_id is the sender's account ID.
   * We extract the 'from' field which identifies the escrow creator/owner.
   */
  all_account_names AS (
    SELECT DISTINCT (o.body->'value'->>'from')::TEXT AS account_name
    FROM ops_in_range o
  ),
  account_ids AS MATERIALIZED (
    /*
     * MATERIALIZED: Forces PostgreSQL to compute account lookups ONCE and reuse.
     * Without this, the planner might re-execute this subquery for each CTE that
     * references it, causing repeated scans of accounts_view.
     *
     * This is critical for performance: accounts_view can be large, and we need
     * these IDs in almost every subsequent CTE.
     */
    SELECT av.name AS account_name, av.id AS account_id
    FROM accounts_view av
    WHERE av.name IN (SELECT account_name FROM all_account_names)
  ),
  --------------------- GET ESCROW EVENTS ---------------------
  escrow_transfers AS MATERIALIZED (
    /*
     * Parse escrow_transfer operations into structured data.
     * Each transfer creates a new escrow with:
     *   - from_id: Creator/sender account
     *   - escrow_id: Per-account unique ID (user specifies this)
     *   - hive_amount/hbd_amount: Funds locked in escrow (can be both or one)
     *
     * MATERIALIZED because this result is used by multiple downstream CTEs
     * (create_transfers_in_range, releases_related_to_in_range_transfers).
     *
     * Example: Alice creates escrow #1 to Bob with agent Carol, locking 100 HIVE + 50 HBD
     */
    SELECT
      o.op_id,
      ai.account_id AS from_id,
      e.escrow_id,
      e.hive_nai,
      e.hive_amount,
      e.hbd_nai,
      e.hbd_amount
    FROM ops_in_range o
    CROSS JOIN btracker_backend.get_escrow_transfers(o.body) AS e
    JOIN account_ids ai ON ai.account_name = e.from_name
    WHERE o.op_type_id = __op_transfer
  ),
  escrow_releases AS MATERIALIZED (
    /*
     * Parse escrow_release operations.
     * A release reduces escrow balances - can be partial or full.
     * Amounts are NEGATIVE (subtracted from escrow balance).
     *
     * Release scenarios:
     *   - Full release to receiver: All funds go to 'to' account
     *   - Partial release: Some funds released, escrow continues
     *   - Split release (after dispute): Agent splits between sender/receiver
     *
     * MATERIALIZED: Used by both in-range and pre-range release calculations.
     *
     * Example: Agent releases 60 HIVE to receiver, 40 HIVE back to sender (after dispute)
     */
    SELECT
      o.op_id,
      ai.account_id AS from_id,
      e.escrow_id,
      e.hive_nai,
      e.hive_amount,
      e.hbd_nai,
      e.hbd_amount
    FROM ops_in_range o
    CROSS JOIN btracker_backend.get_escrow_release(o.body) AS e
    JOIN account_ids ai ON ai.account_name = e.from_name
    WHERE o.op_type_id = __op_release
  ),
  escrow_fees AS MATERIALIZED (
    /*
     * Parse escrow fee information from transfer operations.
     * Fee is paid to the agent for their escrow services.
     * Fee exists from creation until approval (then consumed) or rejection (returned).
     *
     * MATERIALIZED: Referenced by unique_escrow_fees and filter_out_deleted_fees.
     *
     * Example: Alice creates escrow with 1 HBD fee for agent Carol
     */
    SELECT
      o.op_id,
      ai.account_id AS from_id,
      e.escrow_id,
      e.nai,
      e.amount
    FROM ops_in_range o
    CROSS JOIN btracker_backend.get_escrow_fees(o.body) AS e
    JOIN account_ids ai ON ai.account_name = e.from_name
    WHERE o.op_type_id = __op_transfer
  ),
  escrow_rejects AS MATERIALIZED (
    /*
     * Parse escrow_rejected operations.
     * Rejection cancels the escrow entirely - all funds return to sender.
     * Can only happen BEFORE the escrow is approved (ratification deadline).
     *
     * MATERIALIZED: Used by unique_escrow_rejects which feeds into multiple CTEs.
     *
     * Example: Agent Carol rejects escrow #1 - Alice gets all funds back
     */
    SELECT
      o.op_id,
      ai.account_id AS from_id,
      e.escrow_id
    FROM ops_in_range o
    CROSS JOIN btracker_backend.get_escrow_rejects (o.body) AS e
    JOIN account_ids ai ON ai.account_name = e.from_name
    WHERE o.op_type_id = __op_rejected
  ),
  escrow_approved AS MATERIALIZED (
    /*
     * Parse escrow_approved operations.
     * Both agent AND receiver must approve for escrow to become active.
     * When fully approved:
     *   - Fee is transferred to agent (deleted from escrow_fees table)
     *   - to_approved flag set TRUE in escrow_state
     *   - Escrow can now only be released (not rejected)
     *
     * MATERIALIZED: Used by unique_escrow_approves for both state updates and fee deletions.
     *
     * Example: Bob (receiver) approves escrow #1, then Carol (agent) approves
     */
    SELECT
      o.op_id,
      ai.account_id AS from_id,
      e.escrow_id
    FROM ops_in_range o
    CROSS JOIN btracker_backend.get_escrow_approves(o.body) AS e
    JOIN account_ids ai ON ai.account_name = e.from_name
    WHERE o.op_type_id = __op_approved
  ),
  escrow_disputes AS MATERIALIZED (
    /*
     * Parse escrow_dispute operations.
     * A dispute can be raised by sender or receiver AFTER escrow is approved.
     * When disputed:
     *   - Only the AGENT can release funds (not sender/receiver)
     *   - Agent decides how to split funds between parties
     *
     * Uses direct JSON extraction instead of helper function (simpler case).
     *
     * MATERIALIZED: Used by unique_escrow_disputes for state updates.
     *
     * Example: Bob disputes escrow #1 because Alice didn't deliver goods
     */
    SELECT
      o.op_id,
      ai.account_id AS from_id,
      (o.body->'value'->>'escrow_id')::BIGINT AS escrow_id
    FROM ops_in_range o
    JOIN account_ids ai ON ai.account_name = (o.body->'value'->>'from')::TEXT
    WHERE o.op_type_id = __op_dispute
  ),
  --------------------- UNIQUE DISPUTES ---------------------
  unique_escrow_disputes AS MATERIALIZED (
    /*
     * Get the LATEST dispute per escrow.
     * In practice, each escrow can only be disputed once, but we use DISTINCT ON
     * defensively to handle edge cases and ensure deterministic results.
     *
     * ORDER BY op_id DESC: If multiple disputes somehow exist, keep the latest.
     *
     * Result feeds into EXISTS check when inserting escrow_state.disputed flag.
     */
    SELECT DISTINCT ON (ed.from_id, ed.escrow_id)
      ed.from_id,
      ed.escrow_id,
      ed.op_id
    FROM escrow_disputes ed
    ORDER BY ed.from_id, ed.escrow_id, ed.op_id DESC
  ),
  --------------------- UNIQUE FEES ---------------------
  unique_escrow_fees AS MATERIALIZED (
    /*
     * Get the LATEST fee per escrow.
     * Fees are created with escrow_transfer and deleted on approval or rejection.
     *
     * Why take latest? Within a block range, an escrow might be:
     *   created (fee1) -> rejected -> created again (fee2)
     * We only care about the current state (fee2).
     *
     * ORDER BY op_id DESC: Keep the most recent fee definition.
     */
    SELECT DISTINCT ON (er.from_id, er.escrow_id)
      er.from_id,
      er.escrow_id,
      er.nai,
      er.amount,
      er.op_id
    FROM escrow_fees er
    ORDER BY er.from_id, er.escrow_id, er.op_id DESC
  ),
  --------------------- UNIQUE REJECTS ---------------------
  unique_escrow_rejects AS MATERIALIZED (
    /*
     * Get the LATEST rejection per escrow.
     * A rejection cancels the escrow - funds return to sender.
     *
     * MATERIALIZED: This is a key CTE used by multiple downstream CTEs to filter
     * out rejected escrows from transfers and releases.
     *
     * ORDER BY op_id DESC: Ensure we have the authoritative rejection event.
     */
    SELECT DISTINCT ON (er.from_id, er.escrow_id)
      er.from_id,
      er.escrow_id,
      er.op_id
    FROM escrow_rejects er
    ORDER BY er.from_id, er.escrow_id, er.op_id DESC
  ),
  --------------------- UNIQUE APPROVES ---------------------
  unique_escrow_approves AS MATERIALIZED (
    /*
     * Get the LATEST approval per escrow.
     * An approval activates the escrow and consumes the agent fee.
     *
     * Note: Both agent and receiver must approve, but this operation fires for each.
     * We track "to_approved" flag which means receiver has approved.
     *
     * MATERIALIZED: Used for both state updates and fee deletion logic.
     */
    SELECT DISTINCT ON (er.from_id, er.escrow_id)
      er.from_id,
      er.escrow_id,
      er.op_id
    FROM escrow_approved er
    ORDER BY er.from_id, er.escrow_id, er.op_id DESC
  ),
  --------------------- TRANSFERS CREATED IN-RANGE ---------------------
  /*
   * =========================================================================
   * "SQUASHING" PATTERN FOR IN-RANGE TRANSFERS
   * =========================================================================
   *
   * During fast sync, we process thousands of blocks at once. Within this range,
   * an escrow might go through its entire lifecycle:
   *   Block N:   Alice creates escrow #1 with 100 HIVE
   *   Block N+5: Bob approves escrow #1
   *   Block N+10: Alice releases 50 HIVE to Bob
   *   Block N+15: Alice releases remaining 50 HIVE
   *
   * Instead of inserting/updating/deleting for each operation, we "squash"
   * these into the FINAL STATE at the end of the range. This is much faster.
   *
   * The following CTEs implement this squashing logic for in-range transfers.
   */
  create_transfers_in_range AS MATERIALIZED (
    /*
     * Get the LATEST transfer per (from_id, escrow_id).
     *
     * Why latest? Same escrow_id can be reused after rejection/completion.
     * Example in same block range:
     *   op_id 100: Alice creates escrow #1 for 100 HIVE
     *   op_id 110: Escrow #1 rejected
     *   op_id 120: Alice creates escrow #1 again for 200 HIVE (reusing ID)
     *
     * We only care about the escrow #1 created at op_id 120.
     *
     * DISTINCT ON + ORDER BY op_id DESC: Keep most recent transfer per escrow key.
     */
    SELECT DISTINCT ON (from_id, escrow_id)
      from_id,
      escrow_id,
      hive_nai,
      hive_amount,
      hbd_nai,
      hbd_amount,
      op_id
    FROM escrow_transfers
    -- latest transfer per (from_id, escrow_id)
    ORDER BY from_id, escrow_id, op_id DESC
  ),
  filter_out_rejected_transfers AS MATERIALIZED (
    /*
     * Remove transfers that were REJECTED within this block range.
     *
     * A rejection completely cancels the escrow - it should not exist in final state.
     *
     * Logic: LEFT JOIN to rejects, keep only where NO rejection exists (er.from_id IS NULL)
     * or rejection happened BEFORE the transfer (er.op_id <= ct.op_id, meaning old escrow).
     *
     * The condition er.op_id > ct.op_id ensures we only consider rejections that
     * happened AFTER this specific transfer was created.
     */
    SELECT
      ct.*
    FROM create_transfers_in_range ct
    LEFT JOIN unique_escrow_rejects er ON
      er.from_id = ct.from_id AND
      er.escrow_id = ct.escrow_id AND
      er.op_id > ct.op_id
    WHERE er.from_id IS NULL
  ),
  releases_related_to_in_range_transfers AS (
    /*
     * Find all releases that apply to transfers created IN this block range.
     *
     * Critical: er.op_id > frt.op_id ensures we only count releases that occurred
     * AFTER the transfer was created. Releases for previous incarnations of the
     * same escrow_id are ignored.
     *
     * Example:
     *   op_id 100: Create escrow #1 for 100 HIVE
     *   op_id 105: Release 30 HIVE from escrow #1
     *   op_id 110: Release 20 HIVE from escrow #1
     *   -> Sum of releases: -50 HIVE (amounts are negative)
     */
    SELECT
      er.op_id,
      er.from_id,
      er.escrow_id,
      er.hive_nai,
      er.hive_amount,
      er.hbd_nai,
      er.hbd_amount
    FROM escrow_releases er
    WHERE EXISTS (
      SELECT 1
      FROM filter_out_rejected_transfers frt
      WHERE
        frt.from_id = er.from_id AND
        frt.escrow_id = er.escrow_id AND
        er.op_id > frt.op_id
    )
  ),
  union_in_range_transfers_with_related_releases AS (
    /*
     * Combine transfers (positive amounts) with releases (negative amounts).
     *
     * This UNION ALL prepares data for aggregation. Transfers add funds to escrow,
     * releases subtract them. By summing, we get the net balance.
     *
     * Example:
     *   Transfer: +100 HIVE, +50 HBD
     *   Release 1: -30 HIVE, 0 HBD
     *   Release 2: -20 HIVE, -50 HBD
     *   -> Net: 50 HIVE, 0 HBD
     */
    SELECT
      from_id,
      escrow_id,
      hive_nai,
      hive_amount,
      hbd_nai,
      hbd_amount,
      op_id
    FROM filter_out_rejected_transfers
    UNION ALL
    SELECT
      from_id,
      escrow_id,
      hive_nai,
      hive_amount,
      hbd_nai,
      hbd_amount,
      op_id
    FROM releases_related_to_in_range_transfers
  ),
  aggregate_in_range_transfers AS (
    /*
     * Calculate final amounts for each escrow after all in-range operations.
     *
     * SUM aggregates the transfer (positive) and releases (negative) to get net balance.
     * MIN(op_id) captures the original transfer creation op_id for source_op tracking.
     * MAX(nai) works because there's only one NAI per column - just picks the non-null.
     *
     * Example result: Escrow #1 has 50 HIVE remaining after releases.
     */
    SELECT
      uit.from_id,
      uit.escrow_id,
      MAX(uit.hive_nai) AS hive_nai,
      SUM(uit.hive_amount) AS hive_amount,
      MAX(uit.hbd_nai) AS hbd_nai,
      SUM(uit.hbd_amount) AS hbd_amount,
      MIN(uit.op_id) AS op_id -- earliest op_id among the group is transfer creation
    FROM union_in_range_transfers_with_related_releases uit
    GROUP BY uit.from_id, uit.escrow_id
  ),
  filter_out_released_transfers AS (
    /*
     * Remove escrows that were FULLY RELEASED within this block range.
     *
     * If both hive_amount = 0 AND hbd_amount = 0, the escrow is complete
     * and should not be inserted into escrow_state.
     *
     * This is the "squashing" optimization: instead of INSERT then DELETE,
     * we simply never insert fully-consumed escrows.
     *
     * Example: Escrow created for 100 HIVE, fully released in same range -> skip it
     */
    SELECT
      ait.*
    FROM aggregate_in_range_transfers ait
    WHERE NOT (ait.hive_amount = 0 AND ait.hbd_amount = 0)
  ),
  --------------------- PREPARE REJECTIONS ---------------------
  /*
   * =========================================================================
   * PRE-RANGE ESCROW HANDLING
   * =========================================================================
   *
   * Pre-range escrows were created in a previous block range and already exist
   * in escrow_state. We need to handle:
   *   1. Rejections to pre-range escrows -> DELETE from escrow_state
   *   2. Releases to pre-range escrows -> UPDATE remaining amounts
   *   3. Full releases that zero out -> DELETE from escrow_state
   */
  rejections_to_pre_range_transfers AS (
    /*
     * Find rejections that apply to PRE-EXISTING escrows (created before this range).
     *
     * Detection: A rejection is for a pre-range escrow if there's NO transfer for
     * that escrow in this range (LEFT JOIN IS NULL), OR the transfer in this range
     * came AFTER the rejection (meaning old escrow).
     *
     * These escrows must be deleted from escrow_state.
     */
    SELECT
      er.from_id,
      er.escrow_id,
      er.op_id
    FROM unique_escrow_rejects er
    -- exclude rejections that relate to transfers created in the range
    LEFT JOIN create_transfers_in_range frt ON
      frt.from_id = er.from_id AND
      frt.escrow_id = er.escrow_id AND
      frt.op_id > er.op_id
    WHERE frt.from_id IS NULL
  ),
  --------------------- RELEASES PRE-RANGE CREATED TRANSFERS---------------------
  releases_to_pre_range_transfers AS (
    /*
     * Find releases that apply to PRE-EXISTING escrows.
     *
     * Detection: A release is for a pre-range escrow if there's NO matching
     * transfer in create_transfers_in_range (LEFT JOIN IS NULL).
     *
     * These releases need to be aggregated and applied to existing escrow_state rows.
     */
    SELECT
      er.from_id,
      er.escrow_id,
      er.hive_nai,
      er.hive_amount,
      er.hbd_nai,
      er.hbd_amount,
      er.op_id
    FROM escrow_releases er
    -- only those releases that relate to transfers created before the range (not in create_transfers_in_range)
    LEFT JOIN create_transfers_in_range frt ON
      frt.from_id = er.from_id AND frt.escrow_id = er.escrow_id
    WHERE frt.from_id IS NULL
  ),
  filter_out_rejected_releases AS (
    /*
     * Remove releases for escrows that were REJECTED.
     *
     * If an escrow was rejected, any releases to it are moot - the escrow
     * is gone. We filter these out to avoid processing them.
     *
     * Logic: Keep releases only where no rejection exists after the release.
     */
    SELECT
      rt.*
    FROM releases_to_pre_range_transfers rt
    LEFT JOIN unique_escrow_rejects er ON
      er.from_id = rt.from_id AND
      er.escrow_id = rt.escrow_id AND
      er.op_id > rt.op_id
    WHERE er.from_id IS NULL
  ),
  aggregate_releases_to_pre_range_transfers AS (
    /*
     * Sum all releases per pre-existing escrow.
     *
     * Release amounts are negative, so summing gives total reduction.
     * Example: Two releases of -30 and -20 -> sum = -50
     */
    SELECT
      frt.from_id,
      frt.escrow_id,
      SUM(frt.hive_amount) AS hive_amount,
      SUM(frt.hbd_amount) AS hbd_amount
    FROM filter_out_rejected_releases frt
    GROUP BY frt.from_id, frt.escrow_id
  ),
  join_escrow_state_pre_range AS MATERIALIZED (
    /*
     * Calculate NEW remaining amounts for pre-existing escrows after releases.
     *
     * JOIN to escrow_state gets current amounts, then ADD release sums (which are negative).
     * Example: Current = 100 HIVE, releases sum = -50 -> new amount = 50 HIVE
     *
     * MATERIALIZED: This result feeds into both union_deletions (for zeroed escrows)
     * and union_inserts (for escrows with remaining balance).
     *
     * We preserve source_op from existing escrow_state (the original creation op).
     */
    SELECT
      ar.from_id,
      ar.escrow_id,
      es.hive_nai,
      (es.hive_amount + ar.hive_amount) AS hive_amount,
      es.hbd_nai,
      (es.hbd_amount + ar.hbd_amount) AS hbd_amount,
      es.source_op AS op_id -- keep first op_id from escrow_state
    FROM aggregate_releases_to_pre_range_transfers ar
    JOIN escrow_state es ON
      es.from_id = ar.from_id AND
      es.escrow_id = ar.escrow_id
  ),
  --------------------- PREPARE ESCROW DELETIONS ---------------------
  union_deletions AS MATERIALIZED (
    /*
     * Collect ALL escrows that should be DELETED from escrow_state.
     *
     * Two sources of deletions:
     *   1. rejections_to_pre_range_transfers: Pre-existing escrows that were rejected
     *   2. join_escrow_state_pre_range WHERE amounts = 0: Fully released pre-existing escrows
     *
     * Note: In-range escrows that are fully released never get inserted (filtered earlier).
     *
     * MATERIALIZED: Used by both delete_escrows and union_fee_deletions.
     */
    SELECT
      urr.from_id,
      urr.escrow_id,
      urr.op_id
    FROM rejections_to_pre_range_transfers urr
    UNION ALL
    SELECT
      jesp.from_id,
      jesp.escrow_id,
      jesp.op_id
    FROM join_escrow_state_pre_range jesp
    WHERE jesp.hive_amount = 0 AND jesp.hbd_amount = 0
  ),
  --------------------- PREPARE ESCROW INSERTS ---------------------
  union_inserts AS (
    /*
     * Collect ALL escrows that should be INSERTED/UPDATED in escrow_state.
     *
     * Two sources:
     *   1. filter_out_released_transfers: New escrows created in this range (with balance > 0)
     *   2. join_escrow_state_pre_range: Pre-existing escrows with updated balances (> 0)
     *
     * Both are upserted using ON CONFLICT pattern in insert_escrows.
     */
    SELECT
      frit.from_id,
      frit.escrow_id,
      frit.hive_nai,
      frit.hive_amount,
      frit.hbd_nai,
      frit.hbd_amount,
      frit.op_id
    FROM filter_out_released_transfers frit
    UNION ALL
    SELECT
      jesp.from_id,
      jesp.escrow_id,
      jesp.hive_nai,
      jesp.hive_amount,
      jesp.hbd_nai,
      jesp.hbd_amount,
      jesp.op_id
    FROM join_escrow_state_pre_range jesp
    WHERE jesp.hive_amount > 0 OR jesp.hbd_amount > 0
  ),
  --------------------- PREPARE FEE ---------------------
  /*
   * =========================================================================
   * FEE HANDLING
   * =========================================================================
   *
   * Escrow fees are paid to agents for their services. Fee lifecycle:
   *   - Created with escrow_transfer (held in escrow_fees table)
   *   - Deleted when escrow_approved (fee paid to agent)
   *   - Deleted when escrow_rejected (fee returned to sender)
   *   - Deleted when escrow fully released/deleted
   */
  not_approved_fees AS (
    /*
     * Fees that have NOT been approved (consumed) within this block range.
     *
     * A fee is "not approved" if there's no approval event AFTER the fee was created.
     * If approved, the fee goes to the agent and should be deleted from escrow_fees.
     *
     * Logic: LEFT JOIN to approvals, keep where no approval exists after fee creation.
     */
    SELECT
      ef.from_id,
      ef.escrow_id,
      ef.nai,
      ef.amount
    FROM unique_escrow_fees ef
    LEFT JOIN unique_escrow_approves ea ON
      ea.from_id = ef.from_id AND
      ea.escrow_id = ef.escrow_id AND
      ea.op_id > ef.op_id
    WHERE ea.from_id IS NULL
  ),
  filter_out_deleted_fees AS (
    /*
     * Remove fees for escrows that are being deleted.
     *
     * If the escrow itself is being deleted (rejection or full release),
     * the fee record should also be deleted, not inserted.
     *
     * This prevents orphan fee records.
     */
    SELECT
      uf.*
    FROM not_approved_fees uf
    LEFT JOIN union_deletions ud ON
      ud.from_id = uf.from_id AND
      ud.escrow_id = uf.escrow_id
    WHERE ud.from_id IS NULL
  ),
  --------------------- PREPARE FEE DELETIONS  ---------------------
  fee_approved_pre_range AS (
    /*
     * Find approvals for PRE-EXISTING escrows (no fee created in this range).
     *
     * When a pre-existing escrow gets approved, its fee (in escrow_fees table)
     * should be deleted because the agent has been paid.
     *
     * Detection: Approval exists but no fee was created in this range after it.
     */
    SELECT
      ea.from_id,
      ea.escrow_id
    FROM unique_escrow_approves ea
    LEFT JOIN unique_escrow_fees ef ON
      ef.from_id = ea.from_id AND
      ef.escrow_id = ea.escrow_id AND
      ef.op_id > ea.op_id
    WHERE ef.from_id IS NULL
  ),
  union_fee_deletions AS (
    /*
     * Collect ALL fees that should be DELETED from escrow_fees.
     *
     * Two sources of fee deletions:
     *   1. fee_approved_pre_range: Pre-existing escrow approved -> fee goes to agent
     *   2. union_deletions: Escrow rejected or fully released -> fee returned/removed
     *
     * Note: Fees for new escrows approved in same range are never inserted (filtered earlier).
     */
    SELECT
      uap.from_id,
      uap.escrow_id
    FROM fee_approved_pre_range uap
    UNION ALL
    SELECT
      ud.from_id,
      ud.escrow_id
    FROM union_deletions ud

  ),
  unique_fee_deletions AS (
    /*
     * Deduplicate fee deletions.
     *
     * An escrow might appear in multiple deletion sources (e.g., both approved
     * and rejected in weird edge cases). DISTINCT ensures single delete attempt.
     */
    SELECT DISTINCT ON (ufd.from_id, ufd.escrow_id)
      ufd.from_id,
      ufd.escrow_id
    FROM union_fee_deletions ufd
  ),
  --------------------- FINAL INSERT/UPDATE/DELETE ---------------------
  insert_escrows AS (
    /*
     * UPSERT escrows into escrow_state.
     *
     * ON CONFLICT pattern handles both new escrows and updates to existing ones:
     *   - New escrows: INSERT with all values
     *   - Existing escrows: UPDATE amounts and flags
     *
     * Flag handling on conflict:
     *   - to_approved: OR logic preserves TRUE if ever approved (can't un-approve)
     *   - disputed: OR logic preserves TRUE if ever disputed (can't un-dispute)
     *
     * The EXISTS subqueries check if this escrow was approved/disputed in current range.
     *
     * RETURNING 1: Enables row count collection for diagnostics.
     */
    INSERT INTO escrow_state (
      from_id,
      escrow_id,
      hive_nai,
      hive_amount,
      hbd_nai,
      hbd_amount,
      source_op,
      to_approved,
      disputed
    )
    SELECT
      ui.from_id,
      ui.escrow_id,
      ui.hive_nai,
      ui.hive_amount,
      ui.hbd_nai,
      ui.hbd_amount,
      ui.op_id,
      EXISTS (
        SELECT 1 FROM unique_escrow_approves uea
        WHERE uea.from_id = ui.from_id AND uea.escrow_id = ui.escrow_id
      ) AS to_approved,
      EXISTS (
        SELECT 1 FROM unique_escrow_disputes ued
        WHERE ued.from_id = ui.from_id AND ued.escrow_id = ui.escrow_id
      ) AS disputed
    FROM union_inserts ui
    ON CONFLICT ON CONSTRAINT pk_escrow_state DO UPDATE SET
      hive_amount = EXCLUDED.hive_amount,
      hbd_amount  = EXCLUDED.hbd_amount,
      source_op   = EXCLUDED.source_op,
      to_approved = escrow_state.to_approved OR EXCLUDED.to_approved,
      disputed    = escrow_state.disputed OR EXCLUDED.disputed
    RETURNING 1
  ),
  delete_escrows AS (
    /*
     * Delete escrows that are rejected or fully released.
     *
     * Uses USING clause for efficient join-based deletion.
     * Only deletes escrows that exist in union_deletions.
     *
     * RETURNING 1: Enables row count collection for diagnostics.
     */
    DELETE FROM escrow_state es
    USING union_deletions ud
    WHERE
      es.from_id = ud.from_id AND
      es.escrow_id = ud.escrow_id
    RETURNING 1
  ),
  insert_fees AS (
    /*
     * Insert new escrow fees.
     *
     * These are fees for new escrows that haven't been approved or deleted.
     * No ON CONFLICT needed because fees are only inserted for new escrows
     * (existing fees for pre-range escrows were already there).
     *
     * RETURNING 1: Enables row count collection for diagnostics.
     */
    INSERT INTO escrow_fees (
      from_id,
      escrow_id,
      nai,
      fee_amount
    )
    SELECT
      ff.from_id,
      ff.escrow_id,
      ff.nai,
      ff.amount
    FROM filter_out_deleted_fees ff
    RETURNING 1
  ),
  delete_fees AS (
    /*
     * Delete fees for approved or deleted escrows.
     *
     * When escrow is approved, fee goes to agent (removed from tracking).
     * When escrow is rejected/deleted, fee is returned (removed from tracking).
     *
     * RETURNING 1: Enables row count collection for diagnostics.
     */
    DELETE FROM escrow_fees ef
    USING unique_fee_deletions ufd
    WHERE
      ef.from_id = ufd.from_id AND
      ef.escrow_id = ufd.escrow_id
    RETURNING 1
  )
  /*
   * Collect row counts for diagnostics/logging.
   * These counters can be used to verify processing and debug issues.
   */
  SELECT
    (SELECT COUNT(*) FROM insert_escrows),
    (SELECT COUNT(*) FROM delete_escrows),
    (SELECT COUNT(*) FROM insert_fees),
    (SELECT COUNT(*) FROM delete_fees)
  INTO __ins_escrows, __del_escrows, __ins_fee, __del_fee;

END;
$func$;

RESET ROLE;

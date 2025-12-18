SET ROLE btracker_owner;

/*
 * process_block_range_savings: Processes savings balance operations for a block range.
 *
 * Operations handled: transfer_to_savings, transfer_from_savings, fill_transfer_from_savings,
 * cancel_transfer_from_savings, and interest. Maintains savings balances, pending withdrawal
 * tracking, and aggregated history tables (by_day, by_month).
 */
CREATE OR REPLACE FUNCTION process_block_range_savings(IN _from INT, IN _to INT, IN _report_step INT = 1000)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  -- Operation type IDs (looked up dynamically from hafd.operation_types)
  _op_transfer_to_savings          INT := btracker_backend.op_transfer_to_savings();
  _op_transfer_from_savings        INT := btracker_backend.op_transfer_from_savings();
  _op_cancel_transfer_from_savings INT := btracker_backend.op_cancel_transfer_from_savings();
  _op_fill_transfer_from_savings   INT := btracker_backend.op_fill_transfer_from_savings();
  _op_interest                     INT := btracker_backend.op_interest();
  -- Counters
  __delete_transfer_requests       INT;
  __insert_transfer_requests       INT;
  __upsert_transfers               INT;
  __savings_history                INT;
  __savings_history_by_day         INT;
  __savings_history_by_month       INT;
BEGIN

/*
 * ===================================================================================
 * CTE: ops
 * ===================================================================================
 * WHY MATERIALIZED: This is the foundation for ALL subsequent CTEs. Materializing
 * ensures the expensive operations_view scan happens exactly ONCE. Without MATERIALIZED,
 * PostgreSQL might inline this subquery into every CTE that references it, causing
 * repeated full table scans.
 *
 * PURPOSE: Extract all savings-related operations within our block range.
 *
 * OPERATIONS CAPTURED:
 *   - transfer_to_savings: User deposits funds into their savings account
 *   - transfer_from_savings: User initiates withdrawal (3-day delay starts)
 *   - cancel_transfer_from_savings: User cancels pending withdrawal
 *   - fill_transfer_from_savings: Blockchain completes withdrawal after 3-day delay
 *   - interest: HBD interest payment credited to savings
 */
WITH ops AS MATERIALIZED (
  SELECT ov.body, ov.op_type_id, ov.id, ov.block_num
  FROM operations_view ov
  WHERE
    ov.op_type_id IN (_op_transfer_to_savings, _op_transfer_from_savings, _op_cancel_transfer_from_savings, _op_fill_transfer_from_savings, _op_interest) AND
    ov.block_num BETWEEN _from AND _to
),

/*
 * ===================================================================================
 * CTE: process_block_range_data_b
 * ===================================================================================
 * PURPOSE: Simple projection layer that renames columns for consistency with
 * downstream CTEs. Adds source_op and source_op_block aliases.
 *
 * Note: This intermediate CTE exists to maintain a clean separation between
 * raw operation data and the processed/parsed results in filter_interest_ops.
 */
process_block_range_data_b AS (
  SELECT
    o.body,
    o.id AS source_op,
    o.block_num as source_op_block,
    o.op_type_id
  FROM ops o
),

/*
 * ===================================================================================
 * CTE: filter_interest_ops
 * ===================================================================================
 * PURPOSE: Parse each operation's JSON body and extract savings-relevant fields.
 *
 * DATA TRANSFORMATION:
 *   Uses CROSS JOIN LATERAL with CASE to call the appropriate parser function
 *   for each operation type. Each parser returns a consistent record type with:
 *   - account_name: The account whose savings is affected
 *   - asset_symbol_nai: Asset type (13=HBD, 21=HIVE)
 *   - amount: Balance DELTA (positive for deposits, negative for withdrawals)
 *   - savings_withdraw_request: Delta to pending withdrawal count (+1 or -1)
 *   - request_id: Unique identifier for withdrawal requests (used to match
 *                 transfer_from with its fill/cancel)
 *
 * EDGE CASE - Interest operations:
 *   Interest operations have a flag 'is_saved_into_hbd_balance' in their body.
 *   - If TRUE: Interest goes to regular HBD balance (handled by process_balances.sql)
 *   - If FALSE: Interest stays in savings (handled HERE)
 *   The WHERE clause filters to only process interest that stays in savings.
 *
 * EXAMPLE:
 *   transfer_to_savings of 100 HBD:
 *     -> amount = +100, savings_withdraw_request = 0
 *   transfer_from_savings of 50 HBD (request_id=7):
 *     -> amount = -50, savings_withdraw_request = +1, request_id = 7
 */
filter_interest_ops AS
(
  SELECT
    (SELECT av.id FROM accounts_view av WHERE av.name = result.account_name) AS account_id,
    result.asset_symbol_nai AS nai,
    result.amount AS balance,
    result.savings_withdraw_request,
    result.request_id,
    ov.source_op,
    ov.source_op_block,
    ov.op_type_id
  FROM process_block_range_data_b ov
  CROSS JOIN LATERAL (
    SELECT (
      CASE
        WHEN ov.op_type_id = _op_transfer_to_savings
          THEN btracker_backend.process_transfer_to_savings_operation(ov.body)
        WHEN ov.op_type_id = _op_transfer_from_savings
          THEN btracker_backend.process_transfer_from_savings_operation(ov.body)
        WHEN ov.op_type_id = _op_cancel_transfer_from_savings
          THEN btracker_backend.process_cancel_transfer_from_savings_operation(ov.body)
        WHEN ov.op_type_id = _op_fill_transfer_from_savings
          THEN btracker_backend.process_fill_transfer_from_savings_operation(ov.body)
        WHEN ov.op_type_id = _op_interest
          THEN btracker_backend.process_interest_operation(ov.body)
      END
    ).*
  ) AS result
  WHERE
    ov.op_type_id IN (_op_transfer_to_savings, _op_transfer_from_savings, _op_cancel_transfer_from_savings, _op_fill_transfer_from_savings) OR
    (ov.op_type_id = _op_interest AND (ov.body->'value'->>'is_saved_into_hbd_balance')::BOOLEAN = false)
),

---------------------------------------------------------------------------------------
-- OPERATION TYPE FILTERING
-- The next 3 CTEs split operations by type for specialized handling.
-- This is necessary because cancel/fill operations need to look up the original
-- transfer_from_savings to get the withdrawal amount (especially for cancels).
---------------------------------------------------------------------------------------

/*
 * ===================================================================================
 * CTE: cancel_and_fill_transfers
 * ===================================================================================
 * PURPOSE: Isolate operations that COMPLETE a pending withdrawal.
 *
 * These operations need special handling because:
 *   1. They must match back to their originating transfer_from_savings
 *   2. For cancel: We need to RESTORE the withdrawn amount back to savings
 *   3. For fill: The withdrawal completes (funds leave savings permanently)
 *
 * Both operations decrement the savings_withdraw_request counter by 1.
 */
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
  WHERE op_type_id IN (_op_cancel_transfer_from_savings, _op_fill_transfer_from_savings)
),

/*
 * ===================================================================================
 * CTE: transfers_from
 * ===================================================================================
 * PURPOSE: Isolate transfer_from_savings operations.
 *
 * These represent withdrawal REQUESTS (not completions). When a user initiates
 * transfer_from_savings:
 *   - Funds are immediately deducted from savings balance (balance delta is negative)
 *   - A pending withdrawal record is created (savings_withdraw_request = +1)
 *   - After 3-day delay, fill_transfer_from_savings completes the withdrawal
 *   - User can cancel before fill using cancel_transfer_from_savings
 */
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
  WHERE op_type_id = _op_transfer_from_savings
),

/*
 * ===================================================================================
 * CTE: income_transfers
 * ===================================================================================
 * PURPOSE: Isolate operations that ADD funds to savings (deposits and interest).
 *
 * These are the simplest operations:
 *   - transfer_to_savings: User deposits funds (positive balance delta)
 *   - interest: Blockchain credits HBD interest (positive balance delta)
 *
 * Neither affects pending withdrawal tracking (savings_withdraw_request = 0).
 */
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
  WHERE op_type_id IN (_op_transfer_to_savings, _op_interest)
),

---------------------------------------------------------------------------------------
-- SQUASHING PATTERN: Matching cancel/fill with their originating transfer_from
--
-- THE PROBLEM: When processing blocks in batches (e.g., blocks 1000-2000), we might
-- encounter a cancel/fill operation whose originating transfer_from_savings is:
--   A) In the SAME batch (ideal case - we can match in-memory)
--   B) In a PREVIOUS batch (already persisted to transfer_saving_id table)
--
-- THE SOLUTION: Two-phase lookup:
--   1. First try to find the transfer_from in current batch (join_canceled_transfers_in_query)
--   2. If not found, look up from transfer_saving_id table (canceled_transfers_already_inserted)
--
-- WHY THIS MATTERS FOR CANCEL:
--   cancel_transfer_from_savings doesn't include the amount in its body - we must
--   look up the original transfer_from_savings to know how much to restore.
---------------------------------------------------------------------------------------

/*
 * ===================================================================================
 * CTE: join_canceled_transfers_in_query
 * ===================================================================================
 * PURPOSE: Attempt to match cancel/fill operations with their originating
 * transfer_from_savings within the SAME batch of blocks.
 *
 * MATCHING LOGIC:
 *   - Match by account_id (same account)
 *   - Match by request_id (unique per account)
 *   - The transfer_from must have source_op < cancel/fill source_op (happened earlier)
 *   - Take the LATEST matching transfer_from (ORDER BY DESC LIMIT 1)
 *
 * RESULT:
 *   last_transfer_id = source_op of the matching transfer_from, or NULL if not found
 *   in this batch (meaning it's in a previous batch, stored in transfer_saving_id).
 */
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

/*
 * ===================================================================================
 * CTE: prepare_canceled_transfers_in_query
 * ===================================================================================
 * WHY MATERIALIZED: This CTE is referenced by multiple downstream CTEs
 * (canceled_transfers_in_query, prepare_canceled_transfers_already_inserted).
 * Materializing prevents redundant computation of the CASE expressions and JOIN.
 *
 * PURPOSE: For cancel/fill operations whose originating transfer_from was found
 * in this batch, extract the amount from that transfer_from.
 *
 * BALANCE SIGN INVERSION FOR CANCEL:
 *   - transfer_from_savings has NEGATIVE balance (funds leaving savings)
 *   - cancel needs POSITIVE balance (funds returning to savings)
 *   - So we negate: (- tf.balance) turns -100 into +100
 *
 * FLAGS SET:
 *   - delete_transfer_id_from_table = TRUE if transfer_from NOT found in batch
 *     (meaning we need to delete from transfer_saving_id table after processing)
 *   - insert_transfer_id_to_table = FALSE (cancel/fill never insert new entries)
 *
 * EXAMPLE:
 *   Block 1000: transfer_from_savings(request_id=7, amount=-100 HBD)
 *   Block 1050: cancel_transfer_from_savings(request_id=7)
 *   Result: cancel gets balance=+100, matched from in-batch transfer_from
 */
prepare_canceled_transfers_in_query AS MATERIALIZED (
  SELECT
    jct.account_id,
    (CASE WHEN jct.op_type_id = _op_cancel_transfer_from_savings THEN tf.nai ELSE jct.nai END) AS nai,
    -- We change sign of balance for cancel operation - we take the value from transfer_from_savings
    -- operation which is negative, so we need to change it to positive (we want to give it back to savings)
    (CASE WHEN jct.op_type_id = _op_cancel_transfer_from_savings THEN (- tf.balance) ELSE jct.balance END) AS balance,
    jct.savings_withdraw_request,
    jct.request_id,
    jct.op_type_id,
    jct.source_op,
    jct.source_op_block,
	  jct.last_transfer_id,
    -- If transfer wasn't found in join_canceled_transfers_in_query, it must be in transfer_saving_id table
	  (CASE WHEN jct.last_transfer_id IS NULL THEN TRUE ELSE FALSE END) AS delete_transfer_id_from_table,
	  FALSE AS insert_transfer_id_to_table
  FROM join_canceled_transfers_in_query jct
  LEFT JOIN transfers_from tf ON tf.source_op = jct.last_transfer_id
),

----------------------------------------
-- 1 of 2: Cancels/Fills found in same batch as their transfer_from
-- These don't require table lookup - the transfer_from is still in memory
----------------------------------------

/*
 * ===================================================================================
 * CTE: canceled_transfers_in_query
 * ===================================================================================
 * PURPOSE: Filter to only cancel/fill operations whose originating transfer_from
 * was found in the CURRENT batch (delete_transfer_id_from_table = FALSE).
 *
 * These are the "easy" cases - no table lookup needed, everything resolved in-memory.
 */
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
-- 2 of 2: Cancels/Fills whose transfer_from is in a PREVIOUS batch
-- Must look up from transfer_saving_id table to get amount
----------------------------------------

/*
 * ===================================================================================
 * CTE: prepare_canceled_transfers_already_inserted
 * ===================================================================================
 * PURPOSE: Filter to cancel/fill operations whose transfer_from was NOT found in
 * current batch (delete_transfer_id_from_table = TRUE).
 *
 * These need to look up the withdrawal amount from the transfer_saving_id table,
 * where it was persisted when the transfer_from was processed in an earlier batch.
 */
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

/*
 * ===================================================================================
 * CTE: canceled_transfers_already_inserted
 * ===================================================================================
 * PURPOSE: Look up withdrawal amounts from transfer_saving_id table for cancel/fill
 * operations whose originating transfer_from was in a previous batch.
 *
 * JOIN LOGIC:
 *   Match by (account, request_id) - the unique key in transfer_saving_id
 *
 * BALANCE HANDLING:
 *   For cancel: negate the stored balance to convert withdrawal back to deposit
 *   For fill: use the fill operation's balance field (not the stored value)
 *
 * After processing, the matching row in transfer_saving_id will be DELETED
 * (see delete_all_canceled_or_filled_transfers CTE below).
 */
canceled_transfers_already_inserted AS (
  SELECT
    pct.account_id,
    (CASE WHEN pct.op_type_id = _op_cancel_transfer_from_savings THEN tsi.nai ELSE pct.nai END) AS nai,
    -- We change sign of balance for cancel operation - we take the value from transfer_saving_id
    -- table which is negative, so we need to change it to positive (we want to give it back to savings)
    (CASE WHEN pct.op_type_id = _op_cancel_transfer_from_savings THEN (- tsi.balance) ELSE pct.balance END) AS balance,
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

/*
 * ===================================================================================
 * CTE: transfers_from_canceled_in_query
 * ===================================================================================
 * PURPOSE: Mark which transfer_from_savings operations need to be persisted to
 * the transfer_saving_id table.
 *
 * SQUASHING OPTIMIZATION:
 *   If a transfer_from is CANCELED in the same batch, we DON'T need to persist it
 *   to transfer_saving_id (it would just be immediately deleted). This saves
 *   unnecessary INSERT + DELETE operations.
 *
 * insert_transfer_id_to_table = TRUE only if:
 *   - This is a transfer_from_savings operation AND
 *   - It was NOT canceled/filled in this same batch
 *
 * EXAMPLE:
 *   Batch contains: transfer_from(id=100, request_id=7), cancel(request_id=7)
 *   Result: transfer_from gets insert_transfer_id_to_table = FALSE (squashed)
 *
 *   Batch contains: transfer_from(id=100, request_id=7) only
 *   Result: transfer_from gets insert_transfer_id_to_table = TRUE (persist for future)
 */
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
    -- If transfer was cancelled in the same batch of blocks, it doesn't require insertion into transfer_saving_id table
	  (NOT EXISTS (SELECT 1 FROM canceled_transfers_in_query cti WHERE cti.last_transfer_id = cp.source_op)) AS insert_transfer_id_to_table
  FROM transfers_from cp
),

---------------------------------------------------------------------------------------
-- UNION ALL: Combine all operation types into a single stream for balance calculation
---------------------------------------------------------------------------------------

/*
 * ===================================================================================
 * CTE: union_operations
 * ===================================================================================
 * WHY MATERIALIZED: This union is referenced multiple times downstream
 * (group_by_account_nai, union_operations_with_latest_balance, and the DML CTEs).
 * Materializing prevents re-executing the union and all upstream CTEs.
 *
 * PURPOSE: Combine all processed operations into a single stream with:
 *   - Consistent schema (account_id, nai, balance delta, request tracking flags)
 *   - Proper balance signs (positive for deposits, negative for withdrawals)
 *   - Flags indicating what table operations are needed
 *
 * COMPONENTS:
 *   1. transfers_from_canceled_in_query: Withdrawal requests (may need persistence)
 *   2. income_transfers: Deposits and interest (no persistence needed)
 *   3. canceled_transfers_in_query: Cancel/fill matched in-batch
 *   4. canceled_transfers_already_inserted: Cancel/fill matched from table
 */
union_operations AS MATERIALIZED (
-- Transfers FROM savings (withdrawal requests)
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

-- Transfers TO savings (deposits) and interest
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
-- Cancel / Fill operations (both sets)
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

/*
 * ===================================================================================
 * CTE: group_by_account_nai
 * ===================================================================================
 * PURPOSE: Identify unique (account, asset) pairs affected in this batch.
 *
 * Used to fetch PREVIOUS balances from account_savings table. Each pair needs
 * its starting balance to compute running totals.
 */
group_by_account_nai AS (
  SELECT
    cp.account_id,
    cp.nai
  FROM union_operations cp
  GROUP BY cp.account_id, cp.nai
),

/*
 * ===================================================================================
 * CTE: get_latest_balance
 * ===================================================================================
 * PURPOSE: Fetch the PREVIOUS balance state from account_savings for each
 * affected (account, asset) pair.
 *
 * THE BALANCE FORMULA: prev_balance + sum(all_deltas) = new_balance
 *
 * KEY FIELDS:
 *   - balance: The savings balance BEFORE this batch started
 *   - savings_withdraw_request: Count of pending withdrawals before this batch
 *   - balance_seq_no (balance_change_count): For pagination in history API
 *   - source_op = 0: Marker for "previous state" row (not a real operation)
 *
 * WHY LEFT JOIN: Account might not have a savings record yet (first deposit).
 * COALESCE handles this with 0 defaults.
 */
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
-- RUNNING BALANCE CALCULATION
-- Combine previous state with all operations to compute running balances
---------------------------------------------------------------------------------------

/*
 * ===================================================================================
 * CTE: union_operations_with_latest_balance
 * ===================================================================================
 * PURPOSE: Combine previous balance state with all operations in this batch.
 *
 * The previous state row (from get_latest_balance) has source_op = 0 and serves
 * as the starting point for window function calculations.
 *
 * After this union, the data is ready for SUM() OVER to compute running balances.
 */
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

/*
 * ===================================================================================
 * CTE: prepare_savings_history
 * ===================================================================================
 * PURPOSE: Compute running balances using window functions.
 *
 * WINDOW FUNCTIONS EXPLAINED:
 *   - SUM(balance) OVER w_asc: Running total of balance deltas, giving actual
 *     balance at each point in time
 *   - SUM(savings_withdraw_request) OVER w_asc: Running total of pending
 *     withdrawal count changes
 *   - SUM(balance_seq_no) OVER w_asc: Running sequence number for pagination
 *   - ROW_NUMBER() OVER w_desc: Marks the LATEST operation per (account, nai)
 *     with rn=1 (used to identify final balance for upsert)
 *
 * WINDOW ORDERING:
 *   - w_asc: ORDER BY source_op ASC - chronological order for running totals
 *   - w_desc: ORDER BY source_op DESC - reverse order to find latest with rn=1
 *
 * Secondary sort by balance ensures deterministic ordering for consistent results
 * (defensive measure, since source_op should be unique per operation).
 */
prepare_savings_history AS (
  SELECT
    uo.account_id,
    uo.nai,
    SUM(uo.balance) OVER w_asc AS balance,
    SUM(uo.savings_withdraw_request) OVER w_asc AS savings_withdraw_request,
    SUM(uo.balance_seq_no) OVER w_asc AS balance_seq_no,
    uo.source_op,
    uo.source_op_block,
    ROW_NUMBER() OVER w_desc AS rn
  FROM union_operations_with_latest_balance uo
  WINDOW
    w_asc AS (PARTITION BY uo.account_id, uo.nai ORDER BY uo.source_op, uo.balance ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    w_desc AS (PARTITION BY uo.account_id, uo.nai ORDER BY uo.source_op DESC, uo.balance DESC)
),

---------------------------------------------------------------------------------------
-- DML OPERATIONS: Persist changes to database tables
-- These CTEs perform actual INSERT/UPDATE/DELETE operations as part of the query.
---------------------------------------------------------------------------------------

/*
 * ===================================================================================
 * CTE: delete_all_canceled_or_filled_transfers
 * ===================================================================================
 * PURPOSE: Remove completed withdrawal requests from transfer_saving_id table.
 *
 * When a withdrawal is either:
 *   - FILLED (completed after 3-day delay)
 *   - CANCELED (user changed their mind)
 *
 * The pending request record must be deleted from transfer_saving_id.
 *
 * The delete_transfer_id_from_table flag is TRUE for cancel/fill operations
 * whose originating transfer_from was found in a PREVIOUS batch (and thus
 * has a record in transfer_saving_id that needs cleanup).
 */
delete_all_canceled_or_filled_transfers AS (
  DELETE FROM transfer_saving_id tsi
  USING union_operations_with_latest_balance uo
  WHERE
    tsi.request_id = uo.request_id AND
    tsi.account = uo.account_id AND
    uo.delete_transfer_id_from_table
  RETURNING tsi.account AS cleaned_account_id
),

/*
 * ===================================================================================
 * CTE: insert_all_new_registered_transfers_from_savings
 * ===================================================================================
 * PURPOSE: Persist new pending withdrawal requests to transfer_saving_id table.
 *
 * When a transfer_from_savings is NOT immediately canceled/filled in the same
 * batch, we must store it for future lookup when the cancel/fill arrives
 * (potentially in a later batch).
 *
 * The insert_transfer_id_to_table flag is TRUE for transfer_from_savings
 * operations that weren't squashed (canceled in same batch).
 *
 * STORED DATA:
 *   - account: Account making the withdrawal
 *   - nai: Asset type being withdrawn
 *   - balance: Amount (negative, as it's a withdrawal)
 *   - request_id: Unique identifier to match with future cancel/fill
 */
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

/*
 * ===================================================================================
 * CTE: remove_latest_stored_balance_record
 * ===================================================================================
 * WHY MATERIALIZED: This filtered result set is used by multiple downstream
 * DML CTEs (insert_sum_of_transfers, insert_saving_balance_history,
 * join_created_at_to_balance_history). Materializing prevents re-computing
 * the filter and sort for each consumer.
 *
 * PURPOSE: Filter out the "previous state" marker rows (source_op = 0) and
 * prepare data for insertion into history tables.
 *
 * The source_op > 0 filter removes the synthetic rows from get_latest_balance
 * that were only needed as starting points for window calculations.
 *
 * ORDER BY source_op ensures consistent ordering for deterministic results.
 */
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
  -- Remove rows with synthetic "previous balance" marker
  WHERE pbh.source_op > 0
  ORDER BY pbh.source_op
),

/*
 * ===================================================================================
 * CTE: insert_sum_of_transfers
 * ===================================================================================
 * PURPOSE: Update the CURRENT savings balance in account_savings table.
 *
 * Only inserts/updates the LATEST balance (rn = 1) for each (account, nai) pair.
 *
 * ON CONFLICT (UPSERT) PATTERN:
 *   - If account already has a savings record: UPDATE with new values
 *   - If no record exists: INSERT new row
 *
 * WHY ON CONFLICT vs separate INSERT/UPDATE:
 *   Single atomic operation, no race conditions, cleaner SQL.
 *
 * EXCLUDED keyword refers to the row that would have been inserted,
 * allowing us to use the new values in the UPDATE clause.
 */
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

/*
 * ===================================================================================
 * CTE: insert_saving_balance_history
 * ===================================================================================
 * PURPOSE: Insert ALL balance changes into account_savings_history table.
 *
 * Unlike insert_sum_of_transfers (which only keeps latest), this records
 * EVERY balance change for historical tracking and pagination.
 *
 * No ON CONFLICT - history is append-only. Each operation creates a new row.
 *
 * The balance_seq_no provides a monotonically increasing sequence per
 * (account, nai) pair for efficient pagination in the API.
 */
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

---------------------------------------------------------------------------------------
-- AGGREGATED HISTORY: Daily and monthly snapshots with min/max tracking
---------------------------------------------------------------------------------------

/*
 * ===================================================================================
 * CTE: join_created_at_to_balance_history
 * ===================================================================================
 * WHY MATERIALIZED: This join with hive.blocks_view is expensive (timestamp
 * lookup per block). Materializing ensures we do this lookup exactly once,
 * and both daily and monthly aggregations reuse the result.
 *
 * PURPOSE: Add timestamp information to each balance change for time-based
 * aggregation into daily and monthly summaries.
 *
 * DATE_TRUNC usage:
 *   - by_day: Truncates timestamp to start of day (2024-01-15 14:30:00 -> 2024-01-15 00:00:00)
 *   - by_month: Truncates to start of month (2024-01-15 -> 2024-01-01 00:00:00)
 *
 * These truncated values become the primary key in aggregation tables.
 */
join_created_at_to_balance_history AS MATERIALIZED (
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

/*
 * ===================================================================================
 * CTE: aggregated_savings_history
 * ===================================================================================
 * WHY MATERIALIZED: This CTE computes multiple window functions that are used
 * by both insert_saving_balance_history_by_day and insert_saving_balance_history_by_month.
 * Materializing ensures all window computations happen once.
 *
 * PURPOSE: Compute aggregations for both daily and monthly history in a single pass.
 *
 * WINDOW FUNCTIONS:
 *   - ROW_NUMBER() w_day_desc/w_month_desc: Identifies the LATEST balance within
 *     each day/month (rn=1). Used to get end-of-period balance.
 *   - MIN(balance) OVER w_day_all/w_month_all: Lowest balance during period
 *   - MAX(balance) OVER w_day_all/w_month_all: Highest balance during period
 *
 * WINDOW FRAME DIFFERENCE:
 *   - w_day_desc/w_month_desc: Ordered by source_op DESC for row numbering
 *   - w_day_all/w_month_all: No ordering (entire partition) for min/max
 *
 * EFFICIENCY: Using WINDOW clause avoids repeating partition definitions
 * and allows PostgreSQL to compute all window functions in a single pass.
 */
aggregated_savings_history AS MATERIALIZED (
  SELECT
    account_id,
    nai,
    source_op,
    balance,
    by_day,
    by_month,
    ROW_NUMBER() OVER w_day_desc AS rn_by_day,
    ROW_NUMBER() OVER w_month_desc AS rn_by_month,
    MIN(balance) OVER w_day_all AS min_balance_day,
    MAX(balance) OVER w_day_all AS max_balance_day,
    MIN(balance) OVER w_month_all AS min_balance_month,
    MAX(balance) OVER w_month_all AS max_balance_month
  FROM join_created_at_to_balance_history
  WINDOW
    w_day_desc AS (PARTITION BY account_id, nai, by_day ORDER BY source_op DESC),
    w_month_desc AS (PARTITION BY account_id, nai, by_month ORDER BY source_op DESC),
    w_day_all AS (PARTITION BY account_id, nai, by_day),
    w_month_all AS (PARTITION BY account_id, nai, by_month)
),

/*
 * ===================================================================================
 * CTE: insert_saving_balance_history_by_day
 * ===================================================================================
 * PURPOSE: Insert or update daily savings balance snapshots.
 *
 * Only the LATEST balance for each day (rn_by_day = 1) is used as the
 * end-of-day balance.
 *
 * ON CONFLICT HANDLING:
 *   When a day already has a record (from earlier batch processing same day):
 *   - source_op: Update to the later operation
 *   - balance: Update to the later balance
 *   - min_balance: Keep the LOWER of existing and new minimums (LEAST)
 *   - max_balance: Keep the HIGHER of existing and new maximums (GREATEST)
 *
 * WHY LEAST/GREATEST:
 *   A day might be processed across multiple batches. Each batch sees only
 *   part of the day's operations. To get accurate min/max for the full day,
 *   we must compare across all batches.
 *
 * EXAMPLE:
 *   Batch 1 (morning): balance goes 100 -> 50 -> 80. min=50, max=100
 *   Batch 2 (evening): balance goes 80 -> 120 -> 90. min=80, max=120
 *   Final record: min=LEAST(50,80)=50, max=GREATEST(100,120)=120
 *
 * (xmax = 0) as is_new_entry: PostgreSQL trick to detect if INSERT or UPDATE
 * occurred. xmax=0 means INSERT (new row), xmax>0 means UPDATE (existing row).
 */
insert_saving_balance_history_by_day AS (
  INSERT INTO saving_history_by_day AS acc_history
    (account, nai, source_op, updated_at, balance, min_balance, max_balance)
  SELECT
    ash.account_id,
    ash.nai,
    ash.source_op,
    ash.by_day,
    ash.balance,
    ash.min_balance_day,
    ash.max_balance_day
  FROM aggregated_savings_history ash
  WHERE ash.rn_by_day = 1
  ON CONFLICT ON CONSTRAINT pk_saving_history_by_day DO
  UPDATE SET
    source_op = EXCLUDED.source_op,
    balance = EXCLUDED.balance,
    min_balance = LEAST(EXCLUDED.min_balance, acc_history.min_balance),
    max_balance = GREATEST(EXCLUDED.max_balance, acc_history.max_balance)
  RETURNING (xmax = 0) as is_new_entry, acc_history.account
),

/*
 * ===================================================================================
 * CTE: insert_saving_balance_history_by_month
 * ===================================================================================
 * PURPOSE: Insert or update monthly savings balance snapshots.
 *
 * Identical logic to daily aggregation, but partitioned by month instead of day.
 *
 * ON CONFLICT: Same LEAST/GREATEST pattern for min/max across batch boundaries.
 *
 * Monthly snapshots are useful for:
 *   - Long-term trend analysis
 *   - Efficient queries spanning years of history
 *   - Interest calculation verification
 */
insert_saving_balance_history_by_month AS (
  INSERT INTO saving_history_by_month AS acc_history
    (account, nai, source_op, updated_at, balance, min_balance, max_balance)
  SELECT
    ash.account_id,
    ash.nai,
    ash.source_op,
    ash.by_month,
    ash.balance,
    ash.min_balance_month,
    ash.max_balance_month
  FROM aggregated_savings_history ash
  WHERE ash.rn_by_month = 1
  ON CONFLICT ON CONSTRAINT pk_saving_history_by_month DO
  UPDATE SET
    source_op = EXCLUDED.source_op,
    balance = EXCLUDED.balance,
    min_balance = LEAST(EXCLUDED.min_balance, acc_history.min_balance),
    max_balance = GREATEST(EXCLUDED.max_balance, acc_history.max_balance)
  RETURNING (xmax = 0) as is_new_entry, acc_history.account
)

/*
 * ===================================================================================
 * FINAL SELECT: Gather operation counts for logging/monitoring
 * ===================================================================================
 * Each count represents the number of rows affected by the corresponding DML CTE.
 * These are stored in local variables for potential logging or reporting.
 */
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

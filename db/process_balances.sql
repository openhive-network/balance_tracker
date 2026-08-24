SET ROLE btracker_owner;

/*
 * btracker_backend.accumulate_hbd_interest
 * ========================================
 * Single-pass replacement for the recursive-CTE HBD interest accumulator that
 * used to live inside process_block_range_balances. The fold is sequentially
 * dependent (each 30-day reset moves the anchor), which a window function
 * cannot express - but a plain ordered loop expresses it directly, in O(rows)
 * with trivial constants. The recursive form additionally poisoned the whole
 * statement's planning: through the CTE chain the planner estimated it at
 * ~5e17 rows (actual: tens of thousands), distorting join choices everywhere.
 *
 * Consumes the session temp table _hbd_batch_rows staged by
 * process_block_range_balances (liquid-HBD running-balance rows of the batch,
 * HF25-gated there). Timestamp rule, epoch defaults and the reset condition are
 * verbatim from the previous implementation - see the staging comment in
 * process_block_range_balances for the chain model references.
 *
 * Returns the number of accounts upserted into account_hbd_interest.
 */
CREATE OR REPLACE FUNCTION btracker_backend.accumulate_hbd_interest(
    _from INT,
    _to INT,
    _hbd_interest_interval INT
)
RETURNS INT
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  r RECORD;
  __cur_account   INT := NULL;
  __seconds       NUMERIC;
  __last_update   TIMESTAMP;
  __last_balance  BIGINT;
  __last_payment  TIMESTAMP;
  __accrued       NUMERIC;
  __acc_ids  INT[]       := '{}';
  __secs     NUMERIC[]   := '{}';
  __updates  TIMESTAMP[] := '{}';
  __balances BIGINT[]    := '{}';
  __payments TIMESTAMP[] := '{}';
  __n INT := 0;
BEGIN
  FOR r IN
    SELECT
      h.account_id,
      h.balance,
      -- transaction ops (trx_in_block >= 0) are applied before head_block_time
      -- advances -> previous block's time; standalone virtual ops -> own time
      CASE WHEN h.trx_in_block >= 0
           THEN COALESCE(bvp.created_at, bv.created_at)
           ELSE bv.created_at
      END AS effective_ts,
      hi.hbd_seconds               AS p_seconds,
      hi.hbd_seconds_last_update   AS p_update,
      hi.last_balance              AS p_balance,
      hi.hbd_last_interest_payment AS p_payment
    FROM _hbd_batch_rows h
    -- range guards: reversible-union view, planner cannot propagate the range
    JOIN hive.blocks_view bv ON bv.num = h.source_op_block
                            AND bv.num BETWEEN _from AND _to
    LEFT JOIN hive.blocks_view bvp ON bvp.num = h.source_op_block - 1
                                  AND bvp.num BETWEEN _from - 1 AND _to
    LEFT JOIN account_hbd_interest hi ON hi.account = h.account_id
    ORDER BY h.account_id, h.source_op, h.balance_seq_no
  LOOP
    IF __cur_account IS DISTINCT FROM r.account_id THEN
      IF __cur_account IS NOT NULL THEN
        __n := __n + 1;
        __acc_ids[__n] := __cur_account; __secs[__n] := __seconds;
        __updates[__n] := __last_update; __balances[__n] := __last_balance;
        __payments[__n] := __last_payment;
      END IF;
      __cur_account  := r.account_id;
      -- epoch defaults are load-bearing (see chain model notes): the first
      -- positive-balance op of a brand-new account must reset immediately
      __seconds      := COALESCE(r.p_seconds, 0::NUMERIC);
      __last_update  := COALESCE(r.p_update, 'epoch'::TIMESTAMP);
      __last_balance := COALESCE(r.p_balance, 0::BIGINT);
      __last_payment := COALESCE(r.p_payment, 'epoch'::TIMESTAMP);
    END IF;

    __accrued := __seconds
               + __last_balance::NUMERIC * EXTRACT(EPOCH FROM (r.effective_ts - __last_update));
    IF __accrued > 0 AND EXTRACT(EPOCH FROM (r.effective_ts - __last_payment)) > _hbd_interest_interval THEN
      __seconds      := 0::NUMERIC;
      __last_payment := r.effective_ts;
    ELSE
      __seconds := __accrued;
    END IF;
    __last_update  := r.effective_ts;
    __last_balance := r.balance;
  END LOOP;

  IF __cur_account IS NOT NULL THEN
    __n := __n + 1;
    __acc_ids[__n] := __cur_account; __secs[__n] := __seconds;
    __updates[__n] := __last_update; __balances[__n] := __last_balance;
    __payments[__n] := __last_payment;
  END IF;

  INSERT INTO account_hbd_interest
    (account, hbd_seconds, hbd_seconds_last_update, last_balance, hbd_last_interest_payment)
  SELECT * FROM unnest(__acc_ids, __secs, __updates, __balances, __payments)
  ON CONFLICT ON CONSTRAINT pk_account_hbd_interest DO UPDATE SET
    hbd_seconds               = EXCLUDED.hbd_seconds,
    hbd_seconds_last_update   = EXCLUDED.hbd_seconds_last_update,
    last_balance              = EXCLUDED.last_balance,
    hbd_last_interest_payment = EXCLUDED.hbd_last_interest_payment;

  RETURN __n;
END
$$;

/*
 * process_block_range_balances: Processes balance-impacting operations for a block range.
 *
 * Core operations: Extracts balance deltas from blockchain operations, computes running
 * balances, and updates current_account_balances, account_balance_history, and
 * aggregated history tables (by_day, by_month).
 */
CREATE OR REPLACE FUNCTION process_block_range_balances(
    IN _from INT, IN _to INT
)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
-- PostgreSQL 17.11 wraps the get_impacted_balances LATERAL in a Memoize node keyed
-- only on op_type_id, which pushes the bio.id = ho.op_type_id filter above the
-- lateral: the function then runs once per (operation x every balance-impacting op
-- type) instead of once per matched operation, and the cache never hits (~100x
-- slower massive sync). 17.10 and earlier pick the hash-join-first plan on their own.
SET enable_memoize = OFF
AS
$$
DECLARE
 _result INT;
 _nai_hbd              SMALLINT := btracker_backend.nai_hbd();
 _hf_hbd_interest      INT      := btracker_backend.hf_hbd_interest();
 _hf_hbd_interest_block INT;
 _hbd_interest_interval INT     := btracker_backend.hbd_interest_compound_interval_sec();
 __hf01_block INT;
 __balance_history INT;
 __current_balances INT;
 __balance_history_by_day INT;
 __balance_history_by_month INT;
 __hbd_interest INT;
 -- During MASSIVE sync (app indexes not yet created) the by_day/by_month rollups
 -- are deferred: maintaining them per batch costs window sorts + two upserts on
 -- every batch, while finalize_massive_sync() can rebuild both tables in one
 -- set-based pass over account_balance_history at the massive->LIVE transition.
 -- In LIVE (indexes created) the incremental rollups run as before.
 __maintain_period_rollups BOOLEAN := isIndexesCreated();
BEGIN

SELECT block_num
INTO _hf_hbd_interest_block
FROM hafd.applied_hardforks
WHERE hardfork_num = _hf_hbd_interest;

SELECT block_num
INTO __hf01_block
FROM hafd.applied_hardforks
WHERE hardfork_num = 1;

-- Staging table for the HBD interest accumulator (consumed by
-- btracker_backend.accumulate_hbd_interest after the main statement below).
DROP TABLE IF EXISTS _hbd_batch_rows;
CREATE TEMP TABLE _hbd_batch_rows(
  account_id      INT,
  balance         BIGINT,
  source_op       BIGINT,
  source_op_block INT,
  balance_seq_no  BIGINT,
  trx_in_block    SMALLINT
);
--RAISE NOTICE 'Processing balances';

/*
 * ===================================================================================
 * CTE: balance_impacting_ops
 * ===================================================================================
 * WHY MATERIALIZED: This lookup is used by ops_in_range to filter operations.
 * Materializing ensures the subquery executes once and is cached, preventing
 * repeated evaluation during the join. Without MATERIALIZED, the planner might
 * inline this subquery into every row evaluation of the JOIN, causing O(n) calls
 * to get_balance_impacting_operations() instead of O(1).
 *
 * This CTE retrieves all operation type IDs that can affect account balances,
 * such as transfers, rewards, vesting operations, etc.
 */
WITH ops_in_range AS MATERIALIZED
(
  /*
   * WHY MATERIALIZED: foundation for all subsequent calculations; materializing
   * caches the extraction so the union/window CTEs below reuse it.
   *
   * btracker_backend.get_impacted_balances_batch extracts every (account, asset,
   * delta) emission for the range in ONE set-based query over _btracker_ops_batch.
   * It replaces the previous per-operation CROSS JOIN LATERAL
   * btracker_backend.get_impacted_balances(...) call: the per-row variant's SQL was
   * cheap but each of the ~64k calls per op-dense 10k-block batch carried ~55us of
   * SPI/tuplestore invocation overhead, making extraction the dominant select-side
   * cost (measured 3.0s -> 0.8s per dense batch at block ~25M). Row-for-row parity
   * between the two implementations is guarded by tests/parity.
   *
   * NOTE: 'balance' column here is actually a DELTA (amount of change), not an
   * absolute balance. The name is historical but the actual running balance is
   * computed later using SUM() OVER window functions.
   */
  SELECT
    (SELECT av.id FROM accounts_view av WHERE av.name = gib.account_name) AS account_id,
    gib.asset_symbol_nai AS nai,
    gib.amount AS balance,
    gib.source_op,
    gib.source_op_block,
    -- threaded down to the HBD interest accumulator so it does not have to re-read
    -- _btracker_ops_batch; trx_in_block distinguishes transaction ops (>= 0, applied
    -- before head_block_time advances -> use block-1 time) from standalone virtual ops
    -- (-1, applied after -> use block time). See hbd_ops effective_ts.
    gib.trx_in_block
  FROM btracker_backend.get_impacted_balances_batch( _from, _to, __hf01_block ) AS gib
),

/*
 * ===================================================================================
 * CTE: group_by_account_nai
 * ===================================================================================
 * PURPOSE: Identify the unique (account_id, nai) pairs affected in this block range.
 *
 * WHY THIS EXISTS: We need to know which accounts were touched so we can:
 *   1. Fetch their PREVIOUS balance from current_account_balances
 *   2. Use that as the starting point for running balance calculation
 *
 * This is essentially a "distinct accounts touched" list. One account might have
 * multiple operations in this range, but we only need to fetch their starting
 * balance once per (account, asset_type) combination.
 */
group_by_account_nai AS (
  SELECT
    cp.account_id,
    cp.nai
  FROM ops_in_range cp
  GROUP BY cp.account_id, cp.nai
),

/*
 * ===================================================================================
 * CTE: get_latest_balance
 * ===================================================================================
 * PURPOSE: Fetch the PREVIOUS balance state for each affected account/asset pair.
 *
 * THE BALANCE FORMULA: prev_balance + sum(all_deltas) = new_balance
 *
 * This CTE retrieves the "prev_balance" part. For accounts that have never held
 * this asset before, COALESCE defaults to 0.
 *
 * KEY FIELDS:
 *   - balance: The balance BEFORE this block range started
 *   - balance_seq_no: How many balance changes have occurred historically
 *                     (used for pagination in balance history API)
 *   - source_op = 0: Marker indicating this is a "previous state" row, not a
 *                    real operation. Will be filtered out later when inserting
 *                    to history tables.
 *   - source_op_block = 0: Same marker purpose
 *
 * WHY LEFT JOIN: Account might not exist in current_account_balances yet (new
 * account receiving funds for the first time). LEFT JOIN + COALESCE handles
 * this gracefully with default values.
 */
get_latest_balance AS (
  SELECT
    gan.account_id,
    gan.nai,
    COALESCE(cab.balance, 0) as balance,
    COALESCE(cab.balance_change_count, 0) as balance_seq_no,
    0 AS source_op,
    0 AS source_op_block
--    (CASE WHEN cab.balance IS NULL THEN FALSE ELSE TRUE END) AS prev_balance_exists
  FROM group_by_account_nai gan
  LEFT JOIN current_account_balances cab ON cab.account = gan.account_id AND cab.nai = gan.nai
),

/*
 * ===================================================================================
 * CTE: union_latest_balance_with_impacted_balances
 * ===================================================================================
 * PURPOSE: Combine previous balance with all new deltas into a single dataset
 *          for running balance calculation.
 *
 * UNION ALL PATTERN:
 *   Part 1 (ops_in_range): All balance DELTAS from this block range
 *           - balance_seq_no = 1 for each operation (will be summed later)
 *   Part 2 (get_latest_balance): The STARTING balance before this range
 *           - balance_seq_no = previous count (to continue the sequence)
 *           - source_op = 0 (marker for "synthetic" starting row)
 *
 * EXAMPLE with 3 operations on account 42, nai 21 (HIVE):
 *   Previous balance was 1000, with 5 historical changes.
 *   This range has: +100 (op 5001), -50 (op 5002), +200 (op 5003)
 *
 *   After UNION ALL:
 *   | account_id | nai | balance | balance_seq_no | source_op |
 *   |------------|-----|---------|----------------|-----------|
 *   | 42         | 21  | 1000    | 5              | 0         | <- starting point
 *   | 42         | 21  | +100    | 1              | 5001      | <- delta
 *   | 42         | 21  | -50     | 1              | 5002      | <- delta
 *   | 42         | 21  | +200    | 1              | 5003      | <- delta
 *
 * After window function SUM() in next CTE, running balances will be:
 *   source_op=0:    1000
 *   source_op=5001: 1000+100 = 1100
 *   source_op=5002: 1000+100-50 = 1050
 *   source_op=5003: 1000+100-50+200 = 1250
 *
 * And balance_seq_no will be: 5, 6, 7, 8 (continuing the sequence)
 */
union_latest_balance_with_impacted_balances AS (
  SELECT
    cp.account_id,
    cp.nai,
    cp.balance,
    1 AS balance_seq_no,
    cp.source_op,
    cp.source_op_block,
    cp.trx_in_block
  FROM ops_in_range cp

  UNION ALL

-- latest stored balance is needed to replicate balance history
-- (synthetic source_op=0 row; trx_in_block is irrelevant and filtered out later)
  SELECT
    glb.account_id,
    glb.nai,
    glb.balance,
    glb.balance_seq_no,
    glb.source_op,
    glb.source_op_block,
    NULL::SMALLINT AS trx_in_block
  FROM get_latest_balance glb
),

/*
 * ===================================================================================
 * CTE: prepare_balance_history
 * ===================================================================================
 * WHY MATERIALIZED: This CTE performs expensive window function calculations that
 * are referenced multiple times downstream (insert_current_account_balances,
 * remove_latest_stored_balance_record). Materializing prevents duplicate computation.
 *
 * THE CORE CALCULATION - Running Balance with Window Functions:
 *
 *   SUM(balance) OVER (PARTITION BY account_id, nai ORDER BY source_op, balance
 *                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
 *
 *   This computes a CUMULATIVE SUM of all balance deltas, giving us the actual
 *   balance at each point in time. The ORDER BY source_op ensures operations are
 *   processed in chronological order.
 *
 * "LAST OPERATION WINS" PATTERN with ROW_NUMBER():
 *
 *   ROW_NUMBER() OVER (PARTITION BY account_id, nai ORDER BY source_op DESC, balance DESC)
 *
 *   By ordering DESC (descending), rn=1 identifies the MOST RECENT operation for
 *   each (account, nai) pair. This is the "squashing" pattern: we process many
 *   operations but only the FINAL STATE (rn=1) goes into current_account_balances.
 *   This enables fast sync because we avoid updating the same row multiple times.
 *
 * WHY TWO SEPARATE CTEs (sum_balances + prepare_balance_history):
 *   The ROW_NUMBER() must order by the COMPUTED running balance, not the input delta.
 *   PostgreSQL window functions in a WINDOW clause can only reference input columns,
 *   not computed output columns from the same SELECT. Therefore, we must first compute
 *   the running balance in sum_balances, then compute ROW_NUMBER in prepare_balance_history
 *   where sb.balance refers to the already-computed running sum.
 *
 * EDGE CASE - escrow_rejected_operation:
 *   Some operations (like escrow rejection) can trigger MULTIPLE balance changes
 *   for the same asset in a single operation. This creates multiple rows with
 *   identical source_op but different balance amounts. The secondary ORDER BY
 *   on 'balance' ensures deterministic ordering even in this edge case.
 *
 * NUMERICAL WALKTHROUGH (continuing from above example):
 *
 *   Input (sorted by source_op ASC for SUM calculation):
 *   | source_op | balance (delta) |
 *   |-----------|-----------------|
 *   | 0         | 1000            |
 *   | 5001      | +100            |
 *   | 5002      | -50             |
 *   | 5003      | +200            |
 *
 *   After SUM() OVER w_asc:
 *   | source_op | balance (running) | balance_seq_no |
 *   |-----------|-------------------|----------------|
 *   | 0         | 1000              | 5              |
 *   | 5001      | 1100              | 6              |
 *   | 5002      | 1050              | 7              |
 *   | 5003      | 1250              | 8              |
 *
 *   After ROW_NUMBER() OVER w_desc (descending order):
 *   | source_op | balance | rn |
 *   |-----------|---------|-----|
 *   | 5003      | 1250    | 1   | <- MOST RECENT (goes to current_account_balances)
 *   | 5002      | 1050    | 2   |
 *   | 5001      | 1100    | 3   |
 *   | 0         | 1000    | 4   | <- synthetic row (will be filtered out)
 */

-- First compute running balances with SUM window function.
-- This must be a separate CTE because ROW_NUMBER needs to order by the COMPUTED
-- running balance, not the input delta.
sum_balances AS (
  SELECT
    ulb.account_id,
    ulb.nai,
    SUM(ulb.balance) OVER w_asc AS balance,
    SUM(ulb.balance_seq_no) OVER w_asc AS balance_seq_no,
    ulb.source_op,
    ulb.source_op_block,
    ulb.trx_in_block
  FROM union_latest_balance_with_impacted_balances ulb
  WINDOW
    w_asc AS (PARTITION BY ulb.account_id, ulb.nai ORDER BY ulb.source_op, ulb.balance ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

-- The row number must be calculated AFTER the sum since operations like escrow_rejected_operation
-- can trigger multiple balance changes for the same asset, leading to multiple rows with the
-- same source_op. The ROW_NUMBER must order by the COMPUTED running balance (sb.balance),
-- not the input delta, to correctly identify the last operation.
prepare_balance_history AS MATERIALIZED (
  SELECT
    sb.account_id,
    sb.nai,
    sb.balance,
    sb.balance_seq_no,
    sb.source_op,
    sb.source_op_block,
    sb.trx_in_block,
    ROW_NUMBER() OVER (PARTITION BY sb.account_id, sb.nai ORDER BY sb.source_op DESC, sb.balance DESC) AS rn
  FROM sum_balances sb
),

/*
 * ===================================================================================
 * CTE: insert_current_account_balances
 * ===================================================================================
 * PURPOSE: Update the current_account_balances table with the FINAL balance state.
 *
 * "SQUASHING" PATTERN:
 *   WHERE rd.rn = 1 filters to ONLY the most recent operation per (account, nai).
 *   Even if an account had 1000 balance changes in this block range, we only
 *   INSERT/UPDATE the final state. This is critical for fast blockchain sync:
 *   - Without squashing: 1000 separate UPDATE statements
 *   - With squashing: 1 INSERT ... ON CONFLICT DO UPDATE
 *
 * ON CONFLICT (UPSERT) PATTERN:
 *   - If (account, nai) doesn't exist: INSERT new row
 *   - If (account, nai) already exists: UPDATE with new values
 *
 *   ON CONFLICT ON CONSTRAINT pk_current_account_balances DO UPDATE SET ...
 *
 *   PostgreSQL's EXCLUDED pseudo-table references the values that WOULD have been
 *   inserted. We use EXCLUDED.balance, EXCLUDED.balance_change_count, etc. to
 *   update the existing row with new values.
 *
 * RETURNING (xmax = 0):
 *   This is a PostgreSQL trick to detect whether the row was INSERTed (xmax=0)
 *   or UPDATEd (xmax>0). xmax is the transaction ID that deleted/updated the row.
 *   For a fresh INSERT, no transaction has touched it yet, so xmax=0.
 *   Useful for debugging/monitoring insert vs update ratios.
 */
insert_current_account_balances AS (
  INSERT INTO current_account_balances AS acc_balances
    (account, nai, balance_change_count, source_op, balance)
  SELECT
    rd.account_id,
    rd.nai,
    rd.balance_seq_no,
    rd.source_op,
    rd.balance
  FROM prepare_balance_history rd
  WHERE rd.rn = 1
  ON CONFLICT ON CONSTRAINT pk_current_account_balances DO
  UPDATE SET
    balance = EXCLUDED.balance,
    balance_change_count = EXCLUDED.balance_change_count,
    source_op = EXCLUDED.source_op
  RETURNING (xmax = 0) as is_new_entry, acc_balances.account
),

/*
 * ===================================================================================
 * CTE: remove_latest_stored_balance_record
 * ===================================================================================
 * WHY MATERIALIZED: This filtered dataset is used by two downstream CTEs
 * (insert_account_balance_history, join_created_at_to_balance_history).
 * Without materialization, the filter and sort would be re-executed for each reference.
 *
 * PURPOSE: Filter out the synthetic "previous balance" row (source_op = 0) that
 *          was used for running balance calculation but should NOT be inserted
 *          into history tables.
 *
 * FILTER: WHERE pbh.source_op > 0
 *   Removes the synthetic starting balance row (source_op = 0) since it represents
 *   a previously recorded state, not a new operation. We only want to insert
 *   ACTUAL operations into the history table.
 *
 * ORDER BY pbh.source_op:
 *   Ensures chronological ordering for bulk insert into account_balance_history.
 *   While not strictly required for correctness, ordered inserts can improve
 *   index maintenance performance and data locality.
 */
remove_latest_stored_balance_record AS MATERIALIZED (
  SELECT
    pbh.account_id,
    pbh.nai,
    pbh.balance_seq_no,
    pbh.source_op,
    pbh.source_op_block,
    pbh.balance,
    pbh.trx_in_block
  FROM prepare_balance_history pbh
  -- Remove the synthetic row that contained the prepared previous balance
  WHERE pbh.source_op > 0
  ORDER BY pbh.source_op
),

/*
 * ===================================================================================
 * CTE: insert_account_balance_history
 * ===================================================================================
 * PURPOSE: Insert EVERY balance change (not just the final state) into the
 *          account_balance_history table for audit trail and historical queries.
 *
 * Unlike current_account_balances which only stores the LATEST state (squashing),
 * this table stores EVERY operation that affected the balance. This enables:
 *   - Balance history API queries with pagination
 *   - Audit trails showing all balance changes
 *   - Point-in-time balance reconstruction
 *
 * NOTE: This is an INSERT, not UPSERT. Each operation creates a new history row.
 * The primary key is not (account, nai) but includes source_op for uniqueness.
 */
insert_account_balance_history AS (
  INSERT INTO account_balance_history AS acc_history
    (account, nai, balance_seq_no, source_op, balance)
  SELECT
    pbh.account_id,
    pbh.nai,
    pbh.balance_seq_no,
    pbh.source_op,
    pbh.balance
  FROM remove_latest_stored_balance_record pbh
  RETURNING (xmax = 0) as is_new_entry, acc_history.account
),

/*
 * ===================================================================================
 * CTE: join_created_at_to_balance_history
 * ===================================================================================
 * WHY MATERIALIZED: The JOIN with hive.blocks_view is expensive (requires index
 * lookup for each block number). Materializing ensures this join happens ONCE,
 * and the result is reused by aggregated_balance_history.
 *
 * PURPOSE: Enrich balance history records with timestamps for time-based aggregation.
 *          We need to know WHEN each balance change occurred to group by day/month.
 *
 * TIMESTAMP EXTRACTION:
 *   - date_trunc('day', created_at): Truncates to midnight of that day (e.g., 2024-01-15 00:00:00)
 *   - date_trunc('month', created_at): Truncates to first of month (e.g., 2024-01-01 00:00:00)
 *
 * These truncated timestamps become the grouping keys for daily/monthly aggregation.
 * Multiple operations on the same day will share the same by_day value.
 */
-- aggregated balance history by day and month
-- Join block timestamps once and compute all aggregations in a single pass
join_created_at_to_balance_history AS MATERIALIZED (
  SELECT
    rls.account_id,
    rls.nai,
    rls.source_op,
    rls.source_op_block,
    rls.balance,
    rls.balance_seq_no,
    rls.trx_in_block,
    bv.created_at,
    date_trunc('day', bv.created_at) AS by_day,
    date_trunc('month', bv.created_at) AS by_month
  FROM remove_latest_stored_balance_record rls
  -- The explicit range on bv.num is redundant with the join condition but
  -- necessary: blocks_view is a reversible-union view and the planner cannot
  -- propagate the batch range through the join equivalence - without it, the
  -- view is re-executed per row (one pk_hive_blocks probe plus a reversible
  -- Append per emission, ~76k executions per op-dense batch) instead of being
  -- scanned once and hash-joined. Same planner guard as haf_block_explorer!503.
  JOIN hive.blocks_view bv ON bv.num = rls.source_op_block
                          AND bv.num BETWEEN _from AND _to
  -- One-time filter: with the HBD accumulator moved out of this statement
  -- (btracker_backend.accumulate_hbd_interest), the period rollups are this
  -- CTE's only consumers, so massive batches skip the timestamp join entirely
  -- (same gating as the savings pipeline).
  WHERE __maintain_period_rollups
),

/*
 * ===================================================================================
 * CTE: aggregated_balance_history
 * ===================================================================================
 * WHY MATERIALIZED: Multiple window functions are computed here, and the results
 * are used by two downstream CTEs (insert_by_day, insert_by_month). Materializing
 * computes all windows once and caches results.
 *
 * PURPOSE: Compute daily and monthly aggregations in a SINGLE PASS over the data.
 *          This is a performance optimization - instead of scanning the data twice
 *          (once for daily, once for monthly), we compute everything at once.
 *
 * "LAST OPERATION WINS" FOR AGGREGATED PERIODS:
 *   - ROW_NUMBER() OVER w_day_desc: rn_by_day=1 is the LAST operation of that day
 *   - ROW_NUMBER() OVER w_month_desc: rn_by_month=1 is the LAST operation of that month
 *
 *   The "balance" for a day/month is the END-OF-PERIOD balance, i.e., the balance
 *   after the last operation in that period.
 *
 * MIN/MAX TRACKING:
 *   - MIN(balance) OVER w_day_all: Lowest balance seen during that day
 *   - MAX(balance) OVER w_day_all: Highest balance seen during that day
 *   (Same for month)
 *
 *   These are useful for analytics: "What was the minimum HIVE balance for
 *   account X in January 2024?"
 *
 * WINDOW CLAUSE OPTIMIZATION:
 *   Four named windows with shared PARTITION BY allow PostgreSQL to potentially
 *   combine sorts and computations. The w_day_all and w_month_all windows don't
 *   need ORDER BY since MIN/MAX are computed over the entire partition.
 *
 * EXAMPLE:
 *   Account 42, NAI 21 (HIVE) on 2024-01-15:
 *   | source_op | balance | by_day     |
 *   |-----------|---------|------------|
 *   | 5001      | 1100    | 2024-01-15 |
 *   | 5002      | 1050    | 2024-01-15 |
 *   | 5003      | 1250    | 2024-01-15 |
 *
 *   After window functions:
 *   | source_op | balance | rn_by_day | min_balance_day | max_balance_day |
 *   |-----------|---------|-----------|-----------------|-----------------|
 *   | 5003      | 1250    | 1         | 1050            | 1250            | <- last op
 *   | 5002      | 1050    | 2         | 1050            | 1250            |
 *   | 5001      | 1100    | 3         | 1050            | 1250            |
 *
 *   Only rn_by_day=1 row will be inserted into balance_history_by_day.
 */
-- Combine all window functions and aggregations into single pass using WINDOW clause
-- This avoids multiple scans of join_created_at_to_balance_history
aggregated_balance_history AS MATERIALIZED (
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
  -- One-time filter: during MASSIVE sync the rollups are deferred to
  -- finalize_massive_sync() (see __maintain_period_rollups above); emptying this
  -- CTE makes the window sorts and both upserts below no-ops. The HBD interest
  -- accumulator reads join_created_at_to_balance_history directly and is not
  -- affected.
  WHERE __maintain_period_rollups
  WINDOW
    w_day_desc AS (PARTITION BY account_id, nai, by_day ORDER BY source_op DESC),
    w_month_desc AS (PARTITION BY account_id, nai, by_month ORDER BY source_op DESC),
    w_day_all AS (PARTITION BY account_id, nai, by_day),
    w_month_all AS (PARTITION BY account_id, nai, by_month)
),

/*
 * ===================================================================================
 * CTE: insert_account_balance_history_by_day
 * ===================================================================================
 * PURPOSE: Insert/update daily balance summaries.
 *
 * UPSERT with LEAST/GREATEST for MIN/MAX tracking:
 *
 *   ON CONFLICT ... DO UPDATE SET
 *     min_balance = LEAST(EXCLUDED.min_balance, acc_history.min_balance),
 *     max_balance = GREATEST(EXCLUDED.max_balance, acc_history.max_balance)
 *
 *   WHY LEAST/GREATEST:
 *   This handles the case where we're processing a block range that spans part of
 *   a day that was already partially processed. Example:
 *
 *   Day 2024-01-15:
 *   - Previous run processed ops with balances: 1000, 1100, 900
 *     Stored: min=900, max=1100, balance=900 (end of that run)
 *   - This run processes ops with balances: 850, 1200
 *     New data: min=850, max=1200, balance=1200 (end of day)
 *
 *   Using LEAST/GREATEST:
 *   - min_balance = LEAST(850, 900) = 850 (new minimum found)
 *   - max_balance = GREATEST(1200, 1100) = 1200 (new maximum found)
 *
 *   This ensures we capture the TRUE min/max across ALL operations that day,
 *   even if processed in multiple batches.
 *
 * FILTER: WHERE abh.rn_by_day = 1
 *   Only insert/update using the LAST operation of each day. The balance field
 *   represents the END-OF-DAY balance.
 */
-- insert aggregated balance history
-- Now using the combined aggregated_balance_history CTE which has all data in one scan
insert_account_balance_history_by_day AS (
  INSERT INTO balance_history_by_day AS acc_history
    (account, nai, source_op, updated_at, balance, min_balance, max_balance)
  SELECT
    abh.account_id,
    abh.nai,
    abh.source_op,
    abh.by_day,
    abh.balance,
    abh.min_balance_day,
    abh.max_balance_day
  FROM aggregated_balance_history abh
  WHERE abh.rn_by_day = 1
  ON CONFLICT ON CONSTRAINT pk_balance_history_by_day DO
  UPDATE SET
    source_op = EXCLUDED.source_op,
    balance = EXCLUDED.balance,
    min_balance = LEAST(EXCLUDED.min_balance, acc_history.min_balance),
    max_balance = GREATEST(EXCLUDED.max_balance, acc_history.max_balance)
  RETURNING (xmax = 0) as is_new_entry, acc_history.account
),

/*
 * ===================================================================================
 * CTE: insert_account_balance_history_by_month
 * ===================================================================================
 * PURPOSE: Insert/update monthly balance summaries. Identical pattern to daily.
 *
 * Same LEAST/GREATEST pattern ensures correct min/max across partial processing.
 * Monthly aggregates are useful for long-term trend analysis and reduce query load
 * compared to scanning daily records.
 *
 * FILTER: WHERE abh.rn_by_month = 1
 *   Only the LAST operation of each month is used, giving END-OF-MONTH balance.
 */
insert_account_balance_history_by_month AS (
  INSERT INTO balance_history_by_month AS acc_history
    (account, nai, source_op, updated_at, balance, min_balance, max_balance)
  SELECT
    abh.account_id,
    abh.nai,
    abh.source_op,
    abh.by_month,
    abh.balance,
    abh.min_balance_month,
    abh.max_balance_month
  FROM aggregated_balance_history abh
  WHERE abh.rn_by_month = 1
  ON CONFLICT ON CONSTRAINT pk_balance_history_by_month DO
  UPDATE SET
    source_op = EXCLUDED.source_op,
    balance = EXCLUDED.balance,
    min_balance = LEAST(EXCLUDED.min_balance, acc_history.min_balance),
    max_balance = GREATEST(EXCLUDED.max_balance, acc_history.max_balance)
  RETURNING (xmax = 0) as is_new_entry, acc_history.account
),

/*
 * ===================================================================================
 * HBD INTEREST STAGING
 * ===================================================================================
 * The per-account HBD interest accumulator (30-day reset fold; chain model:
 * hived database.cpp adjust_hbd_balance -> evaluate_hbd_interest) is sequentially
 * dependent, and its previous in-statement recursive-CTE implementation both cost
 * real time and poisoned the whole statement's row estimates (~5e17 estimated vs
 * ~1e4-1e5 actual). It now runs as a single ordered pass in
 * btracker_backend.accumulate_hbd_interest, invoked right after this statement.
 * Here we only stage the liquid-HBD running-balance rows it consumes.
 *
 * TIMESTAMP RULE and epoch-default semantics live in the helper; the HF25 gate
 * stays here: Hive core stopped updating liquid hbd_seconds at HF25
 * (database.cpp:3209); _hf_hbd_interest_block IS NULL means HF25 not yet applied
 * in this dataset. The boundary is inclusive (<=): ops within the activation
 * block still accrue.
 */
stage_hbd_rows AS (
  INSERT INTO _hbd_batch_rows
    (account_id, balance, source_op, source_op_block, balance_seq_no, trx_in_block)
  SELECT
    rls.account_id,
    rls.balance,
    rls.source_op,
    rls.source_op_block,
    rls.balance_seq_no,
    rls.trx_in_block
  FROM remove_latest_stored_balance_record rls
  WHERE rls.nai = _nai_hbd
    AND (_hf_hbd_interest_block IS NULL OR rls.source_op_block <= _hf_hbd_interest_block)
  RETURNING 1
)

/*
 * FINAL SELECT - Execute all CTEs and capture row counts for monitoring.
 *
 * PostgreSQL CTEs with INSERT/UPDATE are executed when their results are consumed.
 * By selecting count(*) from each insert CTE, we:
 *   1. Force execution of all the INSERT statements
 *   2. Capture how many rows were affected for logging/debugging
 *
 * These counts can help identify:
 *   - Empty block ranges (all counts = 0)
 *   - Data anomalies (unexpectedly high/low counts)
 *   - Performance characteristics (ratio of new vs updated rows)
 */
SELECT
  (SELECT count(*) FROM insert_account_balance_history) as balance_history,
  (SELECT count(*) FROM insert_current_account_balances) AS current_balances,
  (SELECT count(*) FROM insert_account_balance_history_by_day) as balance_history_by_day,
  (SELECT count(*) FROM insert_account_balance_history_by_month) AS balance_history_by_month,
  (SELECT count(*) FROM stage_hbd_rows) AS hbd_interest
INTO __balance_history, __current_balances, __balance_history_by_day,
     __balance_history_by_month, __hbd_interest;

-- Single-pass fold over the staged rows (replaces the in-statement recursive CTE).
__hbd_interest := btracker_backend.accumulate_hbd_interest( _from, _to, _hbd_interest_interval );

END
$$;

RESET ROLE;

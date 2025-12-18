SET ROLE btracker_owner;

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
AS
$$
DECLARE
 _result INT;
 __balance_history INT;
 __current_balances INT;
 __balance_history_by_day INT;
 __balance_history_by_month INT;
BEGIN
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
WITH balance_impacting_ops AS MATERIALIZED
(
  SELECT ot.id
  FROM hafd.operation_types ot
  WHERE ot.name IN (SELECT * FROM hive.get_balance_impacting_operations())
),

/*
 * ===================================================================================
 * CTE: ops_in_range
 * ===================================================================================
 * WHY MATERIALIZED: This is the foundation for all subsequent calculations.
 * Materializing prevents repeated execution of the expensive CROSS JOIN LATERAL
 * with hive.get_impacted_balances(). This function parses binary operation data
 * and extracts balance impacts - calling it multiple times would be catastrophic
 * for performance.
 *
 * DATA FLOW:
 *   1. Filter operations_view to only balance-impacting ops in our block range
 *   2. For each operation, call get_impacted_balances() to extract:
 *      - account_name: The account whose balance changed
 *      - asset_symbol_nai: The asset type (HIVE=21, HBD=13, VESTS=37)
 *      - amount: The DELTA (change), can be positive or negative
 *   3. Convert account_name to account_id via accounts_view lookup
 *
 * EDGE CASE - Hardfork handling:
 *   The ho.block_num > ah.block_num check (where ah.hardfork_num = 1) passes a
 *   boolean to get_impacted_balances() indicating whether we're past hardfork 1.
 *   This affects how certain legacy operations are interpreted.
 *
 * NOTE: 'balance' column here is actually a DELTA (amount of change), not an
 * absolute balance. The name is historical but the actual running balance is
 * computed later using SUM() OVER window functions.
 */
ops_in_range AS MATERIALIZED
(
  SELECT
    (SELECT av.id FROM accounts_view av WHERE av.name = get_impacted_balances.account_name) AS account_id,
    get_impacted_balances.asset_symbol_nai AS nai,
    get_impacted_balances.amount AS balance,
    ho.id AS source_op,
    ho.block_num AS source_op_block,
    ho.body_binary
  FROM operations_view ho --- APP specific view must be used, to correctly handle reversible part of the data.
  JOIN hafd.applied_hardforks ah ON ah.hardfork_num = 1
  CROSS JOIN hive.get_impacted_balances(
    ho.body_binary,
    ho.block_num > ah.block_num
  ) AS get_impacted_balances
  WHERE
    ho.op_type_id IN (SELECT id FROM balance_impacting_ops) AND
    ho.block_num BETWEEN _from AND _to
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
    cp.source_op_block
  FROM ops_in_range cp

  UNION ALL

-- latest stored balance is needed to replicate balance history
  SELECT
    glb.account_id,
    glb.nai,
    glb.balance,
    glb.balance_seq_no,
    glb.source_op,
    glb.source_op_block
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
 * WHY NAMED WINDOWS:
 *   Both window functions share the same PARTITION BY clause. Using named windows
 *   via the WINDOW clause improves readability and allows PostgreSQL to potentially
 *   share partition data between calculations. However, since ORDER BY differs
 *   (ascending vs descending), separate sorts are still required.
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

-- Combine both window function calculations in a single pass to avoid redundant sorting.
-- The row number must be calculated after the sum since operations like escrow_rejected_operation
-- can trigger multiple balance changes for the same asset, leading to multiple rows with the
-- same source_op and balance.
prepare_balance_history AS MATERIALIZED (
  SELECT
    ulb.account_id,
    ulb.nai,
    SUM(ulb.balance) OVER w_asc AS balance,
    SUM(ulb.balance_seq_no) OVER w_asc AS balance_seq_no,
    ulb.source_op,
    ulb.source_op_block,
    ROW_NUMBER() OVER w_desc AS rn
  FROM union_latest_balance_with_impacted_balances ulb
  WINDOW
    w_asc AS (PARTITION BY ulb.account_id, ulb.nai ORDER BY ulb.source_op, ulb.balance ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    w_desc AS (PARTITION BY ulb.account_id, ulb.nai ORDER BY ulb.source_op DESC, ulb.balance DESC)
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
    pbh.balance
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
    date_trunc('day', bv.created_at) AS by_day,
    date_trunc('month', bv.created_at) AS by_month
  FROM remove_latest_stored_balance_record rls
  JOIN hive.blocks_view bv ON bv.num = rls.source_op_block
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
  (SELECT count(*) FROM insert_account_balance_history_by_month) AS balance_history_by_month
INTO __balance_history, __current_balances, __balance_history_by_day,__balance_history_by_month;

END
$$;

RESET ROLE;

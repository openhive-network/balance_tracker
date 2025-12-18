SET ROLE btracker_owner;

/**
 * Processes transfer operations and aggregates statistics by hour, day, and month.
 * Handles: transfer_operation, fill_recurrent_transfer_operation, escrow_transfer_operation.
 * Uses "squashing" pattern for fast sync - processes block ranges in batches.
 */
CREATE OR REPLACE FUNCTION process_transfer_stats(_from INT, _to INT)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
-- Planner hints: higher collapse limits allow more join reordering for optimal plans
SET from_collapse_limit = 16
SET join_collapse_limit = 16
-- Disable JIT and bitmap scans: for this query pattern, sequential access is faster
SET jit = OFF
SET enable_bitmapscan = OFF
AS
$$
DECLARE
  -- Operation type IDs (looked up dynamically from hafd.operation_types)
  -- These correspond to Hive blockchain operation types we need to process
  _op_transfer                INT := btracker_backend.op_transfer();
  _op_fill_recurrent_transfer INT := btracker_backend.op_fill_recurrent_transfer();
  _op_escrow_transfer         INT := btracker_backend.op_escrow_transfer();
  -- Counters for diagnostic output (how many rows were inserted/updated per time bucket)
  __transfers_by_hour         INT;
  __transfers_by_day          INT;
  __transfers_by_month        INT;
BEGIN
  /**
   * ============================================================================
   * TRANSFER PROCESSING FLOW OVERVIEW
   * ============================================================================
   * 1. ops           - Fetch all transfer-related operations from the block range
   * 2. transfer_results / escrow_results - Extract transfer data from JSON bodies
   * 3. gather_transfers - Combine all transfer types into unified stream
   * 4. join_blocks_date - Add timestamp buckets (hour/day/month) from block metadata
   * 5. aggregated_stats - Single-pass aggregation using GROUPING SETS
   * 6. insert_trx_stats_by_* - Upsert into time-bucketed stats tables
   * ============================================================================
   */

  WITH ops AS MATERIALIZED (
    /**
     * CTE: ops - Fetch raw operations from the specified block range
     *
     * WHY MATERIALIZED: Forces PostgreSQL to execute this CTE first and store results.
     * This prevents the planner from inlining it multiple times (once for transfer_results,
     * once for escrow_results), which would cause duplicate table scans.
     *
     * The block range (_from, _to) enables the "squashing" pattern - processing many
     * blocks in a single function call for efficient bulk sync.
     */
    SELECT ov.body, ov.op_type_id, ov.block_num
    FROM operations_view ov
    WHERE
      ov.op_type_id IN (_op_transfer, _op_fill_recurrent_transfer, _op_escrow_transfer) AND
      ov.block_num BETWEEN _from AND _to
  ),
  transfer_results AS (
    /**
     * CTE: transfer_results - Process standard and recurrent transfers
     *
     * WHY CROSS JOIN LATERAL: The process_transfer function extracts fields from the
     * JSON body (nai, transfer_amount). LATERAL allows the function to reference
     * columns from the outer query (o.body) row-by-row.
     *
     * Handles op_transfer and op_fill_recurrent_transfer which share the same JSON structure.
     */
    SELECT o.block_num, r.nai, r.transfer_amount
    FROM ops o
    CROSS JOIN LATERAL btracker_backend.process_transfer(o.body) r
    WHERE o.op_type_id IN (_op_transfer, _op_fill_recurrent_transfer)
  ),
  escrow_results AS (
    /**
     * CTE: escrow_results - Process escrow transfers separately
     *
     * WHY SEPARATE CTE: Escrow transfers have a different JSON structure requiring
     * a dedicated parsing function (process_escrow_transfer). Keeping them separate
     * allows specialized extraction logic while maintaining the same output schema.
     */
    SELECT o.block_num, r.nai, r.transfer_amount
    FROM ops o
    CROSS JOIN LATERAL btracker_backend.process_escrow_transfer(o.body) r
    WHERE o.op_type_id = _op_escrow_transfer
  ),
  gather_transfers AS (
    /**
     * CTE: gather_transfers - Combine all transfer types into unified stream
     *
     * WHY UNION ALL (not UNION): We need ALL rows including duplicates - each transfer
     * must be counted. UNION would deduplicate, losing transfer data. UNION ALL is
     * also faster since it skips the sort/dedup step.
     */
    SELECT * FROM transfer_results
    UNION ALL
    SELECT * FROM escrow_results
  ),
  join_blocks_date AS MATERIALIZED (
    /**
     * CTE: join_blocks_date - Enrich transfers with time bucket dimensions
     *
     * WHY MATERIALIZED: This CTE is consumed by aggregated_stats which uses
     * GROUPING SETS. Materializing ensures the join happens once, and the
     * grouping can efficiently scan the materialized result multiple ways.
     *
     * WHY date_trunc: Creates consistent time bucket boundaries for aggregation.
     * All transfers within the same hour/day/month will have identical bucket values,
     * enabling proper GROUP BY.
     */
    SELECT
      gt.block_num,
      gt.nai,
      gt.transfer_amount,
      date_trunc('hour',bv.created_at) as by_hour,   -- Truncates to hour boundary (e.g., 14:00:00)
      date_trunc('day',bv.created_at) as by_day,     -- Truncates to day boundary (e.g., 2024-01-15 00:00:00)
      date_trunc('month',bv.created_at) as by_month  -- Truncates to month boundary (e.g., 2024-01-01 00:00:00)
    FROM gather_transfers gt
    JOIN hive.blocks_view bv ON gt.block_num = bv.num
  ),
  /**
   * ============================================================================
   * GROUPING SETS PATTERN - Single-scan multi-level aggregation
   * ============================================================================
   * Instead of running 3 separate GROUP BY queries (one per time bucket),
   * GROUPING SETS computes all three aggregations in a SINGLE PASS over the data.
   * This is critical for performance during fast sync when processing millions of rows.
   *
   * The GROUPING() function returns a bitmask indicating which columns are NULL
   * (aggregated away) in each result row:
   *
   *   GROUPING(by_hour, by_day, by_month) returns a 3-bit binary value:
   *   - Bit 2 (value 4): 1 if by_hour is aggregated (NULL), 0 if grouped
   *   - Bit 1 (value 2): 1 if by_day is aggregated (NULL), 0 if grouped
   *   - Bit 0 (value 1): 1 if by_month is aggregated (NULL), 0 if grouped
   *
   *   grp_level = 3 (binary 011): by_hour grouped, by_day/by_month NULL -> hourly stats
   *   grp_level = 5 (binary 101): by_day grouped, by_hour/by_month NULL -> daily stats
   *   grp_level = 6 (binary 110): by_month grouped, by_hour/by_day NULL -> monthly stats
   * ============================================================================
   */
  aggregated_stats AS MATERIALIZED (
    SELECT
      nai,
      by_hour,
      by_day,
      by_month,
      -- Aggregate transfer metrics: sum for totals, min/max for range, count for volume
      sum(transfer_amount)::BIGINT AS sum_transfer_amount,   -- Total value transferred
      max(transfer_amount)::BIGINT AS max_transfer_amount,   -- Largest single transfer
      min(transfer_amount)::BIGINT AS min_transfer_amount,   -- Smallest single transfer
      count(*)::INT AS transfer_count,                        -- Number of transfers
      max(block_num)::INT AS last_block_num,                  -- Most recent block (for tracking progress)
      GROUPING(by_hour, by_day, by_month) AS grp_level        -- Bitmask to identify aggregation level
    FROM join_blocks_date
    GROUP BY GROUPING SETS (
      (nai, by_hour),   -- Hourly aggregation: groups by asset type + hour
      (nai, by_day),    -- Daily aggregation: groups by asset type + day
      (nai, by_month)   -- Monthly aggregation: groups by asset type + month
    )
  ),
  /**
   * ============================================================================
   * INSERT WITH ON CONFLICT - Incremental update pattern
   * ============================================================================
   * Each insert CTE uses INSERT...ON CONFLICT DO UPDATE (upsert) to handle both:
   * 1. New time buckets: INSERT creates new row
   * 2. Existing time buckets: UPDATE merges new data with existing aggregates
   *
   * This enables incremental processing - we can call this function multiple times
   * for overlapping time periods and get correct cumulative results.
   *
   * Merge logic:
   * - sum: Add new sum to existing sum (cumulative total)
   * - max: Take GREATEST of new and existing (preserve all-time max)
   * - min: Take LEAST of new and existing (preserve all-time min)
   * - count: Add new count to existing count (cumulative count)
   * - last_block_num: Always use EXCLUDED (newest) to track progress
   * ============================================================================
   */

  -- grp_level = 3 (binary 011): by_hour is grouped (non-NULL), by_day and by_month are NULL
  -- This row represents hourly aggregation
  insert_trx_stats_by_hour AS (
    INSERT INTO transfer_stats_by_hour AS trx_agg
    (
      sum_transfer_amount,
      max_transfer_amount,
      min_transfer_amount,
      transfer_count,
      nai,
      last_block_num,
      updated_at
    )
    SELECT
      agg.sum_transfer_amount,
      agg.max_transfer_amount,
      agg.min_transfer_amount,
      agg.transfer_count,
      agg.nai,
      agg.last_block_num,
      agg.by_hour              -- The hour bucket becomes the updated_at timestamp
    FROM aggregated_stats agg
    WHERE agg.grp_level = 3    -- Filter to hourly aggregation rows only
    ON CONFLICT ON CONSTRAINT pk_transfer_stats_by_hour DO
    UPDATE SET
      -- Merge aggregates: add sums/counts, keep min/max extremes
      sum_transfer_amount = trx_agg.sum_transfer_amount + EXCLUDED.sum_transfer_amount,
      max_transfer_amount = GREATEST(EXCLUDED.max_transfer_amount, trx_agg.max_transfer_amount)::BIGINT,
      min_transfer_amount = LEAST(EXCLUDED.min_transfer_amount, trx_agg.min_transfer_amount)::BIGINT,
      transfer_count = trx_agg.transfer_count + EXCLUDED.transfer_count,
      last_block_num = EXCLUDED.last_block_num
    -- RETURNING: xmax is a PostgreSQL system column; xmax=0 means INSERT (new row), xmax!=0 means UPDATE
    RETURNING (xmax = 0) as is_new_entry, trx_agg.updated_at
  ),
  -- grp_level = 5 (binary 101): by_day is grouped (non-NULL), by_hour and by_month are NULL
  -- This row represents daily aggregation
  insert_trx_stats_by_day AS (
    INSERT INTO transfer_stats_by_day AS trx_agg
    (
      sum_transfer_amount,
      max_transfer_amount,
      min_transfer_amount,
      transfer_count,
      nai,
      last_block_num,
      updated_at
    )
    SELECT
      agg.sum_transfer_amount,
      agg.max_transfer_amount,
      agg.min_transfer_amount,
      agg.transfer_count,
      agg.nai,
      agg.last_block_num,
      agg.by_day               -- The day bucket becomes the updated_at timestamp
    FROM aggregated_stats agg
    WHERE agg.grp_level = 5    -- Filter to daily aggregation rows only
    ON CONFLICT ON CONSTRAINT pk_transfer_stats_by_day DO
    UPDATE SET
      -- Merge aggregates: add sums/counts, keep min/max extremes
      sum_transfer_amount = trx_agg.sum_transfer_amount + EXCLUDED.sum_transfer_amount,
      max_transfer_amount = GREATEST(EXCLUDED.max_transfer_amount, trx_agg.max_transfer_amount)::BIGINT,
      min_transfer_amount = LEAST(EXCLUDED.min_transfer_amount, trx_agg.min_transfer_amount)::BIGINT,
      transfer_count = trx_agg.transfer_count + EXCLUDED.transfer_count,
      last_block_num = EXCLUDED.last_block_num
    -- RETURNING: xmax is a PostgreSQL system column; xmax=0 means INSERT (new row), xmax!=0 means UPDATE
    RETURNING (xmax = 0) as is_new_entry, trx_agg.updated_at
  ),
  -- grp_level = 6 (binary 110): by_month is grouped (non-NULL), by_hour and by_day are NULL
  -- This row represents monthly aggregation
  insert_trx_stats_by_month AS (
    INSERT INTO transfer_stats_by_month AS trx_agg
    (
      sum_transfer_amount,
      max_transfer_amount,
      min_transfer_amount,
      transfer_count,
      nai,
      last_block_num,
      updated_at
    )
    SELECT
      agg.sum_transfer_amount,
      agg.max_transfer_amount,
      agg.min_transfer_amount,
      agg.transfer_count,
      agg.nai,
      agg.last_block_num,
      agg.by_month             -- The month bucket becomes the updated_at timestamp
    FROM aggregated_stats agg
    WHERE agg.grp_level = 6    -- Filter to monthly aggregation rows only
    ON CONFLICT ON CONSTRAINT pk_transfer_stats_by_month DO
    UPDATE SET
      -- Merge aggregates: add sums/counts, keep min/max extremes
      sum_transfer_amount = trx_agg.sum_transfer_amount + EXCLUDED.sum_transfer_amount,
      max_transfer_amount = GREATEST(EXCLUDED.max_transfer_amount, trx_agg.max_transfer_amount)::BIGINT,
      min_transfer_amount = LEAST(EXCLUDED.min_transfer_amount, trx_agg.min_transfer_amount)::BIGINT,
      transfer_count = trx_agg.transfer_count + EXCLUDED.transfer_count,
      last_block_num = EXCLUDED.last_block_num
    -- RETURNING: xmax is a PostgreSQL system column; xmax=0 means INSERT (new row), xmax!=0 means UPDATE
    RETURNING (xmax = 0) as is_new_entry, trx_agg.updated_at
  )
  /**
   * Final SELECT: Collect row counts from each insert CTE.
   * This materializes the CTEs (they won't execute without being referenced)
   * and provides diagnostic counts for monitoring sync progress.
   */
  SELECT
    (SELECT count(*) FROM insert_trx_stats_by_hour) as by_hour,
    (SELECT count(*) FROM insert_trx_stats_by_day) as by_day,
    (SELECT count(*) FROM insert_trx_stats_by_month) AS by_month
  INTO __transfers_by_hour, __transfers_by_day, __transfers_by_month;

END
$$;

RESET ROLE;

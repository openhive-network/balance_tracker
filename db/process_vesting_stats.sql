SET ROLE btracker_owner;

/**
 * Aggregates vesting (power-up / power-down) operations into pre-computed
 * GLOBAL daily and monthly stats tables (vesting_stats_by_day,
 * vesting_stats_by_month). Mirrors process_transfer_stats: single
 * MATERIALIZED scan + GROUPING SETS for multi-level aggregation + UPSERT
 * with additive merge.
 *
 * Per-account history and per-account aggregation are populated by a
 * separate function (process_account_vesting_stats) to keep concerns
 * separate.
 *
 * Stored TALL: one row per (kind, period) with generic op_count / hive_amount /
 * vests_amount, so the API pivots kinds into its wide shape at read time.
 * Tracked operations (no per-account fan-out here, so NO kind=4):
 *   - transfer_to_vesting        -> kind=1 (power_up):        op_count, hive=amount
 *   - withdraw_vesting           -> kind=2 (power_down_init): op_count, vests=vesting_shares
 *                                   (excluding amount=0 cancellations)
 *   - fill_vesting_withdraw      -> kind=3 (power_down_fill): op_count, vests=withdrawn,
 *                                   hive=deposited only when deposited NAI=HIVE
 *
 * HF1 precision: pre-HF1 VESTS amounts are scaled by vests_precision_multiplier()
 * before aggregation, matching the treatment in process_withdrawals.
 *
 * No VESTS->HIVE conversion is applied at withdraw_vesting init time: the chain
 * does that conversion at fill time, so HIVE numbers come from
 * fill_vesting_withdraw.deposited (which is realised HIVE; routed-to-VESTS
 * fills carry hive_amount=0).
 */
CREATE OR REPLACE FUNCTION process_vesting_stats(_from INT, _to INT)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
SET enable_bitmapscan = OFF
AS
$$
DECLARE
  _op_transfer_to_vesting    INT := btracker_backend.op_transfer_to_vesting();
  _op_withdraw_vesting       INT := btracker_backend.op_withdraw_vesting();
  _op_fill_vesting_withdraw  INT := btracker_backend.op_fill_vesting_withdraw();
  _hf_vests_precision        INT := btracker_backend.hf_vests_precision();
  _hf_vests_precision_block  INT;
  __by_day_count             INT;
  __by_month_count           INT;
BEGIN
  -- HF1 boundary: pre-HF1 VESTS need ×10^6 multiplier; NULL when HF not applied yet.
  SELECT MAX(block_num)
  INTO _hf_vests_precision_block
  FROM hafd.applied_hardforks
  WHERE hardfork_num = _hf_vests_precision;

  WITH ops AS MATERIALIZED (
    -- Single scan over the pre-fetched batch for the three op types we care about.
    SELECT
      ov.body_value AS body,
      ov.op_type_id,
      ov.block_num
    FROM _btracker_ops_batch ov
    WHERE
      ov.op_type_id IN (_op_transfer_to_vesting, _op_withdraw_vesting, _op_fill_vesting_withdraw) AND
      ov.block_num BETWEEN _from AND _to
  ),
  parsed AS MATERIALIZED (
    -- One row per op (already filters cancellations). HIVE values are satoshi
    -- (×1000); VESTS values are scaled by pre_hf1 multiplier.
    SELECT
      o.block_num,
      (CASE o.op_type_id
        WHEN _op_transfer_to_vesting    THEN 1
        WHEN _op_withdraw_vesting       THEN 2
        WHEN _op_fill_vesting_withdraw  THEN 3
      END)::SMALLINT AS kind,
      (CASE
        WHEN o.op_type_id = _op_transfer_to_vesting
          THEN ((o.body)->'amount'->>'amount')::BIGINT
        WHEN o.op_type_id = _op_fill_vesting_withdraw
         AND ((o.body)->'deposited'->>'precision')::INT = 3
          THEN ((o.body)->'deposited'->>'amount')::BIGINT
        ELSE 0::BIGINT
      END) AS hive_amount,
      (CASE
        WHEN o.op_type_id = _op_withdraw_vesting
          THEN ((o.body)->'vesting_shares'->>'amount')::NUMERIC
               * btracker_backend.vests_precision_multiplier(o.block_num > _hf_vests_precision_block)
        WHEN o.op_type_id = _op_fill_vesting_withdraw
          THEN ((o.body)->'withdrawn'->>'amount')::NUMERIC
               * btracker_backend.vests_precision_multiplier(o.block_num > _hf_vests_precision_block)
        ELSE 0::NUMERIC
      END) AS vests_amount
    FROM ops o
    WHERE NOT (
      o.op_type_id = _op_withdraw_vesting
      AND ((o.body)->'vesting_shares'->>'amount')::BIGINT = 0
    )
  ),
  join_blocks_date AS MATERIALIZED (
    SELECT
      p.block_num,
      p.kind,
      p.hive_amount,
      p.vests_amount,
      date_trunc('day',   bv.created_at) AS by_day,
      date_trunc('month', bv.created_at) AS by_month
    FROM parsed p
    JOIN hive.blocks_view bv ON bv.num = p.block_num
  ),
  /**
   * GROUPING SETS over (kind, by_day) and (kind, by_month) — single-pass two-level
   * aggregation, one output row per (kind, period). grp_level = GROUPING(by_day, by_month):
   *   1 (binary 01): by_day grouped, by_month NULL  -> daily row
   *   2 (binary 10): by_month grouped, by_day NULL  -> monthly row
   */
  aggregated_stats AS MATERIALIZED (
    SELECT
      kind,
      by_day,
      by_month,
      GROUPING(by_day, by_month) AS grp_level,
      COUNT(*)::INT                          AS op_count,
      COALESCE(SUM(hive_amount),  0)::BIGINT  AS hive_amount,
      COALESCE(SUM(vests_amount), 0)::NUMERIC AS vests_amount,
      MAX(block_num)::INT                     AS last_block_num
    FROM join_blocks_date
    GROUP BY GROUPING SETS (
      (kind, by_day),
      (kind, by_month)
    )
  ),
  insert_by_day AS (
    INSERT INTO vesting_stats_by_day AS vs (
      kind, updated_at, op_count, hive_amount, vests_amount, last_block_num
    )
    SELECT
      agg.kind, agg.by_day, agg.op_count, agg.hive_amount, agg.vests_amount, agg.last_block_num
    FROM aggregated_stats agg
    WHERE agg.grp_level = 1
    ON CONFLICT ON CONSTRAINT pk_vesting_stats_by_day DO UPDATE SET
      op_count       = vs.op_count     + EXCLUDED.op_count,
      hive_amount    = vs.hive_amount  + EXCLUDED.hive_amount,
      vests_amount   = vs.vests_amount + EXCLUDED.vests_amount,
      last_block_num = EXCLUDED.last_block_num
    RETURNING (xmax = 0) AS is_new_entry
  ),
  insert_by_month AS (
    INSERT INTO vesting_stats_by_month AS vs (
      kind, updated_at, op_count, hive_amount, vests_amount, last_block_num
    )
    SELECT
      agg.kind, agg.by_month, agg.op_count, agg.hive_amount, agg.vests_amount, agg.last_block_num
    FROM aggregated_stats agg
    WHERE agg.grp_level = 2
    ON CONFLICT ON CONSTRAINT pk_vesting_stats_by_month DO UPDATE SET
      op_count       = vs.op_count     + EXCLUDED.op_count,
      hive_amount    = vs.hive_amount  + EXCLUDED.hive_amount,
      vests_amount   = vs.vests_amount + EXCLUDED.vests_amount,
      last_block_num = EXCLUDED.last_block_num
    RETURNING (xmax = 0) AS is_new_entry
  )
  SELECT
    (SELECT count(*) FROM insert_by_day)   AS by_day,
    (SELECT count(*) FROM insert_by_month) AS by_month
  INTO __by_day_count, __by_month_count;
END
$$;

RESET ROLE;

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
 * Tracked operations:
 *   - transfer_to_vesting        -> power_up_count / power_up_hive
 *   - withdraw_vesting           -> power_down_init_count / power_down_init_vests
 *                                   (excluding amount=0 cancellations)
 *   - fill_vesting_withdraw      -> power_down_fill_count / power_down_fill_vests
 *                                   (always, includes routed-to-VESTS)
 *                                   power_down_fill_hive only when deposited NAI=HIVE
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
   * GROUPING SETS over (by_day) and (by_month) — single-pass two-level
   * aggregation. grp_level = GROUPING(by_day, by_month):
   *   1 (binary 01): by_day grouped, by_month NULL  -> daily row
   *   2 (binary 10): by_month grouped, by_day NULL  -> monthly row
   */
  aggregated_stats AS MATERIALIZED (
    SELECT
      by_day,
      by_month,
      GROUPING(by_day, by_month) AS grp_level,
      COUNT(*) FILTER (WHERE kind = 1)::INT                                     AS power_up_count,
      COALESCE(SUM(hive_amount)  FILTER (WHERE kind = 1), 0)::BIGINT            AS power_up_hive,
      COUNT(*) FILTER (WHERE kind = 2)::INT                                     AS power_down_init_count,
      COALESCE(SUM(vests_amount) FILTER (WHERE kind = 2), 0)::NUMERIC           AS power_down_init_vests,
      COUNT(*) FILTER (WHERE kind = 3)::INT                                     AS power_down_fill_count,
      COALESCE(SUM(vests_amount) FILTER (WHERE kind = 3), 0)::NUMERIC           AS power_down_fill_vests,
      COALESCE(SUM(hive_amount)  FILTER (WHERE kind = 3), 0)::BIGINT            AS power_down_fill_hive,
      MAX(block_num)::INT                                                        AS last_block_num
    FROM join_blocks_date
    GROUP BY GROUPING SETS (
      (by_day),
      (by_month)
    )
  ),
  insert_by_day AS (
    INSERT INTO vesting_stats_by_day AS vs (
      updated_at,
      power_up_count, power_up_hive,
      power_down_init_count, power_down_init_vests,
      power_down_fill_count, power_down_fill_vests, power_down_fill_hive,
      last_block_num
    )
    SELECT
      agg.by_day,
      agg.power_up_count, agg.power_up_hive,
      agg.power_down_init_count, agg.power_down_init_vests,
      agg.power_down_fill_count, agg.power_down_fill_vests, agg.power_down_fill_hive,
      agg.last_block_num
    FROM aggregated_stats agg
    WHERE agg.grp_level = 1
    ON CONFLICT ON CONSTRAINT pk_vesting_stats_by_day DO UPDATE SET
      power_up_count        = vs.power_up_count        + EXCLUDED.power_up_count,
      power_up_hive         = vs.power_up_hive         + EXCLUDED.power_up_hive,
      power_down_init_count = vs.power_down_init_count + EXCLUDED.power_down_init_count,
      power_down_init_vests = vs.power_down_init_vests + EXCLUDED.power_down_init_vests,
      power_down_fill_count = vs.power_down_fill_count + EXCLUDED.power_down_fill_count,
      power_down_fill_vests = vs.power_down_fill_vests + EXCLUDED.power_down_fill_vests,
      power_down_fill_hive  = vs.power_down_fill_hive  + EXCLUDED.power_down_fill_hive,
      last_block_num        = EXCLUDED.last_block_num
    RETURNING (xmax = 0) AS is_new_entry
  ),
  insert_by_month AS (
    INSERT INTO vesting_stats_by_month AS vs (
      updated_at,
      power_up_count, power_up_hive,
      power_down_init_count, power_down_init_vests,
      power_down_fill_count, power_down_fill_vests, power_down_fill_hive,
      last_block_num
    )
    SELECT
      agg.by_month,
      agg.power_up_count, agg.power_up_hive,
      agg.power_down_init_count, agg.power_down_init_vests,
      agg.power_down_fill_count, agg.power_down_fill_vests, agg.power_down_fill_hive,
      agg.last_block_num
    FROM aggregated_stats agg
    WHERE agg.grp_level = 2
    ON CONFLICT ON CONSTRAINT pk_vesting_stats_by_month DO UPDATE SET
      power_up_count        = vs.power_up_count        + EXCLUDED.power_up_count,
      power_up_hive         = vs.power_up_hive         + EXCLUDED.power_up_hive,
      power_down_init_count = vs.power_down_init_count + EXCLUDED.power_down_init_count,
      power_down_init_vests = vs.power_down_init_vests + EXCLUDED.power_down_init_vests,
      power_down_fill_count = vs.power_down_fill_count + EXCLUDED.power_down_fill_count,
      power_down_fill_vests = vs.power_down_fill_vests + EXCLUDED.power_down_fill_vests,
      power_down_fill_hive  = vs.power_down_fill_hive  + EXCLUDED.power_down_fill_hive,
      last_block_num        = EXCLUDED.last_block_num
    RETURNING (xmax = 0) AS is_new_entry
  )
  SELECT
    (SELECT count(*) FROM insert_by_day)   AS by_day,
    (SELECT count(*) FROM insert_by_month) AS by_month
  INTO __by_day_count, __by_month_count;
END
$$;

RESET ROLE;

SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION process_transfer_stats(_from INT, _to INT)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
SET enable_bitmapscan = OFF
AS
$$
DECLARE
  __transfers_by_hour INT;
  __transfers_by_day INT;
  __transfers_by_month INT;
BEGIN
  WITH gather_transfers AS (
    SELECT 
      ov.block_num,
      git.nai,
      git.transfer_amount
    FROM operations_view ov
    CROSS JOIN btracker_backend.get_impacted_transfers(ov.body, ov.op_type_id) git
    WHERE
      ov.op_type_id in (2,83,27) AND
      ov.block_num BETWEEN _from AND _to
  ),
  join_blocks_date AS MATERIALIZED (
    SELECT 
      gt.block_num, 
      gt.nai,
      gt.transfer_amount,
      date_trunc('hour',bv.created_at) as by_hour, 
      date_trunc('day',bv.created_at) as by_day, 
      date_trunc('month',bv.created_at) as by_month
    FROM gather_transfers gt 
    JOIN hive.blocks_view bv ON gt.block_num = bv.num
  ),
  -- Use GROUPING SETS to compute all three aggregation levels in a single scan
  -- grp_level values: 4 = by_hour (only by_hour is non-null), 2 = by_day, 1 = by_month
  aggregated_stats AS MATERIALIZED (
    SELECT
      nai,
      by_hour,
      by_day,
      by_month,
      sum(transfer_amount)::BIGINT AS sum_transfer_amount,
      max(transfer_amount)::BIGINT AS max_transfer_amount,
      min(transfer_amount)::BIGINT AS min_transfer_amount,
      count(*)::INT AS transfer_count,
      max(block_num)::INT AS last_block_num,
      GROUPING(by_hour, by_day, by_month) AS grp_level
    FROM join_blocks_date
    GROUP BY GROUPING SETS (
      (nai, by_hour),
      (nai, by_day),
      (nai, by_month)
    )
  ),
  -- grp_level = 3 (binary 011) means by_hour is set (by_day, by_month are NULL)
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
      agg.by_hour
    FROM aggregated_stats agg
    WHERE agg.grp_level = 3
    ON CONFLICT ON CONSTRAINT pk_transfer_stats_by_hour DO
    UPDATE SET
      sum_transfer_amount = trx_agg.sum_transfer_amount + EXCLUDED.sum_transfer_amount,
      max_transfer_amount = GREATEST(EXCLUDED.max_transfer_amount, trx_agg.max_transfer_amount)::BIGINT,
      min_transfer_amount = LEAST(EXCLUDED.min_transfer_amount, trx_agg.min_transfer_amount)::BIGINT,
      transfer_count = trx_agg.transfer_count + EXCLUDED.transfer_count,
      last_block_num = EXCLUDED.last_block_num
    RETURNING (xmax = 0) as is_new_entry, trx_agg.updated_at
  ),
  -- grp_level = 5 (binary 101) means by_day is set (by_hour, by_month are NULL)
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
      agg.by_day
    FROM aggregated_stats agg
    WHERE agg.grp_level = 5
    ON CONFLICT ON CONSTRAINT pk_transfer_stats_by_day DO
    UPDATE SET
      sum_transfer_amount = trx_agg.sum_transfer_amount + EXCLUDED.sum_transfer_amount,
      max_transfer_amount = GREATEST(EXCLUDED.max_transfer_amount, trx_agg.max_transfer_amount)::BIGINT,
      min_transfer_amount = LEAST(EXCLUDED.min_transfer_amount, trx_agg.min_transfer_amount)::BIGINT,
      transfer_count = trx_agg.transfer_count + EXCLUDED.transfer_count,
      last_block_num = EXCLUDED.last_block_num
    RETURNING (xmax = 0) as is_new_entry, trx_agg.updated_at
  ),
  -- grp_level = 6 (binary 110) means by_month is set (by_hour, by_day are NULL)
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
      agg.by_month
    FROM aggregated_stats agg
    WHERE agg.grp_level = 6
    ON CONFLICT ON CONSTRAINT pk_transfer_stats_by_month DO
    UPDATE SET
      sum_transfer_amount = trx_agg.sum_transfer_amount + EXCLUDED.sum_transfer_amount,
      max_transfer_amount = GREATEST(EXCLUDED.max_transfer_amount, trx_agg.max_transfer_amount)::BIGINT,
      min_transfer_amount = LEAST(EXCLUDED.min_transfer_amount, trx_agg.min_transfer_amount)::BIGINT,
      transfer_count = trx_agg.transfer_count + EXCLUDED.transfer_count,
      last_block_num = EXCLUDED.last_block_num
    RETURNING (xmax = 0) as is_new_entry, trx_agg.updated_at
  )
  SELECT
    (SELECT count(*) FROM insert_trx_stats_by_hour) as by_hour,
    (SELECT count(*) FROM insert_trx_stats_by_day) as by_day,
    (SELECT count(*) FROM insert_trx_stats_by_month) AS by_month
  INTO __transfers_by_hour, __transfers_by_day, __transfers_by_month;

END
$$;

RESET ROLE;

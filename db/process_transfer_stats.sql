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
  group_by_hour AS (
    SELECT 
      sum(transfer_amount)::BIGINT AS sum_transfer_amount,
      avg(transfer_amount)::BIGINT AS avg_transfer_amount,
      max(transfer_amount)::BIGINT AS max_transfer_amount,
      min(transfer_amount)::BIGINT AS min_transfer_amount,
      count(*)::INT AS transfer_count,
      max(block_num)::INT AS last_block_num,
      nai,
      by_hour
    FROM join_blocks_date
    GROUP BY by_hour, nai
  ),
  group_by_day AS (
    SELECT 
      sum(transfer_amount)::BIGINT AS sum_transfer_amount,
      avg(transfer_amount)::BIGINT AS avg_transfer_amount,
      max(transfer_amount)::BIGINT AS max_transfer_amount,
      min(transfer_amount)::BIGINT AS min_transfer_amount,
      count(*)::INT AS transfer_count,
      max(block_num)::INT AS last_block_num,
      nai,
      by_day
    FROM join_blocks_date
    GROUP BY by_day, nai
  ),
  group_by_month AS (
    SELECT 
      sum(transfer_amount)::BIGINT AS sum_transfer_amount,
      avg(transfer_amount)::BIGINT AS avg_transfer_amount,
      max(transfer_amount)::BIGINT AS max_transfer_amount,
      min(transfer_amount)::BIGINT AS min_transfer_amount,
      count(*)::INT AS transfer_count,
      max(block_num)::INT AS last_block_num,
      nai,
      by_month
    FROM join_blocks_date
    GROUP BY by_month, nai
  ),
  insert_trx_stats_by_hour AS (
    INSERT INTO transfer_stats_by_hour AS trx_agg
    (
      sum_transfer_amount,
      avg_transfer_amount,
      max_transfer_amount,
      min_transfer_amount,
      transfer_count,
      nai,
      last_block_num,
      updated_at
    )
    SELECT 
      gh.sum_transfer_amount,
      gh.avg_transfer_amount,
      gh.max_transfer_amount,
      gh.min_transfer_amount,
      gh.transfer_count,
      gh.nai,
      gh.last_block_num,
      gh.by_hour
    FROM group_by_hour gh
    ON CONFLICT ON CONSTRAINT pk_transfer_stats_by_hour DO 
    UPDATE SET 
      sum_transfer_amount = trx_agg.sum_transfer_amount + EXCLUDED.sum_transfer_amount,
      avg_transfer_amount = ((EXCLUDED.avg_transfer_amount + trx_agg.avg_transfer_amount) / 2)::INT,
      max_transfer_amount = GREATEST(EXCLUDED.max_transfer_amount, trx_agg.max_transfer_amount)::INT,
      min_transfer_amount = LEAST(EXCLUDED.min_transfer_amount, trx_agg.min_transfer_amount)::INT,
      transfer_count = trx_agg.transfer_count + EXCLUDED.transfer_count,
      last_block_num = EXCLUDED.last_block_num
    RETURNING (xmax = 0) as is_new_entry, trx_agg.updated_at
  ),
  insert_trx_stats_by_day AS (
    INSERT INTO transfer_stats_by_day AS trx_agg
    (
      sum_transfer_amount,
      avg_transfer_amount,
      max_transfer_amount,
      min_transfer_amount,
      transfer_count,
      nai,
      last_block_num,
      updated_at
    )
    SELECT 
      gh.sum_transfer_amount,
      gh.avg_transfer_amount,
      gh.max_transfer_amount,
      gh.min_transfer_amount,
      gh.transfer_count,
      gh.nai,
      gh.last_block_num,
      gh.by_day
    FROM group_by_day gh
    ON CONFLICT ON CONSTRAINT pk_transfer_stats_by_day DO 
    UPDATE SET 
      sum_transfer_amount = trx_agg.sum_transfer_amount + EXCLUDED.sum_transfer_amount,
      avg_transfer_amount = ((EXCLUDED.avg_transfer_amount + trx_agg.avg_transfer_amount) / 2)::INT,
      max_transfer_amount = GREATEST(EXCLUDED.max_transfer_amount, trx_agg.max_transfer_amount)::INT,
      min_transfer_amount = LEAST(EXCLUDED.min_transfer_amount, trx_agg.min_transfer_amount)::INT,
      transfer_count = trx_agg.transfer_count + EXCLUDED.transfer_count,
      last_block_num = EXCLUDED.last_block_num
    RETURNING (xmax = 0) as is_new_entry, trx_agg.updated_at
  ),
  insert_trx_stats_by_month AS (
    INSERT INTO transfer_stats_by_month AS trx_agg
    (
      sum_transfer_amount,
      avg_transfer_amount,
      max_transfer_amount,
      min_transfer_amount,
      transfer_count,
      nai,
      last_block_num,
      updated_at
    )
    SELECT 
      gh.sum_transfer_amount,
      gh.avg_transfer_amount,
      gh.max_transfer_amount,
      gh.min_transfer_amount,
      gh.transfer_count,
      gh.nai,
      gh.last_block_num,
      gh.by_month
    FROM group_by_month gh
    ON CONFLICT ON CONSTRAINT pk_transfer_stats_by_month DO 
    UPDATE SET 
      sum_transfer_amount = trx_agg.sum_transfer_amount + EXCLUDED.sum_transfer_amount,
      avg_transfer_amount = ((EXCLUDED.avg_transfer_amount + trx_agg.avg_transfer_amount) / 2)::INT,
      max_transfer_amount = GREATEST(EXCLUDED.max_transfer_amount, trx_agg.max_transfer_amount)::INT,
      min_transfer_amount = LEAST(EXCLUDED.min_transfer_amount, trx_agg.min_transfer_amount)::INT,
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

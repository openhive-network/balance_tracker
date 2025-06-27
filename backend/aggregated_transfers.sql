-- noqa: disable=AL01, AM05

SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.transfer_history_return CASCADE;
CREATE TYPE btracker_backend.transfer_history_return AS (
    updated_at TIMESTAMP,
    sum_transfer_amount BIGINT,
    avg_transfer_amount BIGINT,
    max_transfer_amount BIGINT,
    min_transfer_amount BIGINT,
    transfer_count INT,
    source_op_block INT
);

CREATE OR REPLACE FUNCTION btracker_backend.transfer_stats_by_year(
    _nai INT,
    _from TIMESTAMP,
    _to TIMESTAMP
)
RETURNS SETOF btracker_backend.transfer_history_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  WITH get_year AS (
      SELECT
          sum_transfer_amount,
          avg_transfer_amount,
          max_transfer_amount,
          min_transfer_amount,
          transfer_count,
          last_block_num,
          updated_at,
          DATE_TRUNC('year', updated_at) AS by_year
      FROM btracker_app.transfer_stats_by_month
      WHERE updated_at BETWEEN _from AND _to AND nai = _nai
  )
  SELECT
      by_year AS updated_at,
      SUM(sum_transfer_amount) AS sum_transfer_amount,
      AVG(avg_transfer_amount) AS avg_transfer_amount,
      MAX(max_transfer_amount) AS max_transfer_amount,
      MIN(min_transfer_amount) AS min_transfer_amount,
      SUM(transfer_count) AS transfer_count,
      MAX(last_block_num) AS last_block_num
  FROM get_year
  GROUP BY by_year;
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.get_transfer_stats(
    _nai INT,
    _granularity_hourly btracker_backend.granularity_hourly,
    _from TIMESTAMP,
    _to TIMESTAMP
)
RETURNS SETOF btracker_backend.transfer_history_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  IF _granularity_hourly = 'hourly' THEN
    RETURN QUERY 
      SELECT 
        th.updated_at,
        th.sum_transfer_amount,
        th.avg_transfer_amount,
        th.max_transfer_amount,
        th.min_transfer_amount,
        th.transfer_count,
        th.last_block_num
      FROM transfer_stats_by_hour th
      WHERE th.nai = _nai AND th.updated_at BETWEEN _from AND _to;

  ELSIF _granularity_hourly = 'daily' THEN
    RETURN QUERY 
      SELECT 
        td.updated_at,
        td.sum_transfer_amount,
        td.avg_transfer_amount,
        td.max_transfer_amount,
        td.min_transfer_amount,
        td.transfer_count,
        td.last_block_num
      FROM transfer_stats_by_day td
      WHERE td.nai = _nai AND td.updated_at BETWEEN _from AND _to;

  ELSIF _granularity_hourly = 'monthly' THEN
    RETURN QUERY 
      SELECT 
        tm.updated_at,
        tm.sum_transfer_amount,
        tm.avg_transfer_amount,
        tm.max_transfer_amount,
        tm.min_transfer_amount,
        tm.transfer_count,
        tm.last_block_num
      FROM transfer_stats_by_month tm
      WHERE tm.nai = _nai AND tm.updated_at BETWEEN _from AND _to;

  ELSIF _granularity_hourly = 'yearly' THEN
    RETURN QUERY 
      SELECT 
        ty.updated_at,
        ty.sum_transfer_amount,
        ty.avg_transfer_amount,
        ty.max_transfer_amount,
        ty.min_transfer_amount,
        ty.transfer_count,
        ty.source_op_block
      FROM btracker_backend.transfer_stats_by_year(_nai, _from, _to) ty;

  ELSE
    RAISE EXCEPTION 'Unsupported granularity: %', _granularity_hourly;
  END IF;

END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.get_transfer_aggregation(
    _nai INT,
    _granularity_hourly btracker_backend.granularity_hourly,
    _direction btracker_backend.sort_direction,
    _from_block INT,
    _to_block INT
)
RETURNS SETOF btracker_backend.transfer_history_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
    __from INT;
    __to INT;
    __from_timestamp TIMESTAMP;
    __to_timestamp TIMESTAMP;
    __granularity TEXT;
    __one_period INTERVAL;
    __btracker_current_block INT := (SELECT current_block_num FROM hafd.contexts WHERE name = 'btracker_app');
BEGIN
  SELECT from_block, to_block
  INTO __from, __to
  FROM btracker_backend.blocksearch_range(_from_block, _to_block, __btracker_current_block);

  __granularity := (
    CASE 
      WHEN _granularity_hourly = 'hourly' THEN 'hour'
      WHEN _granularity_hourly = 'daily' THEN 'day'
      WHEN _granularity_hourly = 'monthly' THEN 'month'
      WHEN _granularity_hourly = 'yearly' THEN 'year'
      ELSE NULL
    END
  );

  __from_timestamp := DATE_TRUNC(__granularity,(SELECT b.created_at FROM hive.blocks_view b WHERE b.num = _from_block)::TIMESTAMP);
  __to_timestamp := DATE_TRUNC(__granularity, (SELECT b.created_at FROM hive.blocks_view b WHERE b.num = _to_block)::TIMESTAMP);


  __one_period := ('1 ' || __granularity )::INTERVAL;

  RETURN QUERY (
    WITH date_series AS (
      SELECT generate_series(__from_timestamp, __to_timestamp, __one_period) AS date
    ),
    get_daily_aggregation AS MATERIALIZED (
      SELECT 
        bh.updated_at,
        bh.sum_transfer_amount,
        bh.avg_transfer_amount,
        bh.max_transfer_amount,
        bh.min_transfer_amount,
        bh.transfer_count,
        bh.last_block_num
      FROM btracker_backend.get_transfer_stats(_nai, _granularity_hourly, __from_timestamp, __to_timestamp) bh
    ),
    transfer_records AS (
      SELECT 
        ds.date,
        COALESCE(bh.sum_transfer_amount,0) AS sum_transfer_amount,
        COALESCE(bh.avg_transfer_amount,0) AS avg_transfer_amount,
        COALESCE(bh.max_transfer_amount,0) AS max_transfer_amount,
        COALESCE(bh.min_transfer_amount,0) AS min_transfer_amount,
        COALESCE(bh.transfer_count,0) AS transfer_count,
        COALESCE(bh.last_block_num,NULL) AS last_block_num
      FROM date_series ds
      LEFT JOIN get_daily_aggregation bh ON ds.date = bh.updated_at
    )
    SELECT 
      LEAST(fb.date + __one_period - INTERVAL '1 second', CURRENT_TIMESTAMP)::TIMESTAMP AS adjusted_date,
      fb.sum_transfer_amount::BIGINT,
      fb.avg_transfer_amount::BIGINT,
      fb.max_transfer_amount::BIGINT,
      fb.min_transfer_amount::BIGINT,
      fb.transfer_count::INT,
      fb.last_block_num::INT
    FROM transfer_records fb
    ORDER BY
      (CASE WHEN _direction = 'desc' THEN fb.date ELSE NULL END) DESC,
      (CASE WHEN _direction = 'asc' THEN fb.date ELSE NULL END) ASC
  );
END
$$;

RESET ROLE;

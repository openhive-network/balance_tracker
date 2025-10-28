-- noqa: disable=AL01, AM05, LT02

SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.transfer_history_return CASCADE;
CREATE TYPE btracker_backend.transfer_history_return AS (
    updated_at TIMESTAMP,
    sum_transfer_amount BIGINT,
    avg_transfer_amount BIGINT,
    max_transfer_amount BIGINT,
    min_transfer_amount BIGINT,
    transfer_count INT,
    last_block_num INT
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
  RETURN QUERY
    WITH get_year AS (
        SELECT
            sum_transfer_amount,
            max_transfer_amount,
            min_transfer_amount,
            transfer_count,
            last_block_num,
            updated_at,
            DATE_TRUNC('year', updated_at) AS by_year
        FROM transfer_stats_by_month
        WHERE DATE_TRUNC('year', updated_at) BETWEEN _from AND _to AND nai = _nai
    )
    SELECT
        by_year AS updated_at,
        SUM(sum_transfer_amount)::BIGINT AS sum_transfer_amount,
        NULL::BIGINT,
        MAX(max_transfer_amount)::BIGINT AS max_transfer_amount,
        MIN(min_transfer_amount)::BIGINT AS min_transfer_amount,
        SUM(transfer_count)::INT AS transfer_count,
        MAX(last_block_num)::INT AS last_block_num
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
        NULL::BIGINT,
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
        NULL::BIGINT,
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
        NULL::BIGINT,
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
        NULL::BIGINT,
        ty.max_transfer_amount,
        ty.min_transfer_amount,
        ty.transfer_count,
        ty.last_block_num
      FROM btracker_backend.transfer_stats_by_year(_nai, _from, _to) ty;

  ELSE
    RAISE EXCEPTION 'Unsupported granularity: %', _granularity_hourly;
  END IF;

END
$$;

DO $$
DECLARE 
  __schema_name VARCHAR;
BEGIN
  SHOW SEARCH_PATH INTO __schema_name;
  EXECUTE format(

  $BODY$

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
    $pb$
    DECLARE
        __granularity TEXT;
        __one_period INTERVAL;
        -- Get the current block number from the context
        __btracker_current_block INT := (SELECT current_block_num FROM hafd.contexts WHERE name = '%s');
        __ah_range btracker_backend.aggregated_history_paging_return;
    BEGIN
      __granularity := (
        CASE 
          WHEN _granularity_hourly = 'hourly' THEN 'hour'
          WHEN _granularity_hourly = 'daily' THEN 'day'
          WHEN _granularity_hourly = 'monthly' THEN 'month'
          WHEN _granularity_hourly = 'yearly' THEN 'year'
          ELSE NULL
        END
      );

      __ah_range := btracker_backend.aggregated_history_block_range(_from_block, _to_block, __btracker_current_block, __granularity);

      __one_period := ('1 ' || __granularity )::INTERVAL;

      RETURN QUERY (
        WITH date_series AS (
          SELECT generate_series(
              __ah_range.from_timestamp,
              __ah_range.to_timestamp,
              __one_period
          ) AS date
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
          FROM btracker_backend.get_transfer_stats(_nai, _granularity_hourly, __ah_range.from_timestamp, __ah_range.to_timestamp) bh
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
        ),
        join_missing_block AS (
          SELECT
            fb.date,
            fb.sum_transfer_amount,
            fb.avg_transfer_amount,
            fb.max_transfer_amount,
            fb.min_transfer_amount,
            fb.transfer_count,
            COALESCE(fb.last_block_num, bl.last_block_num) AS last_block_num
          FROM transfer_records fb
          LEFT JOIN hive.blocks_latest bl
            ON bl.day <= fb.date
        )
        SELECT 
          LEAST(jb.date + __one_period, CURRENT_TIMESTAMP)::TIMESTAMP AS adjusted_date,
          jb.sum_transfer_amount::BIGINT,
          (CASE WHEN jb.transfer_count = 0 THEN 0 ELSE (jb.sum_transfer_amount / jb.transfer_count) END)::BIGINT,
          jb.max_transfer_amount::BIGINT,
          jb.min_transfer_amount::BIGINT,
          jb.transfer_count::INT,
          jb.last_block_num::INT
        FROM join_missing_block jb
        ORDER BY
          (CASE WHEN _direction = 'desc' THEN jb.date ELSE NULL END) DESC,
          (CASE WHEN _direction = 'asc' THEN jb.date ELSE NULL END) ASC
      );
    END
    $pb$;

  $BODY$, __schema_name, __schema_name);
END
$$;

RESET ROLE;

-- noqa: disable=AL01, AM05, LT02

SET ROLE btracker_owner;

-- Return type for transfer statistics aggregation functions.
-- Contains summary metrics for a time period (hour/day/month/year).
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

/*
Aggregates monthly transfer statistics into yearly summaries.
Called by: btracker_backend.get_transfer_stats() when _granularity_hourly = 'yearly'

Pattern: Reads from transfer_stats_by_month table and re-aggregates by year.
Note: avg_transfer_amount is NULL here; it's computed in get_transfer_aggregation().
*/
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
    -- get_year: Extract monthly records and truncate timestamps to year boundary
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
    -- Re-aggregate monthly data into yearly summaries
    -- NULL for avg: computed later as sum/count in get_transfer_aggregation()
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

/*
Router function that dispatches to the appropriate transfer stats table based on granularity.
Called by: btracker_backend.get_transfer_aggregation()

Routes to:
  - hourly:  transfer_stats_by_hour table
  - daily:   transfer_stats_by_day table
  - monthly: transfer_stats_by_month table
  - yearly:  btracker_backend.transfer_stats_by_year() (aggregates from monthly)
*/
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
  -- Route to appropriate data source based on requested granularity
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

/*
Main transfer aggregation function with gap-filling for continuous time series.
Called by: btracker_endpoints.get_aggregated_transfer()

This function:
1. Converts block range to timestamp range using aggregated_history_block_range()
2. Generates a continuous date series for the requested granularity
3. LEFT JOINs actual transfer data from get_transfer_stats()
4. Gap-fills missing periods with zeros (no transfers = zero values)
5. Computes average dynamically as sum/count
6. Fills in missing block numbers by looking up the nearest block

Uses dynamic SQL (DO block with format()) to inject the HAF context schema name,
which is required to get the current block number for range calculation.
*/
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
        -- Get the current block number from the HAF context for range validation
        __btracker_current_block INT := (SELECT current_block_num FROM hafd.contexts WHERE name = '%s');
        __ah_range btracker_backend.aggregated_history_paging_return;
    BEGIN
      -- Convert enum granularity to PostgreSQL interval unit string
      __granularity := (
        CASE
          WHEN _granularity_hourly = 'hourly' THEN 'hour'
          WHEN _granularity_hourly = 'daily' THEN 'day'
          WHEN _granularity_hourly = 'monthly' THEN 'month'
          WHEN _granularity_hourly = 'yearly' THEN 'year'
          ELSE NULL
        END
      );

      -- Convert block range to timestamp range, clamped to valid data
      __ah_range := btracker_backend.aggregated_history_block_range(_from_block, _to_block, __btracker_current_block, __granularity);

      -- Build interval for generate_series step size
      __one_period := ('1 ' || __granularity )::INTERVAL;

      RETURN QUERY (
        -- date_series: Generate continuous sequence of time buckets (hour/day/month/year)
        -- This ensures every period in the range is represented, even if no transfers occurred
        WITH date_series AS (
          SELECT generate_series(
              __ah_range.from_timestamp,
              __ah_range.to_timestamp,
              __one_period
          ) AS date
        ),
        -- get_daily_aggregation: MATERIALIZED to ensure single execution
        -- Fetches actual transfer data for the date range from appropriate granularity table
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
        -- transfer_records: LEFT JOIN preserves all date_series entries
        -- COALESCE fills missing periods with zeros (gap-filling pattern)
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
        -- join_missing_block: For periods with no transfers, find the nearest block
        -- Uses LATERAL subquery to look up the last block before end of period
        join_missing_block AS (
          SELECT
            fb.date,
            fb.sum_transfer_amount,
            fb.avg_transfer_amount,
            fb.max_transfer_amount,
            fb.min_transfer_amount,
            fb.transfer_count,
            COALESCE(fb.last_block_num, jl.last_block_num) AS last_block_num
          FROM transfer_records fb
          LEFT JOIN LATERAL (
            SELECT
              b.num AS last_block_num
            FROM hive.blocks_view b
            WHERE b.created_at <= fb.date + __one_period
            ORDER BY b.created_at DESC
            LIMIT 1
          ) jl ON fb.last_block_num IS NULL
        )
        -- Final output: compute average, adjust timestamp to period end, apply sort direction
        -- LEAST prevents future timestamps from exceeding current time
        SELECT
          LEAST(fb.date + __one_period, CURRENT_TIMESTAMP)::TIMESTAMP AS adjusted_date,
          fb.sum_transfer_amount::BIGINT,
          (CASE WHEN fb.transfer_count = 0 THEN 0 ELSE (fb.sum_transfer_amount / fb.transfer_count) END)::BIGINT,
          fb.max_transfer_amount::BIGINT,
          fb.min_transfer_amount::BIGINT,
          fb.transfer_count::INT,
          fb.last_block_num::INT
        FROM join_missing_block fb
        ORDER BY
          (CASE WHEN _direction = 'desc' THEN fb.date ELSE NULL END) DESC,
          (CASE WHEN _direction = 'asc' THEN fb.date ELSE NULL END) ASC
      );
    END
    $pb$;

  $BODY$, __schema_name, __schema_name);
END
$$;

RESET ROLE;

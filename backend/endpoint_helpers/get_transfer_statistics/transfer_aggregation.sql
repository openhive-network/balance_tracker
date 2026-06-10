-- noqa: disable=AL01, AM05, LT02

SET ROLE btracker_owner;

-- Main gap-filled transfer aggregation for the /transfer-statistics endpoint.

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

Reads the current synced block via btracker_backend.last_synced_block().
*/
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
    __granularity TEXT;
    __one_period INTERVAL;
    -- Get the current block number from the HAF context for range validation
    __btracker_current_block INT := btracker_backend.last_synced_block();
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
        -- join_missing_block: for periods with no transfers, attribute the nearest block
        -- at or before the period end via btracker_backend.block_at_or_before()
        join_missing_block AS (
          SELECT
            fb.date,
            fb.sum_transfer_amount,
            fb.avg_transfer_amount,
            fb.max_transfer_amount,
            fb.min_transfer_amount,
            fb.transfer_count,
            COALESCE(fb.last_block_num, btracker_backend.block_at_or_before(fb.date + __one_period)) AS last_block_num
          FROM transfer_records fb
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
$$;

RESET ROLE;

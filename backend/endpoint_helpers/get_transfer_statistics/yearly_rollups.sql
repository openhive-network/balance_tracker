-- Yearly transfer-stats rollup: re-aggregates per-month transfer statistics
-- into yearly summaries for the get_transfer_statistics endpoint.

SET ROLE btracker_owner;

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

RESET ROLE;

SET ROLE btracker_owner;

/*
Per-account vesting stats — gap-filled aggregation that reads from the
per-account pre-aggregated tables (account_vesting_by_day / _by_month)
populated during sync by process_account_vesting_stats.

No on-the-fly scans of hive.account_operations_view or account_vesting_history
at query time — everything served from the pre-aggregated tables, pivoted to the
wide shape by get_account_vesting_stats (yearly rolled up from monthly inside
btracker_backend.vesting_pivot).

Mirrors btracker_backend.get_vesting_aggregation (global) in shape and gap-fill
behaviour.
*/
CREATE OR REPLACE FUNCTION btracker_backend.get_account_vesting_aggregation(
    _account_id  INT,
    _granularity btracker_backend.granularity,
    _direction   btracker_backend.sort_direction,
    _from_block  INT,
    _to_block    INT
)
RETURNS SETOF btracker_backend.vesting_stats_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
  __granularity TEXT;
  __one_period  INTERVAL;
  __btracker_current_block INT := btracker_backend.last_synced_block();
  __ah_range btracker_backend.aggregated_history_paging_return;
BEGIN
  __granularity := (
    CASE
      WHEN _granularity = 'daily'   THEN 'day'
      WHEN _granularity = 'monthly' THEN 'month'
      WHEN _granularity = 'yearly'  THEN 'year'
      ELSE NULL
    END
  );

  __ah_range := btracker_backend.aggregated_history_block_range(
    _from_block, _to_block, __btracker_current_block, __granularity
  );

  __one_period := ('1 ' || __granularity)::INTERVAL;

  RETURN QUERY (
    WITH date_series AS (
      SELECT generate_series(
          __ah_range.from_timestamp,
          __ah_range.to_timestamp,
          __one_period
      ) AS date
    ),
    get_aggregation AS MATERIALIZED (
      SELECT
        avs.updated_at,
        avs.power_up_count,
        avs.power_up_hive,
        avs.power_down_init_count,
        avs.power_down_init_vests,
        avs.power_down_fill_count,
        avs.power_down_fill_vests,
        avs.power_down_fill_hive,
        avs.power_down_route_received_count,
        avs.power_down_route_received_hive,
        avs.power_down_route_received_vests,
        avs.last_block_num
      FROM btracker_backend.get_account_vesting_stats(
        _account_id, _granularity, __ah_range.from_timestamp, __ah_range.to_timestamp
      ) avs
    ),
    joined AS (
      SELECT
        ds.date,
        COALESCE(a.power_up_count,        0)::INT     AS power_up_count,
        COALESCE(a.power_up_hive,         0)::BIGINT  AS power_up_hive,
        COALESCE(a.power_down_init_count, 0)::INT     AS power_down_init_count,
        COALESCE(a.power_down_init_vests, 0)::NUMERIC AS power_down_init_vests,
        COALESCE(a.power_down_fill_count, 0)::INT     AS power_down_fill_count,
        COALESCE(a.power_down_fill_vests, 0)::NUMERIC AS power_down_fill_vests,
        COALESCE(a.power_down_fill_hive,  0)::BIGINT  AS power_down_fill_hive,
        COALESCE(a.power_down_route_received_count, 0)::INT     AS power_down_route_received_count,
        COALESCE(a.power_down_route_received_hive,  0)::BIGINT  AS power_down_route_received_hive,
        COALESCE(a.power_down_route_received_vests, 0)::NUMERIC AS power_down_route_received_vests,
        a.last_block_num
      FROM date_series ds
      LEFT JOIN get_aggregation a ON ds.date = a.updated_at
    ),
    join_missing_block AS (
      SELECT
        j.date,
        j.power_up_count,
        j.power_up_hive,
        j.power_down_init_count,
        j.power_down_init_vests,
        j.power_down_fill_count,
        j.power_down_fill_vests,
        j.power_down_fill_hive,
        j.power_down_route_received_count,
        j.power_down_route_received_hive,
        j.power_down_route_received_vests,
        COALESCE(j.last_block_num, btracker_backend.block_at_or_before(j.date + __one_period)) AS last_block_num
      FROM joined j
    )
    SELECT
      LEAST(j.date + __one_period, CURRENT_TIMESTAMP)::TIMESTAMP AS updated_at,
      j.power_up_count::INT,
      j.power_up_hive::BIGINT,
      j.power_down_init_count::INT,
      j.power_down_init_vests::NUMERIC,
      j.power_down_fill_count::INT,
      j.power_down_fill_vests::NUMERIC,
      j.power_down_fill_hive::BIGINT,
      j.power_down_route_received_count::INT,
      j.power_down_route_received_hive::BIGINT,
      j.power_down_route_received_vests::NUMERIC,
      j.last_block_num::INT
    FROM join_missing_block j
    ORDER BY
      (CASE WHEN _direction = 'desc' THEN j.date ELSE NULL END) DESC,
      (CASE WHEN _direction = 'asc'  THEN j.date ELSE NULL END) ASC
  );
END
$$;

RESET ROLE;

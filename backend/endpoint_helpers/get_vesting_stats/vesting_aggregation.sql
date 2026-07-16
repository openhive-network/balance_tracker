-- noqa: disable=AL01, AM05, LT02

SET ROLE btracker_owner;

-- Main gap-filled global aggregation for the /vesting-stats endpoint.

/*
Gap-filled global aggregation. Mirrors btracker_backend.get_transfer_aggregation:
1. Convert block range to timestamp range
2. Generate continuous date series for the granularity
3. LEFT JOIN actual data, COALESCE missing periods with zeros
4. Look up nearest block for periods with no events
5. Apply sort direction
*/
CREATE OR REPLACE FUNCTION btracker_backend.get_vesting_aggregation(
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
        vs.updated_at,
        vs.power_up_count,
        vs.power_up_hive,
        vs.power_down_init_count,
        vs.power_down_init_vests,
        vs.power_down_fill_count,
        vs.power_down_fill_vests,
        vs.power_down_fill_hive,
        vs.last_block_num
      FROM btracker_backend.get_vesting_stats(
        _granularity, __ah_range.from_timestamp, __ah_range.to_timestamp
      ) vs
    ),
    joined AS (
      SELECT
        ds.date,
        COALESCE(vs.power_up_count,        0)::INT     AS power_up_count,
        COALESCE(vs.power_up_hive,         0)::BIGINT  AS power_up_hive,
        COALESCE(vs.power_down_init_count, 0)::INT     AS power_down_init_count,
        COALESCE(vs.power_down_init_vests, 0)::NUMERIC AS power_down_init_vests,
        COALESCE(vs.power_down_fill_count, 0)::INT     AS power_down_fill_count,
        COALESCE(vs.power_down_fill_vests, 0)::NUMERIC AS power_down_fill_vests,
        COALESCE(vs.power_down_fill_hive,  0)::BIGINT  AS power_down_fill_hive,
        vs.last_block_num
      FROM date_series ds
      LEFT JOIN get_aggregation vs ON ds.date = vs.updated_at
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
        COALESCE(j.last_block_num, btracker_backend.block_at_or_before(j.date + __one_period)) AS last_block_num
      FROM joined j
    )
    SELECT
      -- Label each bucket by its period START (the day/month/year the events
      -- occurred), so same-period events are reported on that period, not the next.
      j.date::TIMESTAMP AS updated_at,
      j.power_up_count::INT,
      j.power_up_hive::BIGINT,
      j.power_down_init_count::INT,
      j.power_down_init_vests::NUMERIC,
      j.power_down_fill_count::INT,
      j.power_down_fill_vests::NUMERIC,
      j.power_down_fill_hive::BIGINT,
      j.last_block_num::INT
    FROM join_missing_block j
    ORDER BY
      (CASE WHEN _direction = 'desc' THEN j.date ELSE NULL END) DESC,
      (CASE WHEN _direction = 'asc'  THEN j.date ELSE NULL END) ASC
  );
END
$$;

RESET ROLE;

-- noqa: disable=AL01, AM05, LT02

SET ROLE btracker_owner;

/*
Internal return type for vesting stats aggregation. Uses NUMERIC for VESTS
columns (sum-of-many-months may exceed BIGINT) and BIGINT for HIVE columns.
The endpoint layer wraps these into amount objects.
*/
DROP TYPE IF EXISTS btracker_backend.vesting_stats_return CASCADE;
CREATE TYPE btracker_backend.vesting_stats_return AS (
    updated_at            TIMESTAMP,
    power_up_count        INT,
    power_up_hive         BIGINT,
    power_down_init_count INT,
    power_down_init_vests NUMERIC,
    power_down_fill_count INT,
    power_down_fill_vests NUMERIC,
    power_down_fill_hive  BIGINT,
    power_down_route_received_count INT,
    power_down_route_received_hive  BIGINT,
    power_down_route_received_vests NUMERIC,
    last_block_num        INT
);

/*
Yearly rollup computed on the fly from vesting_stats_by_month.
Mirrors btracker_backend.transfer_stats_by_year.
*/
CREATE OR REPLACE FUNCTION btracker_backend.vesting_stats_by_year(
    _from TIMESTAMP,
    _to   TIMESTAMP
)
RETURNS SETOF btracker_backend.vesting_stats_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  RETURN QUERY
    SELECT
      DATE_TRUNC('year', vm.updated_at)                                    AS updated_at,
      COALESCE(SUM(vm.op_count)     FILTER (WHERE vm.kind = 1), 0)::INT     AS power_up_count,
      COALESCE(SUM(vm.hive_amount)  FILTER (WHERE vm.kind = 1), 0)::BIGINT  AS power_up_hive,
      COALESCE(SUM(vm.op_count)     FILTER (WHERE vm.kind = 2), 0)::INT     AS power_down_init_count,
      COALESCE(SUM(vm.vests_amount) FILTER (WHERE vm.kind = 2), 0)::NUMERIC AS power_down_init_vests,
      COALESCE(SUM(vm.op_count)     FILTER (WHERE vm.kind = 3), 0)::INT     AS power_down_fill_count,
      COALESCE(SUM(vm.vests_amount) FILTER (WHERE vm.kind = 3), 0)::NUMERIC AS power_down_fill_vests,
      COALESCE(SUM(vm.hive_amount)  FILTER (WHERE vm.kind = 3), 0)::BIGINT  AS power_down_fill_hive,
      COALESCE(SUM(vm.op_count)     FILTER (WHERE vm.kind = 4), 0)::INT     AS power_down_route_received_count,
      COALESCE(SUM(vm.hive_amount)  FILTER (WHERE vm.kind = 4), 0)::BIGINT  AS power_down_route_received_hive,
      COALESCE(SUM(vm.vests_amount) FILTER (WHERE vm.kind = 4), 0)::NUMERIC AS power_down_route_received_vests,
      MAX(vm.last_block_num)::INT                                          AS last_block_num
    FROM vesting_stats_by_month vm
    WHERE DATE_TRUNC('year', vm.updated_at) BETWEEN _from AND _to
    GROUP BY DATE_TRUNC('year', vm.updated_at);
END
$$;

/*
Per-account yearly rollup. Mirrors vesting_stats_by_year but filtered by
account, reading from account_vesting_by_month.
*/
CREATE OR REPLACE FUNCTION btracker_backend.account_vesting_stats_by_year(
    _account_id INT,
    _from       TIMESTAMP,
    _to         TIMESTAMP
)
RETURNS SETOF btracker_backend.vesting_stats_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  RETURN QUERY
    SELECT
      DATE_TRUNC('year', avm.updated_at)                                    AS updated_at,
      COALESCE(SUM(avm.op_count)     FILTER (WHERE avm.kind = 1), 0)::INT     AS power_up_count,
      COALESCE(SUM(avm.hive_amount)  FILTER (WHERE avm.kind = 1), 0)::BIGINT  AS power_up_hive,
      COALESCE(SUM(avm.op_count)     FILTER (WHERE avm.kind = 2), 0)::INT     AS power_down_init_count,
      COALESCE(SUM(avm.vests_amount) FILTER (WHERE avm.kind = 2), 0)::NUMERIC AS power_down_init_vests,
      COALESCE(SUM(avm.op_count)     FILTER (WHERE avm.kind = 3), 0)::INT     AS power_down_fill_count,
      COALESCE(SUM(avm.vests_amount) FILTER (WHERE avm.kind = 3), 0)::NUMERIC AS power_down_fill_vests,
      COALESCE(SUM(avm.hive_amount)  FILTER (WHERE avm.kind = 3), 0)::BIGINT  AS power_down_fill_hive,
      COALESCE(SUM(avm.op_count)     FILTER (WHERE avm.kind = 4), 0)::INT     AS power_down_route_received_count,
      COALESCE(SUM(avm.hive_amount)  FILTER (WHERE avm.kind = 4), 0)::BIGINT  AS power_down_route_received_hive,
      COALESCE(SUM(avm.vests_amount) FILTER (WHERE avm.kind = 4), 0)::NUMERIC AS power_down_route_received_vests,
      MAX(avm.last_block_num)::INT                                          AS last_block_num
    FROM account_vesting_by_month avm
    WHERE avm.account = _account_id
      AND DATE_TRUNC('year', avm.updated_at) BETWEEN _from AND _to
    GROUP BY DATE_TRUNC('year', avm.updated_at);
END
$$;

/*
Per-account granularity router. Reads from per-account pre-aggregated tables
and falls back to the yearly rollup for 'yearly'. Mirrors get_vesting_stats.
*/
CREATE OR REPLACE FUNCTION btracker_backend.get_account_vesting_stats(
    _account_id  INT,
    _granularity btracker_backend.granularity,
    _from        TIMESTAMP,
    _to          TIMESTAMP
)
RETURNS SETOF btracker_backend.vesting_stats_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  IF _granularity = 'daily' THEN
    RETURN QUERY
      SELECT
        avd.updated_at,
        COALESCE(SUM(avd.op_count)     FILTER (WHERE avd.kind = 1), 0)::INT     AS power_up_count,
        COALESCE(SUM(avd.hive_amount)  FILTER (WHERE avd.kind = 1), 0)::BIGINT  AS power_up_hive,
        COALESCE(SUM(avd.op_count)     FILTER (WHERE avd.kind = 2), 0)::INT     AS power_down_init_count,
        COALESCE(SUM(avd.vests_amount) FILTER (WHERE avd.kind = 2), 0)::NUMERIC AS power_down_init_vests,
        COALESCE(SUM(avd.op_count)     FILTER (WHERE avd.kind = 3), 0)::INT     AS power_down_fill_count,
        COALESCE(SUM(avd.vests_amount) FILTER (WHERE avd.kind = 3), 0)::NUMERIC AS power_down_fill_vests,
        COALESCE(SUM(avd.hive_amount)  FILTER (WHERE avd.kind = 3), 0)::BIGINT  AS power_down_fill_hive,
        COALESCE(SUM(avd.op_count)     FILTER (WHERE avd.kind = 4), 0)::INT     AS power_down_route_received_count,
        COALESCE(SUM(avd.hive_amount)  FILTER (WHERE avd.kind = 4), 0)::BIGINT  AS power_down_route_received_hive,
        COALESCE(SUM(avd.vests_amount) FILTER (WHERE avd.kind = 4), 0)::NUMERIC AS power_down_route_received_vests,
        MAX(avd.last_block_num)::INT                                           AS last_block_num
      FROM account_vesting_by_day avd
      WHERE avd.account = _account_id
        AND avd.updated_at BETWEEN _from AND _to
      GROUP BY avd.updated_at;

  ELSIF _granularity = 'monthly' THEN
    RETURN QUERY
      SELECT
        avm.updated_at,
        COALESCE(SUM(avm.op_count)     FILTER (WHERE avm.kind = 1), 0)::INT     AS power_up_count,
        COALESCE(SUM(avm.hive_amount)  FILTER (WHERE avm.kind = 1), 0)::BIGINT  AS power_up_hive,
        COALESCE(SUM(avm.op_count)     FILTER (WHERE avm.kind = 2), 0)::INT     AS power_down_init_count,
        COALESCE(SUM(avm.vests_amount) FILTER (WHERE avm.kind = 2), 0)::NUMERIC AS power_down_init_vests,
        COALESCE(SUM(avm.op_count)     FILTER (WHERE avm.kind = 3), 0)::INT     AS power_down_fill_count,
        COALESCE(SUM(avm.vests_amount) FILTER (WHERE avm.kind = 3), 0)::NUMERIC AS power_down_fill_vests,
        COALESCE(SUM(avm.hive_amount)  FILTER (WHERE avm.kind = 3), 0)::BIGINT  AS power_down_fill_hive,
        COALESCE(SUM(avm.op_count)     FILTER (WHERE avm.kind = 4), 0)::INT     AS power_down_route_received_count,
        COALESCE(SUM(avm.hive_amount)  FILTER (WHERE avm.kind = 4), 0)::BIGINT  AS power_down_route_received_hive,
        COALESCE(SUM(avm.vests_amount) FILTER (WHERE avm.kind = 4), 0)::NUMERIC AS power_down_route_received_vests,
        MAX(avm.last_block_num)::INT                                           AS last_block_num
      FROM account_vesting_by_month avm
      WHERE avm.account = _account_id
        AND avm.updated_at BETWEEN _from AND _to
      GROUP BY avm.updated_at;

  ELSIF _granularity = 'yearly' THEN
    RETURN QUERY
      SELECT * FROM btracker_backend.account_vesting_stats_by_year(_account_id, _from, _to);

  ELSE
    RAISE EXCEPTION 'Unsupported granularity: %', _granularity;
  END IF;
END
$$;

/*
Granularity router: returns rows from the appropriate pre-aggregated table.
Mirrors btracker_backend.get_transfer_stats.
*/
CREATE OR REPLACE FUNCTION btracker_backend.get_vesting_stats(
    _granularity btracker_backend.granularity,
    _from        TIMESTAMP,
    _to          TIMESTAMP
)
RETURNS SETOF btracker_backend.vesting_stats_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  IF _granularity = 'daily' THEN
    RETURN QUERY
      SELECT
        vd.updated_at,
        COALESCE(SUM(vd.op_count)     FILTER (WHERE vd.kind = 1), 0)::INT     AS power_up_count,
        COALESCE(SUM(vd.hive_amount)  FILTER (WHERE vd.kind = 1), 0)::BIGINT  AS power_up_hive,
        COALESCE(SUM(vd.op_count)     FILTER (WHERE vd.kind = 2), 0)::INT     AS power_down_init_count,
        COALESCE(SUM(vd.vests_amount) FILTER (WHERE vd.kind = 2), 0)::NUMERIC AS power_down_init_vests,
        COALESCE(SUM(vd.op_count)     FILTER (WHERE vd.kind = 3), 0)::INT     AS power_down_fill_count,
        COALESCE(SUM(vd.vests_amount) FILTER (WHERE vd.kind = 3), 0)::NUMERIC AS power_down_fill_vests,
        COALESCE(SUM(vd.hive_amount)  FILTER (WHERE vd.kind = 3), 0)::BIGINT  AS power_down_fill_hive,
        COALESCE(SUM(vd.op_count)     FILTER (WHERE vd.kind = 4), 0)::INT     AS power_down_route_received_count,
        COALESCE(SUM(vd.hive_amount)  FILTER (WHERE vd.kind = 4), 0)::BIGINT  AS power_down_route_received_hive,
        COALESCE(SUM(vd.vests_amount) FILTER (WHERE vd.kind = 4), 0)::NUMERIC AS power_down_route_received_vests,
        MAX(vd.last_block_num)::INT                                          AS last_block_num
      FROM vesting_stats_by_day vd
      WHERE vd.updated_at BETWEEN _from AND _to
      GROUP BY vd.updated_at;

  ELSIF _granularity = 'monthly' THEN
    RETURN QUERY
      SELECT
        vm.updated_at,
        COALESCE(SUM(vm.op_count)     FILTER (WHERE vm.kind = 1), 0)::INT     AS power_up_count,
        COALESCE(SUM(vm.hive_amount)  FILTER (WHERE vm.kind = 1), 0)::BIGINT  AS power_up_hive,
        COALESCE(SUM(vm.op_count)     FILTER (WHERE vm.kind = 2), 0)::INT     AS power_down_init_count,
        COALESCE(SUM(vm.vests_amount) FILTER (WHERE vm.kind = 2), 0)::NUMERIC AS power_down_init_vests,
        COALESCE(SUM(vm.op_count)     FILTER (WHERE vm.kind = 3), 0)::INT     AS power_down_fill_count,
        COALESCE(SUM(vm.vests_amount) FILTER (WHERE vm.kind = 3), 0)::NUMERIC AS power_down_fill_vests,
        COALESCE(SUM(vm.hive_amount)  FILTER (WHERE vm.kind = 3), 0)::BIGINT  AS power_down_fill_hive,
        COALESCE(SUM(vm.op_count)     FILTER (WHERE vm.kind = 4), 0)::INT     AS power_down_route_received_count,
        COALESCE(SUM(vm.hive_amount)  FILTER (WHERE vm.kind = 4), 0)::BIGINT  AS power_down_route_received_hive,
        COALESCE(SUM(vm.vests_amount) FILTER (WHERE vm.kind = 4), 0)::NUMERIC AS power_down_route_received_vests,
        MAX(vm.last_block_num)::INT                                          AS last_block_num
      FROM vesting_stats_by_month vm
      WHERE vm.updated_at BETWEEN _from AND _to
      GROUP BY vm.updated_at;

  ELSIF _granularity = 'yearly' THEN
    RETURN QUERY
      SELECT * FROM btracker_backend.vesting_stats_by_year(_from, _to);

  ELSE
    RAISE EXCEPTION 'Unsupported granularity: %', _granularity;
  END IF;
END
$$;

/*
Gap-filled global aggregation. Mirrors btracker_backend.get_transfer_aggregation:
1. Convert block range to timestamp range
2. Generate continuous date series for the granularity
3. LEFT JOIN actual data, COALESCE missing periods with zeros
4. Look up nearest block for periods with no events
5. Apply sort direction
*/
DO $$
DECLARE
  __schema_name VARCHAR;
BEGIN
  SHOW SEARCH_PATH INTO __schema_name;
  EXECUTE format(
  $BODY$

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
    $pb$
    DECLARE
        __granularity TEXT;
        __one_period  INTERVAL;
        __btracker_current_block INT := (SELECT current_block_num FROM hafd.contexts WHERE name = '%s');
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
            vs.power_down_route_received_count,
            vs.power_down_route_received_hive,
            vs.power_down_route_received_vests,
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
            COALESCE(vs.power_down_route_received_count, 0)::INT     AS power_down_route_received_count,
            COALESCE(vs.power_down_route_received_hive,  0)::BIGINT  AS power_down_route_received_hive,
            COALESCE(vs.power_down_route_received_vests, 0)::NUMERIC AS power_down_route_received_vests,
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
            j.power_down_route_received_count,
            j.power_down_route_received_hive,
            j.power_down_route_received_vests,
            COALESCE(j.last_block_num, jl.last_block_num) AS last_block_num
          FROM joined j
          LEFT JOIN LATERAL (
            SELECT b.num AS last_block_num
            FROM hive.blocks_view b
            WHERE b.created_at <= j.date + __one_period
            ORDER BY b.created_at DESC
            LIMIT 1
          ) jl ON j.last_block_num IS NULL
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
    $pb$;

  $BODY$, __schema_name);
END
$$;

RESET ROLE;

SET ROLE btracker_owner;

/*
 * Vesting tall->wide pivot engine.
 * --------------------------------------------------------------------------
 * The vesting stats tables are stored TALL (one row per kind); the API returns the
 * wide vesting_stats shape. This single function reconstructs it, replacing the SIX
 * hand-copied SUM-FILTER pivots (global / per-account x daily / monthly / yearly)
 * and the two *_by_year rollup functions.
 *
 *   _table       whitelisted source table, chosen by the caller via a fixed CASE
 *                (never raw request input)
 *   _account_id  NULL  => global table (no `account` column); else per-account filter
 *   _trunc       NULL  => bucket by updated_at as stored (daily / monthly tables);
 *                'year' => roll the monthly table up to whole years
 *
 * The 11-column pivot list is written ONCE here. EXECUTE format() is used because the
 * only thing that varies is the table identifier + two optional predicates — the same
 * dynamic-SQL style the gap-fill aggregation already uses. The kind discriminators come
 * from the btracker_backend.vesting_kind_*() constants (no bare 1/2/3/4 literals).
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

CREATE OR REPLACE FUNCTION btracker_backend.vesting_pivot(
    _table      TEXT,
    _account_id INT,
    _from       TIMESTAMP,
    _to         TIMESTAMP,
    _trunc      TEXT DEFAULT NULL
)
RETURNS SETOF btracker_backend.vesting_stats_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
  _period TEXT := CASE WHEN _trunc IS NULL THEN 'v.updated_at'
                       ELSE format('DATE_TRUNC(%L, v.updated_at)', _trunc) END;
  _acct   TEXT := CASE WHEN _account_id IS NULL THEN ''
                       ELSE format('v.account = %s AND ', _account_id) END;
BEGIN
  RETURN QUERY EXECUTE format($q$
    SELECT
      %1$s AS updated_at,
      COALESCE(SUM(v.op_count)     FILTER (WHERE v.kind = %2$s), 0)::INT     AS power_up_count,
      COALESCE(SUM(v.hive_amount)  FILTER (WHERE v.kind = %2$s), 0)::BIGINT  AS power_up_hive,
      COALESCE(SUM(v.op_count)     FILTER (WHERE v.kind = %3$s), 0)::INT     AS power_down_init_count,
      COALESCE(SUM(v.vests_amount) FILTER (WHERE v.kind = %3$s), 0)::NUMERIC AS power_down_init_vests,
      COALESCE(SUM(v.op_count)     FILTER (WHERE v.kind = %4$s), 0)::INT     AS power_down_fill_count,
      COALESCE(SUM(v.vests_amount) FILTER (WHERE v.kind = %4$s), 0)::NUMERIC AS power_down_fill_vests,
      COALESCE(SUM(v.hive_amount)  FILTER (WHERE v.kind = %4$s), 0)::BIGINT  AS power_down_fill_hive,
      COALESCE(SUM(v.op_count)     FILTER (WHERE v.kind = %5$s), 0)::INT     AS power_down_route_received_count,
      COALESCE(SUM(v.hive_amount)  FILTER (WHERE v.kind = %5$s), 0)::BIGINT  AS power_down_route_received_hive,
      COALESCE(SUM(v.vests_amount) FILTER (WHERE v.kind = %5$s), 0)::NUMERIC AS power_down_route_received_vests,
      MAX(v.last_block_num)::INT AS last_block_num
    FROM %6$I v
    WHERE %7$s %1$s BETWEEN $1 AND $2
    GROUP BY %1$s
  $q$,
    _period,                                                   -- %1$s  period expr (also WHERE + GROUP BY)
    btracker_backend.vesting_kind_power_up(),                  -- %2$s
    btracker_backend.vesting_kind_power_down_init(),           -- %3$s
    btracker_backend.vesting_kind_power_down_fill(),           -- %4$s
    btracker_backend.vesting_kind_power_down_route_received(), -- %5$s
    _table,                                                    -- %6$I  whitelisted table identifier
    _acct                                                      -- %7$s  optional account predicate
  ) USING _from, _to;
END
$$;

RESET ROLE;

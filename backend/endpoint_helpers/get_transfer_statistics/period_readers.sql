-- noqa: disable=AL01, AM05, LT02

-- Per-granularity transfer-stats reader: return type and the router that
-- dispatches to the hourly/daily/monthly/yearly transfer-stats source.

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

RESET ROLE;

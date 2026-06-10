-- noqa: disable=AL01, AM05, LT02

SET ROLE btracker_owner;

/*
 * Dedicated read helpers for the global /vesting-stats endpoint. Both consume the
 * shared tall->wide engine btracker_backend.vesting_pivot()
 * (endpoint_helpers/shared_functions/vesting_pivot.sql). The per-account endpoint has
 * its own mirror pair in endpoint_helpers/get_account_vesting_stats/ (period_readers.sql + vesting_aggregation.sql).
 */

/*
Global granularity router: map granularity -> tall table (+ yearly rollup),
then pivot to the wide vesting_stats shape (no account filter).
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
  RETURN QUERY SELECT * FROM btracker_backend.vesting_pivot(
    CASE WHEN _granularity = 'daily' THEN 'vesting_stats_by_day' ELSE 'vesting_stats_by_month' END,
    NULL, _from, _to,
    CASE WHEN _granularity = 'yearly' THEN 'year' END
  );
END
$$;

RESET ROLE;

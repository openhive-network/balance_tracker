SET ROLE btracker_owner;

/*
Computes blockchain-wide Total Value Locked (TVL) metrics.
Called by: btracker_endpoints.get_total_value_locked()

Returns sum of all VESTS (staked HIVE) and all savings balances (HIVE + HBD).
Full-table aggregations - consider materialized view for high-traffic deployments.
*/
CREATE OR REPLACE FUNCTION btracker_backend.get_total_value_locked()
RETURNS btracker_backend.total_value_locked
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  -- Use NAI lookup functions instead of hardcoded values
  _nai_vests CONSTANT SMALLINT := btracker_backend.nai_vests();
  _nai_hbd   CONSTANT SMALLINT := btracker_backend.nai_hbd();
  _nai_hive  CONSTANT SMALLINT := btracker_backend.nai_hive();

  _synced_block      INT;
  _sum_vests_txt     TEXT;
  _sum_sav_hive_txt  TEXT;
  _sum_sav_hbd_txt   TEXT;
BEGIN
  -- Get current synced block for snapshot reference
  SELECT btracker_endpoints.get_btracker_last_synced_block()
  INTO _synced_block;

  -- Sum all VESTS from current balances
  -- NUMERIC type prevents overflow for very large sums
  SELECT COALESCE(SUM(cab.balance::NUMERIC), 0)::TEXT
  INTO _sum_vests_txt
  FROM current_account_balances cab
  WHERE cab.nai = _nai_vests;

  -- Sum savings balances for both asset types in single pass
  -- Conditional aggregation avoids two separate queries
  SELECT
    COALESCE(SUM(CASE WHEN asv.nai = _nai_hive THEN asv.balance::NUMERIC ELSE 0 END), 0)::TEXT,
    COALESCE(SUM(CASE WHEN asv.nai = _nai_hbd THEN asv.balance::NUMERIC ELSE 0 END), 0)::TEXT
  INTO
    _sum_sav_hive_txt,
    _sum_sav_hbd_txt
  FROM account_savings asv;

  -- Return as TEXT to prevent JSON bigint overflow
  RETURN (
    _synced_block,
    _sum_vests_txt,
    _sum_sav_hive_txt,
    _sum_sav_hbd_txt
  )::btracker_backend.total_value_locked;
END
$$;

RESET ROLE;

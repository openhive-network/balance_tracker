SET ROLE btracker_owner;

-- NAI (Numeric Asset Identifier) lookup functions
-- These provide semantic names for asset types and avoid magic numbers in the code
--
-- NAIs 13, 21, 37 are looked up from asset_table (single source of truth)
-- NAI 38 is VIRTUAL - it represents VESTS converted to HIVE equivalent value
-- and is NOT stored in asset_table (must be hardcoded)

-- HBD (Hive Backed Dollar) - looked up from asset_table
CREATE OR REPLACE FUNCTION btracker_backend.nai_hbd()
RETURNS SMALLINT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT asset_symbol_nai FROM asset_table WHERE asset_name = 'HBD');
END;
$$;

-- HIVE - looked up from asset_table
CREATE OR REPLACE FUNCTION btracker_backend.nai_hive()
RETURNS SMALLINT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT asset_symbol_nai FROM asset_table WHERE asset_name = 'HIVE');
END;
$$;

-- VESTS (Vesting Shares) - looked up from asset_table
CREATE OR REPLACE FUNCTION btracker_backend.nai_vests()
RETURNS SMALLINT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT asset_symbol_nai FROM asset_table WHERE asset_name = 'VESTS');
END;
$$;

-- Virtual NAI: VESTS converted to HIVE equivalent
-- This does NOT exist in asset_table - it's a calculated display value
-- Formula: vests * (total_vesting_fund_hive / total_vesting_shares)
-- Must be hardcoded since there's no asset_table entry for this virtual type
CREATE OR REPLACE FUNCTION btracker_backend.nai_vests_as_hive()
RETURNS SMALLINT LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  RETURN 38::SMALLINT;
END;
$$;

RESET ROLE;

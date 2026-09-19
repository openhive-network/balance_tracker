SET ROLE btracker_owner;

-- NAI (Numeric Asset Identifier) functions
-- These provide semantic names for asset types and avoid magic numbers in the code.
--
-- They are CONSTANTS: LANGUAGE sql IMMUTABLE with a literal body, so the planner
-- inlines and folds each call at plan time (`nai = btracker_backend.nai_vests()`
-- plans as `nai = '37'::smallint`).
--
-- They used to be `plpgsql STABLE` lookups into asset_table. A STABLE function is
-- not folded: it stays in the plan as a call and is re-evaluated at every index
-- (re)scan, and these sit in join conditions on current_account_balances. Measured
-- on a mainnet node in haf_block_explorer's per-block account_vest_stats(): ~42,000
-- plpgsql calls (each a table lookup) per block, 92 ms -> 64 ms once folded, with no
-- change to the query. A non-foldable, PARALLEL UNSAFE call also hides the value
-- from the planner's selectivity estimates and rules out parallel plans.
--
-- NAIs 13, 21, 37 are Hive protocol constants (@@000000013/21/37) and are also
-- what db/btracker_app.sql seeds asset_table with; the DO block below fails the
-- install if the two ever disagree, so asset_table stays the checked reference.
-- NAI 38 is VIRTUAL - it represents VESTS converted to HIVE equivalent value
-- and is NOT stored in asset_table.

-- HBD (Hive Backed Dollar)
CREATE OR REPLACE FUNCTION btracker_backend.nai_hbd()
RETURNS SMALLINT LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT 13::SMALLINT
$$;

-- HIVE
CREATE OR REPLACE FUNCTION btracker_backend.nai_hive()
RETURNS SMALLINT LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT 21::SMALLINT
$$;

-- VESTS (Vesting Shares)
CREATE OR REPLACE FUNCTION btracker_backend.nai_vests()
RETURNS SMALLINT LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT 37::SMALLINT
$$;

-- Virtual NAI: VESTS converted to HIVE equivalent
-- This does NOT exist in asset_table - it's a calculated display value
-- Formula: vests * (total_vesting_fund_hive / total_vesting_shares)
CREATE OR REPLACE FUNCTION btracker_backend.nai_vests_as_hive()
RETURNS SMALLINT LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT 38::SMALLINT
$$;

-- Decimal precision of a HIVE/HBD amount object (used to tell HIVE from VESTS by precision)
CREATE OR REPLACE FUNCTION btracker_backend.asset_precision_hive()
RETURNS INT LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT 3
$$;

-- Decimal precision of a VESTS amount object (used to tell VESTS from HIVE by precision)
CREATE OR REPLACE FUNCTION btracker_backend.asset_precision_vests()
RETURNS INT LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT 6
$$;

-- Guard: the constants above must match asset_table (seeded by db/btracker_app.sql,
-- which install_app.sh always loads before this file). Fail the install loudly
-- rather than let the two drift apart silently.
DO $$
DECLARE
  __bad TEXT;
BEGIN
  SELECT string_agg(format('%s: function=%s asset_table=%s', e.asset_name, e.nai, a.asset_symbol_nai), '; ')
  INTO __bad
  FROM (VALUES ('HBD',   btracker_backend.nai_hbd(),   btracker_backend.asset_precision_hive()),
               ('HIVE',  btracker_backend.nai_hive(),  btracker_backend.asset_precision_hive()),
               ('VESTS', btracker_backend.nai_vests(), btracker_backend.asset_precision_vests())
       ) AS e(asset_name, nai, prec)
  LEFT JOIN asset_table a ON a.asset_name = e.asset_name
  WHERE a.asset_symbol_nai IS DISTINCT FROM e.nai OR a.asset_precision IS DISTINCT FROM e.prec;

  IF __bad IS NOT NULL THEN
    RAISE EXCEPTION 'btracker_backend NAI constants disagree with asset_table: %', __bad;
  END IF;
END
$$;

RESET ROLE;

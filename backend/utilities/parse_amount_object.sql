/**
 * Asset Amount Parser for Balance Tracker
 * =======================================
 *
 * This file provides utilities for parsing JSONB amount objects from
 * blockchain operations into structured asset data.
 *
 * Blockchain Amount Format:
 * -------------------------
 * Hive blockchain operations store amounts as JSONB objects:
 * {
 *   "amount": "1000000",      -- Amount in satoshis (smallest unit)
 *   "precision": 3,           -- Decimal places (3 for HIVE/HBD, 6 for VESTS)
 *   "nai": "@@000000021"      -- Numeric Asset Identifier
 * }
 *
 * NAI Values:
 * -----------
 * - @@000000013 = HBD (Hive Backed Dollars)
 * - @@000000021 = HIVE
 * - @@000000037 = VESTS (Vesting Shares)
 *
 * Usage:
 * ------
 * SELECT * FROM btracker_backend.parse_amount_object(
 *   '{"amount":"1000000","precision":3,"nai":"@@000000021"}'::jsonb
 * );
 * -- Returns: (1000000, 3, 21) meaning 1000.000 HIVE
 *
 * Dependencies:
 * -------------
 * - hive.asset_symbol_from_nai_string(): Converts NAI string to symbol
 * - hive.decode_asset_symbol(): Extracts symbol info
 *
 * @see backend/utilities/nai_types.sql for NAI constants
 */

SET ROLE btracker_owner;

/**
 * btracker_backend.asset
 * ----------------------
 * Composite type representing a parsed blockchain asset amount.
 *
 * Fields:
 * - amount: Raw amount in satoshis (smallest unit)
 * - asset_precision: Decimal places (3 for HIVE/HBD, 6 for VESTS)
 * - asset_symbol_nai: Numeric asset ID (13=HBD, 21=HIVE, 37=VESTS)
 *
 * To convert to human-readable: amount / 10^precision
 * Example: 1000000 with precision 3 = 1000.000
 */
DROP TYPE IF EXISTS btracker_backend.asset CASCADE;
CREATE TYPE btracker_backend.asset AS
(
    amount           BIGINT, -- Amount of asset in satoshis (smallest unit)
    asset_precision  INT,    -- Decimal places (3 for HIVE/HBD, 6 for VESTS)
    asset_symbol_nai INT     -- NAI code (13=HBD, 21=HIVE, 37=VESTS)
);

/**
 * parse_amount_object()
 * ---------------------
 * Parse a JSONB amount object from a blockchain operation.
 *
 * Converts the blockchain's JSONB amount format into a structured
 * btracker_backend.asset type for use in processing functions.
 *
 * @param __amount_object  JSONB with {amount, precision, nai} fields
 * @returns                btracker_backend.asset with parsed values
 *
 * Example:
 *   parse_amount_object('{"amount":"1000","precision":3,"nai":"@@000000021"}')
 *   Returns: (1000, 3, 21)
 */
CREATE OR REPLACE FUNCTION btracker_backend.parse_amount_object(__amount_object JSONB)
RETURNS btracker_backend.asset IMMUTABLE
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  __nai        hafd.asset_symbol;
  __asset_info hive.asset_symbol_info;

  __result     btracker_backend.asset;
BEGIN
  -- Convert NAI string to HAF asset symbol type
  __nai := hive.asset_symbol_from_nai_string(
    (__amount_object ->> 'nai'),
    (__amount_object ->> 'precision')::SMALLINT
  );

  -- Decode the symbol to get precision and NAI code
  __asset_info := hive.decode_asset_symbol(__nai);

  -- Build result tuple
  __result := (
    __amount_object->>'amount',
    __asset_info.precision,
    __asset_info.nai
  );

  RETURN __result;
END
$$;

RESET ROLE;

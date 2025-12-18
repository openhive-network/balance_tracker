/**
 * Coin Type Utilities for Balance Tracker
 * ========================================
 *
 * This file contains functions for working with Hive blockchain asset types
 * (coin types). It handles conversion between human-readable names and
 * internal NAI (Numeric Asset Identifier) codes.
 *
 * Hive Asset Types:
 * -----------------
 * The Hive blockchain has three native asset types:
 *
 * | Name  | NAI  | Precision | Description                           |
 * |-------|------|-----------|---------------------------------------|
 * | HIVE  | 21   | 3         | Native currency (formerly STEEM)      |
 * | HBD   | 13   | 3         | Hive Backed Dollars (stablecoin)      |
 * | VESTS | 37   | 6         | Vesting shares (staked HIVE)          |
 *
 * NAI Format:
 * -----------
 * NAI is a numeric identifier used internally. The asset_table stores
 * mappings between NAI codes and human-readable names/formats.
 *
 * Amount Object Format:
 * ---------------------
 * The API returns amounts as JSON objects with:
 * - nai: String like "@@000000021" (internal format)
 * - amount: String representation of the value
 * - precision: Number of decimal places (3 for HIVE/HBD, 6 for VESTS)
 *
 * Dependencies:
 * -------------
 * - asset_table: Reference table with asset definitions
 * - btracker_backend.nai_type: ENUM ('HBD', 'HIVE', 'VESTS')
 * - btracker_backend.amount: Composite type for JSON-serializable amounts
 */

SET ROLE btracker_owner;

/**
 * get_nai_type()
 * --------------
 * Convert a human-readable coin type name to its NAI code.
 *
 * This function translates API input (text enum) to the internal
 * numeric identifier used in database tables.
 *
 * @param _nai  Coin type enum ('HBD', 'HIVE', or 'VESTS')
 * @returns     NAI code (13 for HBD, 21 for HIVE, 37 for VESTS)
 *
 * Example:
 *   get_nai_type('HIVE') = 21
 *   get_nai_type('HBD')  = 13
 *   get_nai_type('VESTS') = 37
 *
 * Usage:
 *   Used to convert API parameters to internal format for queries:
 *   WHERE nai = btracker_backend.get_nai_type(_coin_type)
 */
CREATE OR REPLACE FUNCTION btracker_backend.get_nai_type(_nai btracker_backend.nai_type)
RETURNS INT STABLE
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RETURN a.asset_symbol_nai FROM asset_table a WHERE a.asset_name = _nai::TEXT;
END
$$;

/**
 * create_amount_object()
 * ----------------------
 * Create a JSON-serializable amount object from NAI and raw amount.
 *
 * This function converts internal balance values (stored as BIGINT)
 * into the structured format expected by API consumers. The amount
 * type is defined as a composite type that PostgREST serializes to JSON.
 *
 * @param __nai    NAI code (13, 21, or 37)
 * @param __amount Raw amount value (in smallest unit)
 * @returns        btracker_backend.amount composite type
 *
 * Amount Precision:
 * -----------------
 * Amounts are stored as integers in the smallest unit:
 * - HIVE/HBD: 1000 = 1.000 (precision 3)
 * - VESTS: 1000000 = 1.000000 (precision 6)
 *
 * Example Output (as JSON):
 *   create_amount_object(21, 1500000) produces:
 *   {
 *     "nai": "@@000000021",
 *     "amount": "1500000",
 *     "precision": 3
 *   }
 *   This represents 1500.000 HIVE.
 *
 * Usage:
 *   SELECT btracker_backend.create_amount_object(nai, balance)
 *   FROM current_account_balances WHERE account = 123;
 */
CREATE OR REPLACE FUNCTION btracker_backend.create_amount_object(IN __nai INT, IN __amount BIGINT)
RETURNS btracker_backend.amount
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (a.nai_string, __amount::TEXT, a.asset_precision)::btracker_backend.amount
  FROM asset_table a
  WHERE a.asset_symbol_nai = __nai;
END
$$;

RESET ROLE;

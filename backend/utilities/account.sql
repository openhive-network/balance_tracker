/**
 * Account Utilities for Balance Tracker
 * ======================================
 *
 * This file contains functions for looking up account information.
 * It provides the bridge between human-readable account names (used in APIs)
 * and internal account IDs (used in database tables).
 *
 * Account Storage:
 * ----------------
 * Hive blockchain accounts are stored in HAF's hive.accounts_view with:
 * - id: Internal numeric ID (used in Balance Tracker tables)
 * - name: Account name (lowercase, 3-16 chars, used in APIs)
 *
 * Balance Tracker tables store account references as numeric IDs for
 * storage efficiency and faster joins. These functions handle the
 * translation between names and IDs.
 *
 * Validation Integration:
 * -----------------------
 * get_account_id() integrates with validate_account() to provide
 * automatic existence checking. This ensures that invalid account
 * names are caught early with clear error messages.
 *
 * Dependencies:
 * -------------
 * - hive.accounts_view: HAF view containing all blockchain accounts
 * - btracker_backend.validate_account(): Validation function
 *
 * @see validators.sql for validate_account() documentation
 */

SET ROLE btracker_owner;

/**
 * get_account_id()
 * ----------------
 * Look up an account's internal ID from its name, with optional validation.
 *
 * This is the primary function for converting API account parameters
 * to internal IDs. It combines lookup and validation in one call.
 *
 * @param _account_name  Account name to look up (case-sensitive)
 * @param _required      Whether the account must exist
 * @returns              Account ID if found, NULL if not found
 *
 * Behavior:
 * ---------
 * 1. Queries hive.accounts_view for the account ID
 * 2. Calls validate_account() to check existence requirements
 * 3. Returns the ID (or NULL if account doesn't exist and not required)
 *
 * Validation Cases:
 * -----------------
 * - _required=true: Raises exception if account doesn't exist
 * - _required=false, name provided: Raises exception if account doesn't exist
 * - _required=false, name=NULL: Returns NULL (no validation needed)
 *
 * Example Usage:
 *   -- Required account (will error if not found)
 *   SELECT btracker_backend.get_account_id('alice', true);
 *
 *   -- Optional account (will error only if name provided but not found)
 *   SELECT btracker_backend.get_account_id(NULL, false);  -- Returns NULL
 *   SELECT btracker_backend.get_account_id('bob', false); -- Errors if 'bob' doesn't exist
 *
 * @see validate_account() for validation logic details
 */
CREATE OR REPLACE FUNCTION btracker_backend.get_account_id(_account_name TEXT, _required BOOLEAN)
RETURNS INT STABLE
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  _account_id INT := (SELECT av.id FROM hive.accounts_view av WHERE av.name = _account_name);
BEGIN
  PERFORM btracker_backend.validate_account(_account_id, _account_name, _required);

  RETURN _account_id;
END
$$;

/**
 * get_account_name()
 * ------------------
 * Look up an account's name from its internal ID.
 *
 * This is the reverse of get_account_id(). It's used when displaying
 * results to users - internal queries use IDs, but API responses
 * should show human-readable account names.
 *
 * @param _account_id  Internal account ID to look up
 * @returns            Account name if found, NULL if not found
 *
 * Note: This function does not validate existence. If an invalid ID
 * is passed, it simply returns NULL. Callers should ensure IDs come
 * from trusted sources (e.g., database foreign keys).
 *
 * Example Usage:
 *   SELECT btracker_backend.get_account_name(123);  -- Returns 'alice'
 *   SELECT btracker_backend.get_account_name(999999);  -- Returns NULL if not found
 */
CREATE OR REPLACE FUNCTION btracker_backend.get_account_name(_account_id INT)
RETURNS TEXT STABLE
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RETURN av.name FROM hive.accounts_view av WHERE av.id = _account_id;
END
$$;

RESET ROLE;

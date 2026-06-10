/**
 * Exception Functions for Balance Tracker API
 * ============================================
 *
 * This file contains functions that validate input parameters and raise
 * descriptive exceptions when validation fails. These functions are designed
 * to provide clear, user-friendly error messages for API consumers.
 *
 * Design Principles:
 * ------------------
 * 1. Fail Fast: Check constraints early to avoid wasted computation
 * 2. Clear Messages: Include the invalid value in error messages
 * 3. Consistent Format: Error messages follow a predictable pattern
 * 4. HTTP-Friendly: PostgREST converts these to HTTP 400 responses
 *
 * Error Message Format:
 * ---------------------
 * Messages include both the constraint and the violating value:
 *   "page-size <= 1000: page-size of 5000 is greater than maximum allowed"
 *   "page <= 5: page of 10 is greater than maximum page"
 *
 * Usage Pattern:
 * --------------
 * These functions are called via PERFORM when validation might fail:
 *   PERFORM btracker_backend.validate_limit(_page_size, 1000);
 *   -- If _page_size > 1000, raises exception and stops execution
 *
 * Dependencies:
 * -------------
 * - btracker_backend.balance_type: ENUM ('balance', 'savings_balance')
 * - btracker_backend.nai_type: ENUM ('HBD', 'HIVE', 'VESTS')
 */

SET ROLE btracker_owner;

/**
 * validate_limit()
 * ----------------
 * Validate that a limit/page-size value does not exceed the maximum allowed.
 *
 * This prevents API abuse by capping how many results can be returned
 * in a single request. The actual maximum is defined by the endpoint.
 *
 * @param given_limit      User-provided limit value
 * @param expected_limit   Maximum allowed limit
 * @param given_limit_name Parameter name for error message (default: 'page-size')
 * @returns                VOID (raises exception if validation fails)
 *
 * Example Error:
 *   validate_limit(5000, 1000) raises:
 *   "page-size <= 1000: page-size of 5000 is greater than maximum allowed"
 */
CREATE OR REPLACE FUNCTION btracker_backend.validate_limit(given_limit BIGINT, expected_limit INT,given_limit_name TEXT DEFAULT 'page-size')
RETURNS VOID -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
IMMUTABLE
AS
$$
BEGIN
  IF given_limit > expected_limit THEN
    RAISE EXCEPTION '% <= %: % of % is greater than maxmimum allowed', given_limit_name, expected_limit, given_limit_name, given_limit;
  END IF;

  RETURN;
END
$$;

/**
 * validate_page()
 * ---------------
 * Validate that a page number does not exceed the total number of pages.
 *
 * Special case: Page 1 is always valid, even if max_page is 0 (empty result).
 * This allows the API to return an empty result with proper pagination
 * metadata rather than an error.
 *
 * @param given_page  User-requested page number (1-indexed)
 * @param max_page    Total number of pages available
 * @returns           VOID (raises exception if validation fails)
 *
 * Example Error:
 *   validate_page(10, 5) raises:
 *   "page <= 5: page of 10 is greater than maximum page"
 *
 * Special Case:
 *   validate_page(1, 0) -- OK: page 1 is always valid
 */
CREATE OR REPLACE FUNCTION btracker_backend.validate_page(given_page BIGINT, max_page INT)
RETURNS VOID -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
IMMUTABLE
AS
$$
BEGIN
  IF given_page > max_page AND given_page != 1 THEN
    RAISE EXCEPTION 'page <= %: page of % is greater than maxmimum page', max_page, given_page;
  END IF;

  RETURN;
END
$$;


/**
 * validate_negative_limit()
 * -------------------------
 * Validate that a limit/page-size is positive (greater than 0).
 *
 * A page-size of 0 or negative makes no sense and would cause
 * division-by-zero errors in pagination calculations.
 *
 * @param given_limit      User-provided limit value
 * @param given_limit_name Parameter name for error message (default: 'page-size')
 * @returns                VOID (raises exception if validation fails)
 *
 * Example Error:
 *   validate_negative_limit(0) raises:
 *   "page-size <= 0: page-size of 0 is lesser or equal 0"
 */
CREATE OR REPLACE FUNCTION btracker_backend.validate_negative_limit(given_limit BIGINT, given_limit_name TEXT DEFAULT 'page-size')
RETURNS VOID -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
IMMUTABLE
AS
$$
BEGIN
  IF given_limit <= 0 THEN
    RAISE EXCEPTION '% <= 0: % of % is lesser or equal 0', given_limit_name, given_limit_name, given_limit;
  END IF;

  RETURN;
END
$$;

/**
 * validate_negative_page()
 * ------------------------
 * Validate that a page number is positive (1 or greater).
 *
 * Pages are 1-indexed, so page 0 or negative pages are invalid.
 *
 * @param given_page  User-requested page number
 * @returns           VOID (raises exception if validation fails)
 *
 * Example Error:
 *   validate_negative_page(0) raises:
 *   "page <= 0: page of 0 is lesser or equal 0"
 */
CREATE OR REPLACE FUNCTION btracker_backend.validate_negative_page(given_page BIGINT)
RETURNS VOID -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
IMMUTABLE
AS
$$
BEGIN
  IF given_page <= 0 THEN
    RAISE EXCEPTION 'page <= 0: page of % is lesser or equal 0', given_page;
  END IF;

  RETURN;
END
$$;

/**
 * rest_raise_missing_account()
 * ----------------------------
 * Raise a user-friendly error when an account is not found.
 *
 * This function is called by validate_account() when account lookup
 * returns NULL. The error message includes the account name so users
 * can identify typos or other issues.
 *
 * @param _account_name  Account name that was not found
 * @returns              Never returns (always raises exception)
 *
 * Error Message:
 *   "Account 'nonexistent_user' does not exist"
 */
CREATE OR REPLACE FUNCTION btracker_backend.rest_raise_missing_account(_account_name TEXT)
RETURNS VOID
LANGUAGE 'plpgsql'
IMMUTABLE
AS
$$
BEGIN
  RAISE EXCEPTION 'Account ''%'' does not exist', _account_name;
END
$$;

/**
 * rest_raise_vest_saving_balance()
 * --------------------------------
 * Raise an error when querying VESTS savings balance (invalid combination).
 *
 * On the Hive blockchain, VESTS cannot be deposited into savings accounts.
 * Only HIVE and HBD have savings functionality. This error explains the
 * limitation to API consumers.
 *
 * @param _balance_type  The balance type requested ('savings_balance')
 * @param _nai_type      The coin type requested ('VESTS')
 * @returns              Never returns (always raises exception)
 *
 * Error Message:
 *   "The selected balance type 'savings_balance' does not support 'VESTS' option.
 *    Please choose a different balance type."
 */
CREATE OR REPLACE FUNCTION btracker_backend.rest_raise_vest_saving_balance(
    _balance_type btracker_backend.balance_type,
    _nai_type btracker_backend.nai_type
)
RETURNS VOID
LANGUAGE 'plpgsql'
IMMUTABLE
AS
$$
BEGIN
  RAISE EXCEPTION 'The selected balance type ''%'' does not support ''%'' option. Please choose a different balance type.',
   _balance_type::TEXT, _nai_type::TEXT;
END
$$;

RESET ROLE;

/**
 * Input Validators for Balance Tracker API
 * =========================================
 *
 * This file contains validation functions that check API input parameters
 * before processing queries. These validators ensure data consistency and
 * provide meaningful error messages to API consumers.
 *
 * Validation Strategy:
 * --------------------
 * Validators are called early in endpoint functions to fail fast with
 * clear error messages. They use PERFORM to call exception-raising
 * functions when validation fails.
 *
 * Dependencies:
 * -------------
 * - btracker_backend.balance_type: ENUM ('balance', 'savings_balance')
 * - btracker_backend.nai_type: ENUM ('HBD', 'HIVE', 'VESTS')
 * - btracker_backend.rest_raise_missing_account(): Exception function
 * - btracker_backend.rest_raise_vest_saving_balance(): Exception function
 *
 * @see exceptions.sql for the underlying exception-raising functions
 */

SET ROLE btracker_owner;

/**
 * validate_account()
 * ------------------
 * Validate that an account exists when required.
 *
 * This function handles two scenarios:
 * 1. Required account: If _required=true and account doesn't exist, raise error
 * 2. Optional account: If account name was provided but ID is NULL, raise error
 *
 * The second scenario catches cases where a user provided an account name
 * in the API request, but that account doesn't exist. Even if the account
 * is "optional" for the query, a non-existent account name is still an error.
 *
 * @param _account_id   Account ID from lookup (NULL if account doesn't exist)
 * @param _account_name Original account name from API request
 * @param _required     Whether the account is required for this query
 * @returns             VOID (raises exception on validation failure)
 *
 * Validation Logic:
 * -----------------
 * Raise exception when:
 *   (_required AND _account_id IS NULL)
 *     -> Account is required but doesn't exist
 *
 *   OR
 *
 *   (NOT _required AND _account_name IS NOT NULL AND _account_id IS NULL)
 *     -> Account is optional, user provided a name, but it doesn't exist
 *
 * Examples:
 * ---------
 * validate_account(123, 'alice', true)   -- OK: account exists
 * validate_account(NULL, 'alice', true)  -- ERROR: required account missing
 * validate_account(NULL, 'alice', false) -- ERROR: user specified non-existent account
 * validate_account(NULL, NULL, false)    -- OK: no account specified, none required
 */
CREATE OR REPLACE FUNCTION btracker_backend.validate_account(_account_id INT, _account_name TEXT, _required BOOLEAN)
RETURNS VOID
LANGUAGE 'plpgsql'
IMMUTABLE
AS
$$
BEGIN
  IF (_required AND _account_id IS NULL) OR (NOT _required AND _account_name IS NOT NULL AND _account_id IS NULL) THEN
    PERFORM btracker_backend.rest_raise_missing_account(_account_name);
  END IF;
END
$$;

/**
 * validate_balance_history()
 * --------------------------
 * Validate that the balance_type and coin_type combination is valid.
 *
 * On the Hive blockchain, VESTS (vesting shares) do not have a savings
 * account. Only HIVE and HBD can be held in savings. This function
 * prevents invalid queries that would return no results anyway.
 *
 * Invalid Combinations:
 * ---------------------
 * - balance_type='savings_balance' AND coin_type='VESTS'
 *   VESTS cannot be deposited into savings, so this query is meaningless.
 *
 * Valid Combinations:
 * -------------------
 * - balance_type='balance' + any coin_type (HBD, HIVE, VESTS)
 * - balance_type='savings_balance' + HBD
 * - balance_type='savings_balance' + HIVE
 *
 * @param _balance_type  'balance' or 'savings_balance'
 * @param _coin_type     'HBD', 'HIVE', or 'VESTS'
 * @returns              VOID (raises exception on invalid combination)
 *
 * @see rest_raise_vest_saving_balance() for the error message format
 */
CREATE OR REPLACE FUNCTION btracker_backend.validate_balance_history(
    _balance_type btracker_backend.balance_type,
    _coin_type btracker_backend.nai_type
)
RETURNS VOID
LANGUAGE 'plpgsql'
IMMUTABLE
AS
$$
BEGIN
  IF _balance_type = 'savings_balance' AND _coin_type = 'VESTS' THEN
    PERFORM btracker_backend.rest_raise_vest_saving_balance(_balance_type, _coin_type);
  END IF;
END
$$;

RESET ROLE;

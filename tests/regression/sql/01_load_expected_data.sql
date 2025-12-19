-- =============================================================================
-- Functions for Loading Expected Account Data
-- =============================================================================
--
-- PURPOSE:
--   Provides functions to load expected account balance data from JSON format
--   (as returned by hived's database_api.list_accounts endpoint) into the
--   test schema for comparison.
--
-- FUNCTIONS:
--   - load_expected_account(jsonb): Inserts a single account's expected values
--   - get_btracker_account_stats(int): Retrieves btracker's computed values
--
-- TYPES:
--   - account_stats_type: Composite type for account statistics
-- =============================================================================

SET ROLE btracker_owner;

-- -----------------------------------------------------------------------------
-- Function: load_expected_account
-- -----------------------------------------------------------------------------
-- Parses a single account's JSON data from hived and inserts it into
-- the expected_account_balances table.
--
-- INPUT:
--   account_data: JSON object representing one account from hived's
--                 database_api.list_accounts response
--
-- JSON STRUCTURE EXPECTED:
--   {
--     "id": 12345,
--     "balance": {"amount": "1000000", ...},
--     "hbd_balance": {"amount": "500000", ...},
--     "vesting_shares": {"amount": "2000000000", ...},
--     ...
--   }
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION btracker_test.load_expected_account(account_data jsonb)
RETURNS void
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
    INSERT INTO btracker_test.expected_account_balances (
        account_id,
        balance,
        hbd_balance,
        vesting_shares,
        savings_balance,
        savings_hbd_balance,
        savings_withdraw_requests,
        reward_hbd_balance,
        reward_hive_balance,
        reward_vesting_balance,
        reward_vesting_hive,
        delegated_vesting_shares,
        received_vesting_shares,
        vesting_withdraw_rate,
        to_withdraw,
        withdrawn,
        withdraw_routes,
        posting_rewards,
        curation_rewards
    )
    SELECT
        (account_data->>'id')::INT,
        (account_data->'balance'->>'amount')::BIGINT,
        (account_data->'hbd_balance'->>'amount')::BIGINT,
        (account_data->'vesting_shares'->>'amount')::BIGINT,
        (account_data->'savings_balance'->>'amount')::BIGINT,
        (account_data->'savings_hbd_balance'->>'amount')::BIGINT,
        (account_data->>'savings_withdraw_requests')::INT,
        (account_data->'reward_hbd_balance'->>'amount')::BIGINT,
        (account_data->'reward_hive_balance'->>'amount')::BIGINT,
        (account_data->'reward_vesting_balance'->>'amount')::BIGINT,
        (account_data->'reward_vesting_hive'->>'amount')::BIGINT,
        (account_data->'delegated_vesting_shares'->>'amount')::BIGINT,
        (account_data->'received_vesting_shares'->>'amount')::BIGINT,
        (account_data->'vesting_withdraw_rate'->>'amount')::BIGINT,
        (account_data->>'to_withdraw')::BIGINT,
        (account_data->>'withdrawn')::BIGINT,
        (account_data->>'withdraw_routes')::INT,
        (account_data->>'posting_rewards')::BIGINT,
        (account_data->>'curation_rewards')::BIGINT;
END;
$$;

COMMENT ON FUNCTION btracker_test.load_expected_account(jsonb) IS
    'Loads a single account''s expected balance data from JSON into the test schema';

-- -----------------------------------------------------------------------------
-- Type: account_stats_type
-- -----------------------------------------------------------------------------
-- Composite type representing all tracked account statistics.
-- Used for comparing expected vs actual values and returning results.
-- -----------------------------------------------------------------------------
DROP TYPE IF EXISTS btracker_test.account_stats_type CASCADE;

CREATE TYPE btracker_test.account_stats_type AS (
    account_id INT,
    balance BIGINT,
    hbd_balance BIGINT,
    vesting_shares BIGINT,
    savings_balance BIGINT,
    savings_hbd_balance BIGINT,
    savings_withdraw_requests INT,
    vesting_withdraw_rate BIGINT,
    to_withdraw BIGINT,
    withdrawn BIGINT,
    withdraw_routes INT,
    reward_hbd_balance BIGINT,
    reward_hive_balance BIGINT,
    reward_vesting_balance BIGINT,
    reward_vesting_hive BIGINT,
    delegated_vesting_shares BIGINT,
    received_vesting_shares BIGINT,
    posting_rewards BIGINT
);

-- -----------------------------------------------------------------------------
-- Function: get_btracker_account_stats
-- -----------------------------------------------------------------------------
-- Retrieves Balance Tracker's computed values for a specific account.
-- This aggregates data from multiple btracker_endpoints functions into
-- a single record for easy comparison.
--
-- INPUT:
--   _account_id: The account ID to retrieve stats for
--
-- RETURNS:
--   account_stats_type record with all balance fields populated from
--   Balance Tracker's computed values
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION btracker_test.get_btracker_account_stats(_account_id INT)
RETURNS btracker_test.account_stats_type
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    result btracker_test.account_stats_type;
BEGIN
    SELECT
        _account_id,
        COALESCE(bal.hive_balance, 0),
        COALESCE(bal.hbd_balance, 0),
        COALESCE(bal.vesting_shares, 0),
        COALESCE(sav.hive_savings, 0),
        COALESCE(sav.hbd_savings, 0),
        COALESCE(sav.savings_withdraw_requests, 0),
        COALESCE(wdr.vesting_withdraw_rate, 0),
        COALESCE(wdr.to_withdraw, 0),
        COALESCE(wdr.withdrawn, 0),
        COALESCE(wdr.withdraw_routes, 0),
        COALESCE(rwd.hbd_rewards, 0),
        COALESCE(rwd.hive_rewards, 0),
        COALESCE(rwd.vests_rewards, 0),
        COALESCE(rwd.hive_vesting_rewards, 0),
        COALESCE(del.delegated_vests, 0),
        COALESCE(del.received_vests, 0),
        COALESCE(info.posting_rewards, 0)
    INTO result
    FROM
        (SELECT * FROM btracker_endpoints.get_account_balances(_account_id)) AS bal,
        (SELECT * FROM btracker_endpoints.get_account_delegations(_account_id)) AS del,
        (SELECT * FROM btracker_endpoints.get_account_rewards(_account_id)) AS rwd,
        (SELECT * FROM btracker_endpoints.get_account_savings(_account_id)) AS sav,
        (SELECT * FROM btracker_endpoints.get_account_info_rewards(_account_id)) AS info,
        (SELECT * FROM btracker_endpoints.get_account_withdraws(_account_id)) AS wdr;

    RETURN result;
END;
$$;

COMMENT ON FUNCTION btracker_test.get_btracker_account_stats(INT) IS
    'Retrieves Balance Tracker''s computed values for an account, aggregating from all endpoints';

RESET ROLE;

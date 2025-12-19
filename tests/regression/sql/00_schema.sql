-- =============================================================================
-- Balance Tracker Regression Test Schema
-- =============================================================================
--
-- PURPOSE:
--   This schema provides infrastructure for regression testing Balance Tracker
--   by comparing its computed values against expected values from a hived node.
--
-- HOW IT WORKS:
--   1. Expected account balances are loaded from a hived node JSON snapshot
--      into the `expected_account_balances` table
--   2. The `compare_accounts()` function compares these expected values
--      against Balance Tracker's computed values from btracker_endpoints
--   3. Any discrepancies are recorded in `differing_accounts` for debugging
--
-- TABLES:
--   - expected_account_balances: Stores expected values from hived node snapshot
--   - differing_accounts: Stores account IDs where btracker differs from expected
--
-- USAGE:
--   This schema should only be installed for regression testing, not in production.
--   Use `tests/regression/install_test_schema.sh` to install.
-- =============================================================================

SET ROLE btracker_owner;

DO $$
BEGIN

-- Create a dedicated schema for regression testing
-- This isolates test infrastructure from production tables
CREATE SCHEMA btracker_test AUTHORIZATION btracker_owner;

-- -----------------------------------------------------------------------------
-- Table: expected_account_balances
-- -----------------------------------------------------------------------------
-- Stores the expected account balance values loaded from a hived node snapshot.
-- These values serve as the "ground truth" for regression testing.
--
-- The data is populated by the Python script `load_expected_balances.py` which
-- reads a JSON dump from hived's database_api.list_accounts endpoint.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS btracker_test.expected_account_balances (
    account_id INT NOT NULL,

    -- Core balances (HIVE, HBD, VESTS)
    balance BIGINT DEFAULT 0,                    -- HIVE balance
    hbd_balance BIGINT DEFAULT 0,                -- HBD balance
    vesting_shares BIGINT DEFAULT 0,             -- VESTS (vesting shares)

    -- Savings balances
    savings_balance BIGINT DEFAULT 0,            -- HIVE in savings
    savings_hbd_balance BIGINT DEFAULT 0,        -- HBD in savings
    savings_withdraw_requests INT DEFAULT 0,     -- Number of pending savings withdrawals

    -- Pending rewards (not yet claimed)
    reward_hbd_balance BIGINT DEFAULT 0,         -- Pending HBD rewards
    reward_hive_balance BIGINT DEFAULT 0,        -- Pending HIVE rewards
    reward_vesting_balance BIGINT DEFAULT 0,     -- Pending VESTS rewards
    reward_vesting_hive BIGINT DEFAULT 0,        -- VESTS rewards expressed as HIVE equivalent

    -- Delegation state
    delegated_vesting_shares BIGINT DEFAULT 0,   -- VESTS delegated TO others
    received_vesting_shares BIGINT DEFAULT 0,    -- VESTS received FROM others

    -- Power-down (vesting withdrawal) state
    vesting_withdraw_rate BIGINT DEFAULT 0,      -- Weekly withdrawal rate
    to_withdraw BIGINT DEFAULT 0,                -- Total VESTS scheduled for withdrawal
    withdrawn BIGINT DEFAULT 0,                  -- VESTS already withdrawn
    withdraw_routes INT DEFAULT 0,               -- Number of withdrawal routes configured

    -- Lifetime earnings
    posting_rewards BIGINT DEFAULT 0,            -- Total posting rewards earned
    curation_rewards BIGINT DEFAULT 0,           -- Total curation rewards earned

    CONSTRAINT pk_expected_account_balances PRIMARY KEY (account_id)
);

-- -----------------------------------------------------------------------------
-- Table: differing_accounts
-- -----------------------------------------------------------------------------
-- Stores account IDs where Balance Tracker's computed values differ from
-- the expected values. Used for debugging regression test failures.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS btracker_test.differing_accounts (
    account_id INT NOT NULL
);

EXCEPTION WHEN duplicate_schema THEN
    RAISE NOTICE 'Schema btracker_test already exists, skipping creation';
END
$$;

RESET ROLE;

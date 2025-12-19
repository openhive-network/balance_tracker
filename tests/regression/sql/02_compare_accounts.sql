-- =============================================================================
-- Account Comparison Functions
-- =============================================================================
--
-- PURPOSE:
--   Provides functions to compare Balance Tracker's computed account balances
--   against expected values loaded from a hived node snapshot.
--
-- FUNCTIONS:
--   - compare_accounts(): Compares all accounts and records discrepancies
--   - get_account_comparison(int): Returns expected vs actual for debugging
--
-- ALGORITHM:
--   1. Load expected values from btracker_test.expected_account_balances
--   2. For each account, query Balance Tracker's computed values via views
--   3. Compare each field and record mismatches in differing_accounts table
-- =============================================================================

SET ROLE btracker_owner;

-- -----------------------------------------------------------------------------
-- Function: compare_accounts
-- -----------------------------------------------------------------------------
-- Compares ALL accounts in the expected_account_balances table against
-- Balance Tracker's computed values. Any account with at least one
-- differing field is recorded in the differing_accounts table.
--
-- This is the main entry point for regression testing.
--
-- SIDE EFFECTS:
--   - Inserts account IDs with discrepancies into differing_accounts table
--   - Should be run after loading expected data and after btracker sync
--
-- PERFORMANCE NOTE:
--   Uses MATERIALIZED CTEs to compute each data source once, then joins.
--   This is efficient when comparing many accounts.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION btracker_test.compare_accounts()
RETURNS void
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN

WITH
-- Expected values from the loaded hived snapshot
expected AS MATERIALIZED (
    SELECT
        account_id,
        balance,
        hbd_balance,
        vesting_shares,
        savings_balance,
        savings_hbd_balance,
        savings_withdraw_requests,
        vesting_withdraw_rate,
        to_withdraw,
        withdrawn,
        withdraw_routes,
        reward_hbd_balance,
        reward_hive_balance,
        reward_vesting_balance,
        reward_vesting_hive,
        delegated_vesting_shares,
        received_vesting_shares,
        posting_rewards
    FROM btracker_test.expected_account_balances
),

-- Balance Tracker's computed HIVE/HBD/VESTS balances
btracker_balances AS MATERIALIZED (
    SELECT
        cab.account AS account_id,
        MAX(CASE WHEN cab.nai = 13 THEN cab.balance END) AS hbd_balance,
        MAX(CASE WHEN cab.nai = 21 THEN cab.balance END) AS balance,
        MAX(CASE WHEN cab.nai = 37 THEN cab.balance END) AS vesting_shares
    FROM btracker_backend.current_account_balances_view cab
    GROUP BY cab.account
),

-- Balance Tracker's computed delegation totals
btracker_delegations AS MATERIALIZED (
    SELECT
        ad.account AS account_id,
        ad.delegated_vests AS delegated_vesting_shares,
        ad.received_vests AS received_vesting_shares
    FROM btracker_app.account_delegations ad
),

-- Balance Tracker's computed pending rewards
btracker_rewards AS MATERIALIZED (
    SELECT
        ar.account AS account_id,
        MAX(CASE WHEN ar.nai = 13 THEN ar.balance END) AS reward_hbd_balance,
        MAX(CASE WHEN ar.nai = 21 THEN ar.balance END) AS reward_hive_balance,
        MAX(CASE WHEN ar.nai = 37 THEN ar.balance END) AS reward_vesting_balance,
        MAX(CASE WHEN ar.nai = 38 THEN ar.balance END) AS reward_vesting_hive
    FROM btracker_backend.account_rewards_view ar
    GROUP BY ar.account
),

-- Balance Tracker's computed savings balances
btracker_savings AS MATERIALIZED (
    SELECT
        asv.account AS account_id,
        MAX(CASE WHEN asv.nai = 13 THEN asv.balance END) AS savings_hbd_balance,
        MAX(CASE WHEN asv.nai = 21 THEN asv.balance END) AS savings_balance
    FROM btracker_backend.account_savings_view asv
    GROUP BY asv.account
),

-- Balance Tracker's computed savings withdrawal request counts
btracker_savings_withdrawals AS MATERIALIZED (
    SELECT
        asv.account AS account_id,
        SUM(asv.savings_withdraw_requests) AS savings_withdraw_requests
    FROM btracker_backend.account_savings_view asv
    GROUP BY asv.account
),

-- Balance Tracker's computed lifetime rewards
btracker_info_rewards AS MATERIALIZED (
    SELECT
        air.account AS account_id,
        air.posting_rewards
    FROM btracker_app.account_info_rewards air
),

-- Balance Tracker's computed power-down state
btracker_withdraws AS MATERIALIZED (
    SELECT
        aw.account AS account_id,
        aw.vesting_withdraw_rate,
        aw.to_withdraw,
        aw.withdrawn,
        aw.withdraw_routes
    FROM btracker_app.account_withdraws aw
),

-- Join expected with all btracker sources for comparison
comparison AS MATERIALIZED (
    SELECT
        e.account_id,

        -- Expected values
        e.balance,
        e.hbd_balance,
        e.vesting_shares,
        e.savings_balance,
        e.savings_hbd_balance,
        e.savings_withdraw_requests,
        e.vesting_withdraw_rate,
        e.to_withdraw,
        e.withdrawn,
        e.withdraw_routes,
        e.reward_hbd_balance,
        e.reward_hive_balance,
        e.reward_vesting_balance,
        e.reward_vesting_hive,
        e.delegated_vesting_shares,
        e.received_vesting_shares,
        e.posting_rewards,

        -- Actual (btracker) values
        COALESCE(bb.balance, 0) AS actual_balance,
        COALESCE(bb.hbd_balance, 0) AS actual_hbd_balance,
        COALESCE(bb.vesting_shares, 0) AS actual_vesting_shares,
        COALESCE(bs.savings_balance, 0) AS actual_savings_balance,
        COALESCE(bs.savings_hbd_balance, 0) AS actual_savings_hbd_balance,
        COALESCE(bsw.savings_withdraw_requests, 0) AS actual_savings_withdraw_requests,
        COALESCE(bw.vesting_withdraw_rate, 0) AS actual_vesting_withdraw_rate,
        COALESCE(bw.to_withdraw, 0) AS actual_to_withdraw,
        COALESCE(bw.withdrawn, 0) AS actual_withdrawn,
        COALESCE(bw.withdraw_routes, 0) AS actual_withdraw_routes,
        COALESCE(br.reward_hbd_balance, 0) AS actual_reward_hbd_balance,
        COALESCE(br.reward_hive_balance, 0) AS actual_reward_hive_balance,
        COALESCE(br.reward_vesting_balance, 0) AS actual_reward_vesting_balance,
        COALESCE(br.reward_vesting_hive, 0) AS actual_reward_vesting_hive,
        COALESCE(bd.delegated_vesting_shares, 0) AS actual_delegated_vesting_shares,
        COALESCE(bd.received_vesting_shares, 0) AS actual_received_vesting_shares,
        COALESCE(bi.posting_rewards, 0) AS actual_posting_rewards

    FROM expected e
    LEFT JOIN btracker_balances bb ON bb.account_id = e.account_id
    LEFT JOIN btracker_delegations bd ON bd.account_id = e.account_id
    LEFT JOIN btracker_rewards br ON br.account_id = e.account_id
    LEFT JOIN btracker_savings bs ON bs.account_id = e.account_id
    LEFT JOIN btracker_savings_withdrawals bsw ON bsw.account_id = e.account_id
    LEFT JOIN btracker_info_rewards bi ON bi.account_id = e.account_id
    LEFT JOIN btracker_withdraws bw ON bw.account_id = e.account_id
)

-- Record all accounts where ANY field differs
INSERT INTO btracker_test.differing_accounts (account_id)
SELECT c.account_id
FROM comparison c
WHERE
       c.balance != c.actual_balance
    OR c.hbd_balance != c.actual_hbd_balance
    OR c.vesting_shares != c.actual_vesting_shares
    OR c.savings_balance != c.actual_savings_balance
    OR c.savings_hbd_balance != c.actual_savings_hbd_balance
    OR c.savings_withdraw_requests != c.actual_savings_withdraw_requests
    OR c.vesting_withdraw_rate != c.actual_vesting_withdraw_rate
    OR c.to_withdraw != c.actual_to_withdraw
    OR c.withdrawn != c.actual_withdrawn
    OR c.withdraw_routes != c.actual_withdraw_routes
    OR c.reward_hbd_balance != c.actual_reward_hbd_balance
    OR c.reward_hive_balance != c.actual_reward_hive_balance
    OR c.reward_vesting_balance != c.actual_reward_vesting_balance
    OR c.reward_vesting_hive != c.actual_reward_vesting_hive
    OR c.delegated_vesting_shares != c.actual_delegated_vesting_shares
    OR c.received_vesting_shares != c.actual_received_vesting_shares
    OR c.posting_rewards != c.actual_posting_rewards;

END;
$$;

COMMENT ON FUNCTION btracker_test.compare_accounts() IS
    'Compares all expected account balances against btracker values, recording discrepancies';

-- -----------------------------------------------------------------------------
-- Function: get_account_comparison
-- -----------------------------------------------------------------------------
-- Returns both expected and actual values for a specific account.
-- Useful for debugging when an account fails the comparison.
--
-- INPUT:
--   _account_id: The account ID to compare
--
-- RETURNS:
--   Two rows as account_stats_type:
--     Row 1: Expected values from the loaded snapshot
--     Row 2: Actual values computed by Balance Tracker
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION btracker_test.get_account_comparison(_account_id INT)
RETURNS SETOF btracker_test.account_stats_type
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    -- Return expected values (from loaded snapshot)
    RETURN QUERY
    SELECT
        account_id,
        balance,
        hbd_balance,
        vesting_shares,
        savings_balance,
        savings_hbd_balance,
        savings_withdraw_requests,
        vesting_withdraw_rate,
        to_withdraw,
        withdrawn,
        withdraw_routes,
        reward_hbd_balance,
        reward_hive_balance,
        reward_vesting_balance,
        reward_vesting_hive,
        delegated_vesting_shares,
        received_vesting_shares,
        posting_rewards
    FROM btracker_test.expected_account_balances
    WHERE account_id = _account_id;

    -- Return actual values (from btracker)
    RETURN QUERY
    SELECT * FROM btracker_test.get_btracker_account_stats(_account_id);
END;
$$;

COMMENT ON FUNCTION btracker_test.get_account_comparison(INT) IS
    'Returns expected vs actual values for a specific account for debugging';

RESET ROLE;

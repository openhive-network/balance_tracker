SET ROLE btracker_owner;

DO $$
BEGIN

CREATE SCHEMA btracker_account_dump AUTHORIZATION btracker_owner;

CREATE TABLE IF NOT EXISTS btracker_account_dump.account_balances (
    name TEXT,
    balance BIGINT DEFAULT 0,
    hbd_balance BIGINT DEFAULT 0,
    vesting_shares BIGINT DEFAULT 0,
    savings_balance BIGINT DEFAULT 0,
    savings_hbd_balance BIGINT DEFAULT 0,
    savings_withdraw_requests INT DEFAULT 0,
    reward_hbd_balance BIGINT DEFAULT 0,
    reward_hive_balance BIGINT DEFAULT 0,
    reward_vesting_balance BIGINT DEFAULT 0,
    reward_vesting_hive BIGINT DEFAULT 0,
    delegated_vesting_shares BIGINT DEFAULT 0,
    received_vesting_shares BIGINT DEFAULT 0,
    vesting_withdraw_rate BIGINT DEFAULT 0,
    to_withdraw BIGINT DEFAULT 0,
    withdrawn BIGINT DEFAULT 0,
    withdraw_routes INT DEFAULT 0,
    posting_rewards BIGINT DEFAULT 0,
    curation_rewards BIGINT DEFAULT 0,

CONSTRAINT pk_account_balances_comparison PRIMARY KEY (name)
);

CREATE TABLE IF NOT EXISTS btracker_account_dump.differing_accounts (
  account_name TEXT
);

EXCEPTION WHEN duplicate_schema THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;

END
$$;

RESET ROLE;

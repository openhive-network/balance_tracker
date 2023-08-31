CREATE TABLE IF NOT EXISTS btracker_da.account_balances (
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
    post_voting_power BIGINT DEFAULT 0,
    posting_rewards BIGINT DEFAULT 0,
    curation_rewards BIGINT DEFAULT 0,
    last_post TIMESTAMP DEFAULT '1970-01-01T00:00:00',
    last_root_post TIMESTAMP DEFAULT '1970-01-01T00:00:00',
    last_vote_time TIMESTAMP DEFAULT '1970-01-01T00:00:00',
    post_count INT DEFAULT 0,

CONSTRAINT pk_account_balances_comparison PRIMARY KEY (name)
);


CREATE OR REPLACE FUNCTION btracker_da.dump_current_account_stats(account_data jsonb)
RETURNS VOID
LANGUAGE 'plpgsql'
VOLATILE
AS
$$
BEGIN
INSERT INTO btracker_da.account_balances 

SELECT
    account_data->>'name'AS name,
    (account_data->'balance'->>'amount')::BIGINT AS balance,
    (account_data->'hbd_balance'->>'amount')::BIGINT AS hbd_balance,
    (account_data->'vesting_shares'->>'amount')::BIGINT AS vesting_shares,
    (account_data->'savings_balance'->>'amount')::BIGINT AS savings_balance,
    (account_data->'savings_hbd_balance'->>'amount')::BIGINT AS savings_hbd_balance,
    (account_data->>'savings_withdraw_requests')::INT AS savings_withdraw_requests,
    (account_data->'reward_hbd_balance'->>'amount')::BIGINT AS reward_hbd_balance,
    (account_data->'reward_hive_balance'->>'amount')::BIGINT AS reward_hive_balance,
    (account_data->'reward_vesting_balance'->>'amount')::BIGINT AS reward_vesting_balance,
    (account_data->'reward_vesting_hive'->>'amount')::BIGINT AS reward_vesting_hive,
    (account_data->'delegated_vesting_shares'->>'amount')::BIGINT AS delegated_vesting_shares,
    (account_data->'received_vesting_shares'->>'amount')::BIGINT AS received_vesting_shares,
    (account_data->'vesting_withdraw_rate'->>'amount')::BIGINT AS vesting_withdraw_rate,
    (account_data->>'to_withdraw')::BIGINT AS to_withdraw,
    (account_data->>'withdrawn')::BIGINT AS withdrawn,
    (account_data->>'withdraw_routes')::INT AS withdraw_routes,
    (account_data->'post_voting_power'->>'amount')::BIGINT AS post_voting_power,
    (account_data->>'posting_rewards')::BIGINT AS posting_rewards,
    (account_data->>'curation_rewards')::BIGINT AS curation_rewards,
    (account_data->>'last_post')::TIMESTAMP AS last_post,
    (account_data->>'last_root_post')::TIMESTAMP AS last_root_post,
    (account_data->>'last_vote_time')::TIMESTAMP AS last_vote_time,
    (account_data->>'post_count')::BIGINT AS post_count; 
END
$$
;

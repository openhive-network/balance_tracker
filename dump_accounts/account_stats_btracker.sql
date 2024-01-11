CREATE OR REPLACE FUNCTION btracker_account_dump.dump_current_account_stats(account_data jsonb)
RETURNS void
LANGUAGE 'plpgsql'
VOLATILE
AS
$$
BEGIN
INSERT INTO btracker_account_dump.account_balances 

SELECT
    (account_data->>'id')::INT AS account_id,
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
    (account_data->>'posting_rewards')::BIGINT AS posting_rewards,
    (account_data->>'curation_rewards')::BIGINT AS curation_rewards;
END
$$;


DROP TYPE IF EXISTS btracker_account_dump.account_type CASCADE; -- noqa: LT01
CREATE TYPE btracker_account_dump.account_type AS
(
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

CREATE OR REPLACE FUNCTION btracker_account_dump.get_account_setof(_account_id int)
RETURNS btracker_account_dump.account_type -- noqa: LT01
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
  __result btracker_account_dump.account_type;
BEGIN
  SELECT
    _account_id,
    COALESCE(_result_balance.hive_balance, 0) AS hive_balance,
    COALESCE(_result_balance.hbd_balance, 0)AS hbd_balance,
    COALESCE(_result_balance.vesting_shares, 0) AS vesting_shares,

    COALESCE(_result_savings.hive_savings, 0) AS hive_savings,
    COALESCE(_result_savings.hbd_savings, 0) AS hbd_savings,
    COALESCE(_result_savings.savings_withdraw_requests, 0) AS savings_withdraw_requests,

    COALESCE(_result_withdraws.vesting_withdraw_rate, 0) AS vesting_withdraw_rate,
    COALESCE(_result_withdraws.to_withdraw, 0) AS to_withdraw,
    COALESCE(_result_withdraws.withdrawn, 0) AS withdrawn,
    COALESCE(_result_withdraws.withdraw_routes, 0) AS withdraw_routes,

    COALESCE(_result_rewards.hbd_rewards, 0) AS hbd_rewards,
    COALESCE(_result_rewards.hive_rewards, 0) AS hive_rewards,
    COALESCE(_result_rewards.vests_rewards, 0) AS vests_rewards,
    COALESCE(_result_rewards.hive_vesting_rewards, 0) AS hive_vesting_rewards,

    COALESCE(_result_vest_balance.delegated_vests, 0) AS delegated_vests,
    COALESCE(_result_vest_balance.received_vests, 0) AS received_vests,

    COALESCE(_result_curation_posting.posting_rewards, 0) AS posting_rewards
  INTO __result
  FROM
    (SELECT * FROM btracker_endpoints.get_account_balances(_account_id)) AS _result_balance,
    (SELECT * FROM btracker_endpoints.get_account_delegations(_account_id)) AS _result_vest_balance,
    (SELECT * FROM btracker_endpoints.get_account_rewards(_account_id)) AS _result_rewards,
    (SELECT * FROM btracker_endpoints.get_account_savings(_account_id)) AS _result_savings,
    (SELECT * FROM btracker_endpoints.get_account_info_rewards(_account_id)) AS _result_curation_posting,
    (SELECT * FROM btracker_endpoints.get_account_withdraws(_account_id)) AS _result_withdraws;

  RETURN __result;
END
$$;

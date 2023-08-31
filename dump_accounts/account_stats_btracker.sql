DROP TYPE IF EXISTS btracker_da.account_type CASCADE;
CREATE TYPE btracker_da.account_type AS
(
  name TEXT,
  balance BIGINT,
  hbd_balance BIGINT,
  vesting_shares BIGINT,
  savings_balance BIGINT,
  savings_hbd_balance BIGINT,
  vesting_withdraw_rate BIGINT,
  to_withdraw BIGINT,
  withdrawn BIGINT,
  reward_hbd_balance BIGINT,
  reward_hive_balance BIGINT,
  reward_vesting_balance BIGINT,
  reward_vesting_hive BIGINT,
  delegated_vesting_shares BIGINT,
  received_vesting_shares BIGINT,
  posting_rewards BIGINT
);

CREATE OR REPLACE FUNCTION btracker_da.get_account_setof(_account TEXT)
RETURNS btracker_da.account_type
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
  __account_id INT := (SELECT id FROM hive.accounts_view WHERE name = _account);
  __result btracker_da.account_type;
BEGIN
  SELECT
    _account,
    COALESCE(_result_balance.hive_balance, 0) AS hive_balance,
    COALESCE(_result_balance.hbd_balance, 0)AS hbd_balance,
    COALESCE(_result_balance.vesting_shares, 0) AS vesting_shares,

    COALESCE(_result_savings.hive_savings, 0) AS hive_savings,
    COALESCE(_result_savings.hbd_savings, 0) AS hbd_savings,

    COALESCE(_result_withdraws.vesting_withdraw_rate, 0) AS vesting_withdraw_rate,
    COALESCE(_result_withdraws.to_withdraw, 0) AS to_withdraw,
    COALESCE(_result_withdraws.withdrawn, 0) AS withdrawn,

    COALESCE(_result_rewards.hbd_rewards, 0) AS hbd_rewards,
    COALESCE(_result_rewards.hive_rewards, 0) AS hive_rewards,
    COALESCE(_result_rewards.vests_rewards, 0) AS vests_rewards,
    COALESCE(_result_rewards.hive_vesting_rewards, 0) AS hive_vesting_rewards,

    COALESCE(_result_vest_balance.delegated_vests, 0) AS delegated_vests,
    COALESCE(_result_vest_balance.received_vests, 0) AS received_vests,

    COALESCE(_result_curation_posting.posting_rewards, 0) AS posting_rewards
  INTO __result
  FROM
    (SELECT * FROM btracker_endpoints.get_account_balances(__account_id)) AS _result_balance,
    (SELECT * FROM btracker_endpoints.get_account_delegations(__account_id)) AS _result_vest_balance,
    (SELECT * FROM btracker_endpoints.get_account_rewards(__account_id)) AS _result_rewards,
    (SELECT * FROM btracker_endpoints.get_account_savings(__account_id)) AS _result_savings,
    (SELECT * FROM btracker_endpoints.get_account_info_rewards(__account_id)) AS _result_curation_posting,
    (SELECT * FROM btracker_endpoints.get_account_withdraws(__account_id)) AS _result_withdraws

  RETURN __result;
END
$$
;

CREATE OR REPLACE FUNCTION btracker_da.compare_differing_account(_account TEXT)
RETURNS SETOF btracker_da.account_type
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  RETURN QUERY SELECT 
    name,
    balance,
    hbd_balance,
    vesting_shares,
    savings_balance,
    savings_hbd_balance,
    vesting_withdraw_rate,
    to_withdraw,
    withdrawn,
    reward_hbd_balance,
    reward_hive_balance,
    reward_vesting_balance,
    reward_vesting_hive,
    delegated_vesting_shares,
    received_vesting_shares,
    posting_rewards,

  FROM btracker_da.account_balances WHERE name = _account
  UNION ALL
SELECT * FROM btracker_da.get_account_setof(_account);

END
$$
;

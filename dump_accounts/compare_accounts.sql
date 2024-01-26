SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_account_dump.compare_accounts()
RETURNS void
LANGUAGE 'plpgsql'
VOLATILE
AS
$$
BEGIN
WITH account_balances AS MATERIALIZED (
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

  FROM btracker_account_dump.account_balances
),
account_hive_hbd_vests AS MATERIALIZED (
  SELECT  
    account as account_id,
    MAX(CASE WHEN nai = 13 THEN balance END) AS hbd_balance,
    MAX(CASE WHEN nai = 21 THEN balance END) AS balance, 
    MAX(CASE WHEN nai = 37 THEN balance END) AS vesting_shares 
  FROM btracker_app.current_account_balances
  GROUP BY account
),
account_delegations AS MATERIALIZED (
  SELECT 
    cv.account as account_id,
    cv.delegated_vests AS delegated_vesting_shares,
    cv.received_vests AS received_vesting_shares
  FROM btracker_app.account_delegations cv 
  GROUP BY cv.account
),
account_rewards AS MATERIALIZED (
  SELECT
    account as account_id,
    MAX(CASE WHEN nai = 13 THEN balance END) AS reward_hbd_balance,
    MAX(CASE WHEN nai = 21 THEN balance END) AS reward_hive_balance,
    MAX(CASE WHEN nai = 37 THEN balance END) AS reward_vesting_balance,
    MAX(CASE WHEN nai = 38 THEN balance END) AS reward_vesting_hive
  FROM btracker_app.account_rewards
  GROUP BY account
),
account_savings AS MATERIALIZED (
  SELECT  
    account as account_id,
    MAX(CASE WHEN nai = 13 THEN saving_balance END) AS savings_hbd_balance,
    MAX(CASE WHEN nai = 21 THEN saving_balance END) AS savings_balance
  FROM btracker_app.account_savings
  GROUP BY account
),
account_withdraw_savings AS MATERIALIZED (
  SELECT 
    account as account_id,
    SUM (savings_withdraw_requests) AS savings_withdraw_requests
  FROM btracker_app.account_savings
  GROUP BY account
),
account_info_rewards AS MATERIALIZED (
  SELECT 
    cv.account as account_id,
    cv.posting_rewards as posting_rewards
  FROM btracker_app.account_info_rewards cv
  GROUP BY cv.account
),
account_withdraws AS MATERIALIZED (
  SELECT 
    account as account_id,
    vesting_withdraw_rate,
    to_withdraw,
    withdrawn,
    withdraw_routes
  FROM btracker_app.account_withdraws 
  GROUP BY account
),
selected AS MATERIALIZED (
SELECT
  ab.account_id,
  ab.balance,
  ab.hbd_balance,
  ab.vesting_shares,
  ab.savings_balance,
  ab.savings_hbd_balance,
  ab.savings_withdraw_requests,
  ab.vesting_withdraw_rate,
  ab.to_withdraw,
  ab.withdrawn,
  ab.withdraw_routes,
  ab.reward_hbd_balance,
  ab.reward_hive_balance,
  ab.reward_vesting_balance,
  ab.reward_vesting_hive,
  ab.delegated_vesting_shares,
  ab.received_vesting_shares,
  ab.posting_rewards,

  COALESCE(ahhv.balance, 0) AS current_balance,
  COALESCE(ahhv.hbd_balance, 0) AS current_hbd_balance,
  COALESCE(ahhv.vesting_shares, 0) AS current_vesting_shares,
  COALESCE(asa.savings_balance, 0) AS current_savings_balance,
  COALESCE(asa.savings_hbd_balance, 0) AS current_savings_hbd_balance,
  COALESCE(aws.savings_withdraw_requests, 0) AS current_savings_withdraw_requests,
  COALESCE(aw.vesting_withdraw_rate, 0) AS current_vesting_withdraw_rate,
  COALESCE(aw.to_withdraw, 0) AS current_to_withdraw,
  COALESCE(aw.withdrawn, 0) AS current_withdrawn,
  COALESCE(aw.withdraw_routes, 0) AS current_withdraw_routes,
  COALESCE(ar.reward_hbd_balance, 0) AS current_reward_hbd_balance,
  COALESCE(ar.reward_hive_balance, 0) AS current_reward_hive_balance,
  COALESCE(ar.reward_vesting_balance, 0) AS current_reward_vesting_balance,
  COALESCE(ar.reward_vesting_hive, 0) AS current_reward_vesting_hive,
  COALESCE(ad.delegated_vesting_shares, 0) AS current_delegated_vesting_shares,
  COALESCE(ad.received_vesting_shares, 0) AS current_received_vesting_shares,
  COALESCE(air.posting_rewards, 0) AS current_posting_rewards

FROM account_balances ab
LEFT JOIN account_hive_hbd_vests ahhv ON ahhv.account_id = ab.account_id
LEFT JOIN account_delegations ad ON ad.account_id = ab.account_id
LEFT JOIN account_rewards ar ON ar.account_id = ab.account_id
LEFT JOIN account_savings asa ON asa.account_id = ab.account_id
LEFT JOIN account_withdraw_savings aws ON aws.account_id = ab.account_id
LEFT JOIN account_info_rewards air ON air.account_id = ab.account_id
LEFT JOIN account_withdraws aw ON aw.account_id = ab.account_id
)

INSERT INTO btracker_account_dump.differing_accounts
SELECT ab.account_id
FROM selected ab
WHERE 
     ab.balance != ab.current_balance 
  OR ab.hbd_balance != ab.current_hbd_balance
  OR ab.vesting_shares != ab.current_vesting_shares
  OR ab.savings_balance != ab.current_savings_balance
  OR ab.savings_hbd_balance != ab.current_savings_hbd_balance
  OR ab.savings_withdraw_requests != ab.current_savings_withdraw_requests
  OR ab.vesting_withdraw_rate != ab.current_vesting_withdraw_rate
  OR ab.to_withdraw != ab.current_to_withdraw
  OR ab.withdrawn != ab.current_withdrawn
  OR ab.withdraw_routes != ab.current_withdraw_routes
  OR ab.reward_hbd_balance != ab.current_reward_hbd_balance
  OR ab.reward_hive_balance != ab.current_reward_hive_balance
  OR ab.reward_vesting_balance != ab.current_reward_vesting_balance
  OR ab.reward_vesting_hive != ab.current_reward_vesting_hive
  OR ab.delegated_vesting_shares != ab.current_delegated_vesting_shares
  OR ab.received_vesting_shares != ab.current_received_vesting_shares
  OR ab.posting_rewards != ab.current_posting_rewards

;
END
$$;

CREATE OR REPLACE FUNCTION btracker_account_dump.compare_differing_account(_account_id int)
RETURNS SETOF btracker_account_dump.account_type -- noqa: LT01
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN QUERY SELECT 
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

  FROM btracker_account_dump.account_balances WHERE account_id = _account_id
  UNION ALL
SELECT * FROM btracker_account_dump.get_account_setof(_account_id);

END
$$;

RESET ROLE;

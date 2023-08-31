CREATE TABLE IF NOT EXISTS btracker_da.differing_accounts (
  account_name TEXT
);

CREATE OR REPLACE FUNCTION btracker_da.compare_accounts()
RETURNS VOID
LANGUAGE 'plpgsql'
VOLATILE
AS
$$
BEGIN
WITH account_balances AS (
  SELECT 
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
    posting_rewards

  FROM btracker_da.account_balances
)
INSERT INTO btracker_da.differing_accounts
SELECT account_balances.name
FROM account_balances
JOIN btracker_da.get_account_setof(account_balances.name) AS _current_account_stats
ON _current_account_stats.name = account_balances.name
WHERE account_balances.balance <> _current_account_stats.balance 
  OR account_balances.hbd_balance <> _current_account_stats.hbd_balance
  OR account_balances.vesting_shares <> _current_account_stats.vesting_shares
  OR account_balances.savings_balance <> _current_account_stats.savings_balance
  OR account_balances.savings_hbd_balance <> _current_account_stats.savings_hbd_balance

  OR account_balances.vesting_withdraw_rate <> _current_account_stats.vesting_withdraw_rate
  OR account_balances.to_withdraw <> _current_account_stats.to_withdraw
  OR account_balances.withdrawn <> _current_account_stats.withdrawn

  OR account_balances.reward_hbd_balance <> _current_account_stats.reward_hbd_balance
  OR account_balances.reward_hive_balance <> _current_account_stats.reward_hive_balance
  OR account_balances.reward_vesting_balance <> _current_account_stats.reward_vesting_balance
  OR account_balances.reward_vesting_hive <> _current_account_stats.reward_vesting_hive
  OR account_balances.delegated_vesting_shares <> _current_account_stats.delegated_vesting_shares
  OR account_balances.received_vesting_shares <> _current_account_stats.received_vesting_shares
  OR account_balances.posting_rewards <> _current_account_stats.posting_rewards
;
END
$$
;

CREATE OR REPLACE FUNCTION btracker_da.compare_differing_account(_account TEXT)
RETURNS SETOF btracker_da.account_type
LANGUAGE 'plpgsql' STABLE
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
    posting_rewards
  FROM btracker_da.account_balances WHERE name = _account
  UNION ALL
SELECT * FROM btracker_da.get_account_setof(_account);

END
$$
;

SET ROLE btracker_owner;

/** openapi:paths
/accounts/{account-name}/balances:
  get:
    tags:
      - Accounts
    summary: Account balances
    description: |
      Lists account hbd, hive and vest balances

      SQL example
      * `SELECT * FROM btracker_endpoints.get_account_balances(''blocktrades'');`

      REST call example
      * `GET ''https://%1$s/balance-api/accounts/blocktrades/balances''`
    operationId: btracker_endpoints.get_account_balances
    parameters:
      - in: path
        name: account-name
        required: true
        schema:
          type: string
        description: Name of the account
    responses:
      '200':
        description: |
          Account balances 
          (VEST balances are represented as string due to json limitations)

          * Returns `btracker_backend.balance`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.balance'
            example: {
              "hbd_balance": 77246982,
              "hive_balance": 29594875,
              "vesting_shares": "8172549681941451",
              "vesting_balance_hive": 2720696229,
              "post_voting_power_vests": "8172549681941451",
              "delegated_vests": "0",
              "received_vests": "0",
              "curation_rewards": "196115157",
              "posting_rewards": "65916519",
              "hbd_rewards": 0,
              "hive_rewards": 0,
              "vests_rewards": "0",
              "hive_vesting_rewards": 0,
              "hbd_savings": 0,
              "hive_savings": 0,
              "savings_withdraw_requests": 0,
              "vesting_withdraw_rate": "80404818220529",
              "to_withdraw": "8362101094935031",
              "withdrawn": "804048182205290",
              "withdraw_routes": 4,
              "delayed_vests": "0",
              conversion_pending_amount_hbd: 0,
              conversion_pending_count_hbd: 0,
              conversion_pending_amount_hive: 0,
              conversion_pending_count_hive: 0,
              open_orders_hbd_count: 0,
              open_orders_hive_count: 0,
              open_orders_hive_amount: 0,
              open_orders_hbd_amount: 0

            }
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_balances;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_balances(
    "account-name" TEXT
)
RETURNS btracker_backend.balance 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
  _account_id INT = (SELECT av.id FROM hive.accounts_view av WHERE av.name = "account-name");
BEGIN
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

  IF _account_id IS NULL THEN
    PERFORM btracker_backend.rest_raise_missing_account("account-name");
  END IF;

  --balance
  RETURN btracker_backend.get_acc_balances(_account_id);
END
$$;

RESET ROLE;

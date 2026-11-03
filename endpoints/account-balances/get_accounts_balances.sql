SET ROLE btracker_owner;

/** openapi:paths
/balances:
  get:
    tags:
      - Accounts
    summary: Balances of several accounts
    description: |
      Lists the balances of up to 100 accounts, at the head block or at a past block (`at-block`):
      one entry per requested account, in request order, each holding the account''s name and
      its balances exactly as `/accounts/{account-name}/balances` returns them.

      SQL example
      * `SELECT * FROM btracker_endpoints.get_accounts_balances(''blocktrades,gtg'');`

      * `SELECT * FROM btracker_endpoints.get_accounts_balances(''blocktrades,gtg'', ''4000000'');`

      REST call example
      * `GET ''https://%1$s/balance-api/balances?accounts=blocktrades,gtg''`

      * `GET ''https://%1$s/balance-api/balances?accounts=blocktrades,gtg&at-block=4000000''`
    operationId: btracker_endpoints.get_accounts_balances
    parameters:
      - in: query
        name: accounts
        required: true
        schema:
          type: string
          x-sql-default-value: "NULL"
        description: Comma-separated account names (at most 100), e.g. `blocktrades,gtg`
      - in: query
        name: at-block
        required: false
        schema:
          type: string
          default: NULL
        description: |
          Block whose balances to return, as for `/accounts/{account-name}/balances`: a block-number
          (integer) or a timestamp (YYYY-MM-DD HH:MI:SS, converted to the last block created at or
          before it), between block 1 and the last block balance tracker has processed. At a block
          before the last processed one, the fields without history are `null`.
    responses:
      '200':
        description: |
          Balances per requested account

          * Returns array of `btracker_backend.account_balance`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.array_of_account_balance'
            example: [
              {
                "account_name": "gtg",
                "balance": {
                  "hbd_balance": 3465,
                  "hive_balance": 0,
                  "vesting_shares": "17579100476774",
                  "vesting_balance_hive": 5852200,
                  "post_voting_power_vests": "17579100476774",
                  "delegated_vests": "0",
                  "received_vests": "0",
                  "curation_rewards": "19888",
                  "posting_rewards": "1972916",
                  "hbd_rewards": 0,
                  "hive_rewards": 0,
                  "vests_rewards": "0",
                  "hive_vesting_rewards": 0,
                  "hbd_savings": 0,
                  "hive_savings": 0,
                  "savings_withdraw_requests": 0,
                  "vesting_withdraw_rate": "0",
                  "to_withdraw": "0",
                  "withdrawn": "0",
                  "withdraw_routes": 0,
                  "delayed_vests": "0",
                  "conversion_pending_amount_hbd": 0,
                  "conversion_pending_count_hbd": 0,
                  "conversion_pending_amount_hive": 0,
                  "conversion_pending_count_hive": 0,
                  "open_orders_hbd_count": 0,
                  "open_orders_hive_count": 0,
                  "open_orders_hive_amount": 0,
                  "open_orders_hbd_amount": 0,
                  "savings_pending_amount_hbd": 0,
                  "savings_pending_amount_hive": 0,
                  "escrow_pending_amount_hbd": 0,
                  "escrow_pending_amount_hive": 0,
                  "escrow_pending_count": 0
                }
              }
            ]
      '400':
        description: More than 100 accounts, an unknown account, or a malformed or out-of-range `at-block`
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_accounts_balances;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_accounts_balances(
    "accounts" TEXT = NULL,
    "at-block" TEXT = NULL
)
RETURNS SETOF btracker_backend.account_balance 
-- openapi-generated-code-end
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  _account_names TEXT[] := btracker_backend.parse_account_names("accounts", 100);
  _block_num INT        := btracker_backend.resolve_at_block("at-block");
BEGIN
  PERFORM btracker_backend.set_history_cache_headers(_block_num);

  RETURN QUERY
  SELECT
    a.account_name,
    btracker_backend.account_balances_at(btracker_backend.get_account_id(a.account_name, TRUE), _block_num)
  FROM unnest(_account_names) WITH ORDINALITY AS a(account_name, pos)
  ORDER BY a.pos;
END
$$;

RESET ROLE;

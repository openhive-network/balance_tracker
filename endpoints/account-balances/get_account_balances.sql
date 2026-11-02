SET ROLE btracker_owner;

/** openapi:paths
/accounts/{account-name}/balances:
  get:
    tags:
      - Accounts
    summary: Account balances
    description: |
      Lists account hbd, hive and vest balances, at the head block or at a past block (`at-block`)

      SQL example
      * `SELECT * FROM btracker_endpoints.get_account_balances(''blocktrades'');`

      * `SELECT * FROM btracker_endpoints.get_account_balances(''blocktrades'', ''4000000'');`

      REST call example
      * `GET ''https://%1$s/balance-api/accounts/blocktrades/balances''`

      * `GET ''https://%1$s/balance-api/accounts/blocktrades/balances?at-block=4000000''`
    operationId: btracker_endpoints.get_account_balances
    parameters:
      - in: path
        name: account-name
        required: true
        schema:
          type: string
        description: Name of the account
      - in: query
        name: at-block
        required: false
        schema:
          type: string
          default: NULL
        description: |
          Block whose balances to return, either a block-number (integer) or a timestamp
          (in the format YYYY-MM-DD HH:MI:SS), which is converted to the last block
          created at or before it. Between block 1 and the last block balance tracker
          has processed (`/last-synced-block`). Without it, or at the last processed
          block, the head block''s balances are returned.

          At a block before the last processed one, these fields are the values at that block:
          `hbd_balance`, `hive_balance`, `vesting_shares`, `vesting_balance_hive` (at that
          block''s VESTS price), `post_voting_power_vests`, `delegated_vests`, `received_vests`,
          `hbd_savings`, `hive_savings`, `vesting_withdraw_rate`, `to_withdraw` and `withdrawn`.
          Balance tracker keeps no history of the other fields: they are `null`.

          example input:

          * `2016-09-15 19:47:21`

          * `5000000`
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
              open_orders_hbd_amount: 0,
              savings_pending_amount_hbd: 0,
              savings_pending_amount_hive: 0,
              escrow_pending_amount_hbd: 0,
              escrow_pending_amount_hive: 0,
              escrow_pending_count: 0
            }
      '400':
        description: The `at-block` is malformed or out of range
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_balances;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_balances(
    "account-name" TEXT,
    "at-block" TEXT = NULL
)
RETURNS btracker_backend.balance 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
STABLE
AS
$$
/*
================================================================================
ENDPOINT: get_account_balances
================================================================================
PURPOSE:
  Returns comprehensive balance information for a single Hive account, including
  liquid balances, vesting shares, delegations, rewards, savings, power-down state,
  pending conversions, open market orders, and escrow amounts.

ARCHITECTURE:
  This is a thin wrapper that:
  1. Resolves account name to internal account_id (with 404 on missing account)
     and at-block to a past block (NULL: the head)
  2. Sets HTTP cache headers for PostgREST (2 second cache for live data, 1 year
     for an irreversible past block)
  3. Delegates to btracker_backend.account_balances_at(): the head helper
     btracker_backend.get_account_balances(), or the history reader
     btracker_backend.get_account_balances_at_block() for a past block

DATA SOURCES (via backend helper):
  - current_account_balances: Liquid HBD/HIVE/VESTS balances
  - account_delegations: Delegated and received VESTS summary
  - account_info_rewards: Lifetime curation and posting rewards
  - account_rewards: Pending (unclaimed) reward balances
  - account_savings: Savings balances with pending withdrawal counts
  - account_withdraws: Power-down state (rate, amounts, routes, delayed vests)
  - convert_state: Pending HBD<->HIVE conversions
  - order_state: Open limit orders on internal market
  - transfer_saving_id: Pending savings withdrawal requests
  - escrow_state: Active escrow agreements

PERFORMANCE NOTES:
  - Single account lookup, so all queries are point lookups by account_id
  - Backend uses MATERIALIZED CTEs to prevent repeated computation
  - 2-second cache appropriate for frequently-changing balance data

RETURN TYPE: btracker_backend.balance (composite type with 33 fields)
  - Numeric values use BIGINT for precision
  - Large values (VESTS) use TEXT to avoid JSON integer overflow
================================================================================
*/
DECLARE
  _account_id INT := btracker_backend.get_account_id("account-name", TRUE);
  _block_num INT  := btracker_backend.resolve_at_block("at-block");
BEGIN
  PERFORM btracker_backend.set_history_cache_headers(_block_num);

  RETURN btracker_backend.account_balances_at(_account_id, _block_num);
END
$$;

RESET ROLE;

SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_endpoints.account_balance:
  type: object
  properties:
    hbd_balance:
      type: integer
      x-sql-datatype: BIGINT
      description: number of HIVE backed dollars the account has
    hive_balance:
      type: integer
      x-sql-datatype: BIGINT
      description: account's HIVE balance
    vesting_shares:
      type: integer
      x-sql-datatype: BIGINT
      description: account's VEST balance
    vesting_balance_hive:
      type: integer
      x-sql-datatype: BIGINT
      description: >- 
        the VEST balance, presented in HIVE, 
        is calculated based on the current HIVE price
    post_voting_power_vests:
      type: integer
      x-sql-datatype: BIGINT
      description: >-
        account's VEST balance - delegated VESTs + reveived VESTs,
        presented in HIVE,calculated based on the current HIVE price
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_endpoints.account_balance CASCADE;
CREATE TYPE btracker_endpoints.account_balance AS (
    "hbd_balance" BIGINT,
    "hive_balance" BIGINT,
    "vesting_shares" BIGINT,
    "vesting_balance_hive" BIGINT,
    "post_voting_power_vests" BIGINT
);
-- openapi-generated-code-end

/** openapi:paths
/account-balances/{account-name}:
  get:
    tags:
      - Account-balances
    summary: Account balances
    description: |
      Lists account hbd, hive and vest balances

      SQL example
      * `SELECT * FROM btracker_endpoints.get_account_balances('blocktrades');`

      * `SELECT * FROM btracker_endpoints.get_account_balances('initminer');`

      REST call example
      * `GET https://{btracker-host}/%1$s/account-balances/blocktrades`
      
      * `GET https://{btracker-host}/%1$s/account-balances/initminer`
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

          * Returns `btracker_endpoints.account_balance`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_endpoints.account_balance'
            example:
              - hbd_balance: 1000
                hive_balance: 1000
                vesting_shares: 1000
                vesting_balance_hive: 1000
                post_voting_power_vests: 1000
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_balances;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_balances(
    "account-name" TEXT
)
RETURNS btracker_endpoints.account_balance 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
  __result btracker_endpoints.account_balance;
  _account INT = (SELECT av.id FROM hive.accounts_view av WHERE av.name = "account-name");
BEGIN

PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

SELECT  
  MAX(CASE WHEN nai = 13 THEN balance END) AS hbd,
  MAX(CASE WHEN nai = 21 THEN balance END) AS hive, 
  MAX(CASE WHEN nai = 37 THEN balance END) AS vest 
INTO __result
FROM current_account_balances WHERE account= _account;

SELECT hive.get_vesting_balance((SELECT num FROM hive.blocks_view ORDER BY num DESC LIMIT 1), __result.vesting_shares) 
INTO __result.vesting_balance_hive;

SELECT (__result.vesting_shares - delegated_vests + received_vests) 
INTO __result.post_voting_power_vests
FROM btracker_endpoints.get_account_delegations("account-name");

RETURN __result;

END
$$;

RESET ROLE;

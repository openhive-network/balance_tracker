SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_endpoints.account_withdrawals:
  type: object
  properties:
    vesting_withdraw_rate:
      type: integer
      x-sql-datatype: BIGINT
      description: >-
        received until the withdrawal is complete, 
        with each installment amounting to 1/13 of the withdrawn total
    to_withdraw:
      type: integer
      x-sql-datatype: BIGINT
      description: the remaining total VESTS needed to complete withdrawals
    withdrawn:
      type: integer
      x-sql-datatype: BIGINT
      description: the total VESTS already withdrawn from active withdrawals
    withdraw_routes:
      type: integer
      description: list of account receiving the part of a withdrawal
    delayed_vests:
      type: integer
      x-sql-datatype: BIGINT
      description: blocked VESTS by a withdrawal
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_endpoints.account_withdrawals CASCADE;
CREATE TYPE btracker_endpoints.account_withdrawals AS (
    "vesting_withdraw_rate" BIGINT,
    "to_withdraw" BIGINT,
    "withdrawn" BIGINT,
    "withdraw_routes" INT,
    "delayed_vests" BIGINT
);
-- openapi-generated-code-end

/** openapi:paths
/btracker/account-balances/{account-name}/withdrawals:
  get:
    tags:
      - Account-balances
    summary: Account withdrawals
    description: |
      Returns current vest withdrawals, amount withdrawn, amount left to withdraw and 
      rate of a withdrawal 

      SQL example
      * `SELECT * FROM btracker_endpoints.get_account_withdraws('blocktrades');`

      * `SELECT * FROM btracker_endpoints.get_account_withdraws('initminer');`

      REST call example
      * `GET https://{btracker-host}/btracker/account-balances/blocktrades/withdrawals`
      
      * `GET https://{btracker-host}/btracker/account-balances/initminer/withdrawals`
    operationId: btracker_endpoints.get_account_withdraws
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
          Account withdrawals

          * Returns `btracker_endpoints.account_withdrawals`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_endpoints.account_withdrawals'
            example:
              - vesting_withdraw_rate: 1000
                to_withdraw: 1000
                withdrawn: 1000
                withdraw_routes: 1
                delayed_vests: 1000
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_withdraws;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_withdraws(
    "account-name" TEXT
)
RETURNS btracker_endpoints.account_withdrawals 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
  __result btracker_endpoints.account_withdrawals;
  _account INT = (SELECT av.id FROM hive.accounts_view av WHERE av.name = "account-name");
BEGIN

PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

SELECT vesting_withdraw_rate, to_withdraw, withdrawn, withdraw_routes, delayed_vests
INTO __result
FROM account_withdraws WHERE account= _account;
RETURN __result;
END
$$;

RESET ROLE;

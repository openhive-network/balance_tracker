SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_endpoints.account_savings:
  type: object
  properties:
    hbd_savings:
      type: integer
      x-sql-datatype: numeric
      description: saving balance of HIVE backed dollars
    hive_savings:
      type: integer
      x-sql-datatype: numeric
      description: HIVE saving balance
    savings_withdraw_requests:
      type: integer
      description: >-
        number representing how many payouts are pending 
        from user''s saving balance
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_endpoints.account_savings CASCADE;
CREATE TYPE btracker_endpoints.account_savings AS (
    "hbd_savings" numeric,
    "hive_savings" numeric,
    "savings_withdraw_requests" INT
);
-- openapi-generated-code-end

/** openapi:paths
/account-balances/{account-name}/savings:
  get:
    tags:
      - Account-balances
    summary: Account saving balances
    description: |
      Returns current hbd and hive savings

      SQL example
      * `SELECT * FROM btracker_endpoints.get_account_savings(''blocktrades'');`

      * `SELECT * FROM btracker_endpoints.get_account_savings(''initminer'');`

      REST call example
      * `GET https://{btracker-host}/%1$s/account-balances/blocktrades/savings`
      
      * `GET https://{btracker-host}/%1$s/account-balances/initminer/savings`
    operationId: btracker_endpoints.get_account_savings
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
          Account saving balance

          * Returns `btracker_endpoints.account_savings`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_endpoints.account_savings'
            example:
              - hbd_savings: 1000
                hive_savings: 1000
                savings_withdraw_requests: 1
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_savings;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_savings(
    "account-name" TEXT
)
RETURNS btracker_endpoints.account_savings 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
  __result btracker_endpoints.account_savings;
  _account INT = (SELECT av.id FROM hive.accounts_view av WHERE av.name = "account-name");
BEGIN

PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

SELECT  
  MAX(CASE WHEN nai = 13 THEN saving_balance END) AS hbd,
  MAX(CASE WHEN nai = 21 THEN saving_balance END) AS hive
INTO __result.hbd_savings, __result.hive_savings
FROM account_savings WHERE account= _account;

SELECT SUM (savings_withdraw_requests) AS total
INTO __result.savings_withdraw_requests
FROM account_savings
WHERE account= _account;

RETURN __result;
END
$$;

RESET ROLE;

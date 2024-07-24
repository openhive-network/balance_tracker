SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_endpoints.account_rewards:
  type: object
  properties:
    hbd_rewards:
      type: integer
      x-sql-datatype: numeric
      description: >-
        not yet claimed HIVE backed dollars 
        stored in hbd reward balance
    hive_rewards:
      type: integer
      x-sql-datatype: numeric
      description: >-
        not yet claimed HIVE 
        stored in hive reward balance
    vests_rewards:
      type: integer
      x-sql-datatype: numeric
      description: >-
        not yet claimed VESTS 
        stored in vest reward balance
    hive_vesting_rewards:
      type: integer
      x-sql-datatype: numeric
      description: >-
        the reward vesting balance, denominated in HIVE, 
        is determined by the prevailing HIVE price at the time of reward reception
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_endpoints.account_rewards CASCADE;
CREATE TYPE btracker_endpoints.account_rewards AS (
    "hbd_rewards" numeric,
    "hive_rewards" numeric,
    "vests_rewards" numeric,
    "hive_vesting_rewards" numeric
);
-- openapi-generated-code-end

/** openapi:paths
/account-balances/{account-name}/rewards:
  get:
    tags:
      - Account-balances
    summary: Account reward balances
    description: |
      Returns current, not yet claimed rewards obtained by posting and commenting

      SQL example
      * `SELECT * FROM btracker_endpoints.get_account_rewards(''blocktrades'');`

      * `SELECT * FROM btracker_endpoints.get_account_rewards(''initminer'');`

      REST call example
      * `GET https://{btracker-host}/%1$s/account-balances/blocktrades/rewards`
      
      * `GET https://{btracker-host}/%1$s/account-balances/initminer/rewards`
    operationId: btracker_endpoints.get_account_rewards
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
          Account reward balance

          * Returns `btracker_endpoints.account_rewards`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_endpoints.account_rewards'
            example:
              - hbd_rewards: 1000
                hive_rewards: 1000
                vests_rewards: 1000
                hive_vesting_rewards: 1000
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_rewards;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_rewards(
    "account-name" TEXT
)
RETURNS btracker_endpoints.account_rewards 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
  __result btracker_endpoints.account_rewards;
  _account INT = (SELECT av.id FROM hive.accounts_view av WHERE av.name = "account-name");
BEGIN

PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

SELECT  
  MAX(CASE WHEN nai = 13 THEN balance END) AS hbd,
  MAX(CASE WHEN nai = 21 THEN balance END) AS hive,
  MAX(CASE WHEN nai = 37 THEN balance END) AS vests,
  MAX(CASE WHEN nai = 38 THEN balance END) AS vesting_hive
INTO __result
FROM account_rewards WHERE account= _account;

RETURN __result;
END
$$;

RESET ROLE;

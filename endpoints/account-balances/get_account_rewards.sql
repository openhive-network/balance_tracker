SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_endpoints.rewards:
  type: object
  properties:
    hbd_rewards:
      type: integer
      x-sql-datatype: BIGINT
      description: >-
        not yet claimed HIVE backed dollars 
        stored in hbd reward balance
    hive_rewards:
      type: integer
      x-sql-datatype: BIGINT
      description: >-
        not yet claimed HIVE 
        stored in hive reward balance
    vests_rewards:
      type: string
      description: >-
        not yet claimed VESTS 
        stored in vest reward balance
    hive_vesting_rewards:
      type: integer
      x-sql-datatype: BIGINT
      description: >-
        the reward vesting balance, denominated in HIVE, 
        is determined by the prevailing HIVE price at the time of reward reception
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_endpoints.rewards CASCADE;
CREATE TYPE btracker_endpoints.rewards AS (
    "hbd_rewards" BIGINT,
    "hive_rewards" BIGINT,
    "vests_rewards" TEXT,
    "hive_vesting_rewards" BIGINT
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

      REST call example
      * `GET ''https://%1$s/balance-api/account-balances/blocktrades/rewards''`
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
          (VEST balances are represented as string due to json limitations)

          * Returns `btracker_endpoints.rewards`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_endpoints.rewards'
            example:
              - hbd_rewards: 0
                hive_rewards: 0
                vests_rewards: "0"
                hive_vesting_rewards: 0
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_rewards;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_rewards(
    "account-name" TEXT
)
RETURNS btracker_endpoints.rewards 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

  RETURN (
    WITH get_account_id AS MATERIALIZED
    (
      SELECT av.id FROM hive.accounts_view av WHERE av.name = "account-name" 
    ),
    get_balances AS MATERIALIZED
    (
      SELECT  
        MAX(CASE WHEN ar.nai = 13 THEN ar.balance END) AS hbd_rewards,
        MAX(CASE WHEN ar.nai = 21 THEN ar.balance END) AS hive_rewards,
        MAX(CASE WHEN ar.nai = 37 THEN ar.balance END) AS vests_rewards,
        MAX(CASE WHEN ar.nai = 38 THEN ar.balance END) AS hive_vesting_rewards
      FROM account_rewards ar
      WHERE ar.account= (SELECT id FROM get_account_id)
    )
    SELECT (
      COALESCE(gb.hbd_rewards,0),
      COALESCE(gb.hive_rewards ,0),
      COALESCE(gb.vests_rewards::TEXT ,'0'),
      COALESCE(gb.hive_vesting_rewards, 0))::btracker_endpoints.rewards
    FROM 
      get_balances gb
  );
END
$$;

RESET ROLE;

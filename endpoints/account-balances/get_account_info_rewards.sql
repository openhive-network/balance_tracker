SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_endpoints.account_info_rewards:
  type: object
  properties:
    curation_rewards:
      type: integer
      x-sql-datatype: BIGINT
      description: rewards obtained by posting and commenting expressed in VEST
    posting_rewards:
      type: integer
      x-sql-datatype: BIGINT
      description: curator''s reward expressed in VEST
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_endpoints.account_info_rewards CASCADE;
CREATE TYPE btracker_endpoints.account_info_rewards AS (
    "curation_rewards" BIGINT,
    "posting_rewards" BIGINT
);
-- openapi-generated-code-end

/** openapi:paths
/account-balances/{account-name}/rewards/info:
  get:
    tags:
      - Account-balances
    summary: Account posting and curation rewards
    description: |
      Returns the sum of the received posting and curation rewards of the account since its creation

      SQL example
      * `SELECT * FROM btracker_endpoints.get_account_info_rewards(''blocktrades'');`

      * `SELECT * FROM btracker_endpoints.get_account_info_rewards(''initminer'');`

      REST call example
      * `GET https://{btracker-host}/%1$s/account-balances/blocktrades/rewards/info`
      
      * `GET https://{btracker-host}/%1$s/account-balances/initminer/rewards/info`
    operationId: btracker_endpoints.get_account_info_rewards
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
          Account posting and curation rewards

          * Returns `btracker_endpoints.account_info_rewards`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_endpoints.account_info_rewards'
            example:
              - curation_rewards: 1000
                posting_rewards: 1000
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_info_rewards;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_info_rewards(
    "account-name" TEXT
)
RETURNS btracker_endpoints.account_info_rewards 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
  _result btracker_endpoints.account_info_rewards;
  _account INT = (SELECT av.id FROM hive.accounts_view av WHERE av.name = "account-name");
BEGIN

PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

SELECT cv.curation_rewards, cv.posting_rewards
INTO _result
FROM account_info_rewards cv
WHERE cv.account = _account;

IF NOT FOUND THEN 
  _result = (0::BIGINT, 0::BIGINT);
END IF;

RETURN _result;
END
$$;

RESET ROLE;

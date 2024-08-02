SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_endpoints.info_rewards:
  type: object
  properties:
    curation_rewards:
      type: string
      description: rewards obtained by posting and commenting expressed in VEST
    posting_rewards:
      type: string
      description: curator''s reward expressed in VEST
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_endpoints.info_rewards CASCADE;
CREATE TYPE btracker_endpoints.info_rewards AS (
    "curation_rewards" TEXT,
    "posting_rewards" TEXT
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

      REST call example
      * `GET ''https://%2$s/%1$s/account-balances/blocktrades/rewards/info''`
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
          (VEST balances are represented as string due to json limitations)

          * Returns `btracker_endpoints.info_rewards`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_endpoints.info_rewards'
            example:
              - curation_rewards: "196115157"
                posting_rewards: "65916519"
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_info_rewards;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_info_rewards(
    "account-name" TEXT
)
RETURNS btracker_endpoints.info_rewards 
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
    )
    SELECT 
      (COALESCE(air.curation_rewards::TEXT, '0'),
      COALESCE(air.posting_rewards::TEXT, '0'))::btracker_endpoints.info_rewards 
    FROM (
      SELECT 
        NULL::TEXT AS curation_rewards,
        NULL::TEXT AS posting_rewards
    ) default_values
    LEFT JOIN get_account_id gai ON true
    LEFT JOIN account_info_rewards air ON air.account = gai.id
  );

END
$$;

RESET ROLE;

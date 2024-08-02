SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_endpoints.delegations:
  type: object
  properties:
    delegated_vests:
      type: string
      description: >-
        VESTS delegated to another user, 
        account''s power is lowered by delegated VESTS
    received_vests:
      type: string
      description: >-
        VESTS received from another user, 
        account''s power is increased by received VESTS
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_endpoints.delegations CASCADE;
CREATE TYPE btracker_endpoints.delegations AS (
    "delegated_vests" TEXT,
    "received_vests" TEXT
);
-- openapi-generated-code-end

/** openapi:paths
/account-balances/{account-name}/delegations:
  get:
    tags:
      - Account-balances
    summary: Account delegations
    description: |
      Lists current account delegated and received VESTs

      SQL example
      * `SELECT * FROM btracker_endpoints.get_account_delegations(''blocktrades'');`

      REST call example
      * `GET ''https://%2$s/%1$s/account-balances/blocktrades/delegations''`
    operationId: btracker_endpoints.get_account_delegations
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
          Account delegations
          (VEST balances are represented as string due to json limitations)

          * Returns `btracker_endpoints.delegations`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_endpoints.delegations'
            example:
              - delegated_vests: "0"
                received_vests: "0"
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_delegations;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_delegations(
    "account-name" TEXT
)
RETURNS btracker_endpoints.delegations 
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
      (COALESCE(ad.delegated_vests::TEXT, '0'),
      COALESCE(ad.received_vests::TEXT, '0'))::btracker_endpoints.delegations 
    FROM (
      SELECT 
        NULL::TEXT AS delegated_vests,
        NULL::TEXT AS received_vests
    ) default_values
    LEFT JOIN get_account_id gai ON true
    LEFT JOIN account_delegations ad ON ad.account = gai.id
);
END
$$;

RESET ROLE;

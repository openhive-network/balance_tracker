SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_endpoints.vests_balance:
  type: object
  properties:
    delegated_vests:
      type: integer
      x-sql-datatype: BIGINT
      description: >-
        VESTS delegated to another user, 
        account's power is lowered by delegated VESTS
    received_vests:
      type: integer
      x-sql-datatype: BIGINT
      description: >-
        VESTS received from another user, 
        account's power is increased by received VESTS
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_endpoints.vests_balance CASCADE;
CREATE TYPE btracker_endpoints.vests_balance AS (
    "delegated_vests" BIGINT,
    "received_vests" BIGINT
);
-- openapi-generated-code-end

/** openapi:paths
/btracker/account-balances/{account-name}/delegations:
  get:
    tags:
      - Account-balances
    summary: Account delegations
    description: |
      Lists current account delegated and received VESTs

      SQL example
      * `SELECT * FROM btracker_endpoints.get_account_delegations('blocktrades');`

      * `SELECT * FROM btracker_endpoints.get_account_delegations('initminer');`

      REST call example
      * `GET https://{btracker-host}/btracker/account-balances/blocktrades/delegations`
      
      * `GET https://{btracker-host}/btracker/account-balances/initminer/delegations`
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

          * Returns `btracker_endpoints.vests_balance`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_endpoints.vests_balance'
            example:
              - delegated_vests: 1000
                received_vests: 1000
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_delegations;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_delegations(
    "account-name" TEXT
)
RETURNS btracker_endpoints.vests_balance 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
  _result btracker_endpoints.vests_balance;
  _account INT = (SELECT av.id FROM hive.accounts_view av WHERE av.name = "account-name");
BEGIN

PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

SELECT cv.delegated_vests, cv.received_vests
INTO _result
FROM account_delegations cv WHERE cv.account = _account;

IF NOT FOUND THEN 
  _result = (0::BIGINT, 0::BIGINT);
END IF;

RETURN _result;
END
$$;

RESET ROLE;

SET ROLE btracker_owner;

/** openapi:paths
/accounts/{account-name}/delegations:
  get:
    tags:
      - Accounts
    summary: Historical balance change
    description: |
      History of change of `coin-type` balance in given block range

      SQL example
      * `SELECT * FROM btracker_endpoints.get_balance_delegations(''blocktrades'');`

      REST call example
      * `GET ''https://%1$s/balance-api/accounts/blocktrades/delegations''`
    operationId: btracker_endpoints.get_balance_delegations
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
          Balance change
        content:
          application/json:
            schema:
              type: string
              x-sql-datatype: JSON
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_balance_delegations;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_balance_delegations(
    "account-name" TEXT
)
RETURNS JSON 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
SET jit = OFF
AS
$$
  _account_id INT = (SELECT av.id FROM hive.accounts_view av WHERE av.name = "account-name");
BEGIN

  RETURN (
    SELECT json_build_object(
      'delegated', (
        SELECT to_json(array_agg(row)) FROM (
        ) row
      ),
      'received', (
        SELECT to_json(array_agg(row)) FROM (
        ) row
      ),
    )
  ); 
END
$$;

RESET ROLE;

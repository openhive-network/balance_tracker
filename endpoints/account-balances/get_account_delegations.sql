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
DECLARE
  _account_id INT = (SELECT av.id FROM hive.accounts_view av WHERE av.name = "account-name");
BEGIN

  RETURN (
    SELECT json_build_object(
      'outgoing_delegations', (
        SELECT to_json(array_agg(row)) FROM (
          SELECT 
            (SELECT av.name FROM hive.accounts_view av WHERE av.id = d.delegatee) AS delegatee,
            d.balance::TEXT AS amount,
            d.source_op::TEXT AS operation_id,
            d.source_op_block AS block_num 
          FROM current_accounts_delegations d
          WHERE delegator = _account_id
        ) row
      ),
      'incoming_delegations', (
        SELECT to_json(array_agg(row)) FROM (
          SELECT 
            (SELECT av.name FROM hive.accounts_view av WHERE av.id = d.delegator) AS delegator,
            d.balance::TEXT AS amount,
            d.source_op::TEXT AS operation_id,
            d.source_op_block AS block_num 
          FROM current_accounts_delegations d
          WHERE delegatee = _account_id
        ) row
      )
    )
  ); 
END
$$;

RESET ROLE;

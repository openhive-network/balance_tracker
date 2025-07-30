SET ROLE btracker_owner;

/** openapi:paths
/accounts/{account-name}/delegations:
  get:
    tags:
      - Accounts
    summary: Account delegations
    description: |
      List of incoming and outgoing delegations

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
          Incoming and outgoing delegations

          * Returns `btracker_backend.delegations`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.delegations'
            example: {
                  "outgoing_delegations": [],
                  "incoming_delegations": []
                }
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_balance_delegations;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_balance_delegations(
    "account-name" TEXT
)
RETURNS btracker_backend.delegations 
-- openapi-generated-code-end
LANGUAGE 'plpgsql' STABLE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  _account_id INT := btracker_backend.get_account_id("account-name", TRUE);
  _incoming_delegations btracker_backend.incoming_delegations[];
  _outgoing_delegations btracker_backend.outgoing_delegations[];
BEGIN
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

  _incoming_delegations := array_agg(row ORDER BY row.amount::BIGINT DESC) FROM (
      SELECT 
        ba.delegator,
        ba.amount,
        ba.operation_id,
        ba.block_num
      FROM btracker_backend.incoming_delegations(_account_id) ba
  ) row;

  _outgoing_delegations := array_agg(row ORDER BY row.amount::BIGINT DESC) FROM (
      SELECT 
        ba.delegatee,
        ba.amount,
        ba.operation_id,
        ba.block_num
      FROM btracker_backend.outgoing_delegations(_account_id) ba
  ) row;

  RETURN (
    COALESCE(_outgoing_delegations, '{}'::btracker_backend.outgoing_delegations[]), 
    COALESCE(_incoming_delegations, '{}'::btracker_backend.incoming_delegations[])
  )::btracker_backend.delegations; 
END
$$;

RESET ROLE;

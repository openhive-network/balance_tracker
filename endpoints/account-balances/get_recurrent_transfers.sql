SET ROLE btracker_owner;

/** openapi:paths
/accounts/{account-name}/recurrent-transfers:
  get:
    tags:
      - Accounts
    summary: Account recurrent transfers
    description: |
      List of incoming and outgoing recurrent transfers

      SQL example
      * `SELECT * FROM btracker_endpoints.get_recurrent_transfers(''blocktrades'');`

      REST call example
      * `GET ''https://%1$s/balance-api/accounts/blocktrades/recurrent-transfers''`
    operationId: btracker_endpoints.get_recurrent_transfers
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
          Incoming and outgoing recurrent transfers

          * Returns `btracker_backend.recurrent_transfers`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.recurrent_transfers'
            example: {
                  "outgoing_recurrent_transfers": [],
                  "incoming_recurrent_transfers": []
                }
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_recurrent_transfers;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_recurrent_transfers(
    "account-name" TEXT
)
RETURNS btracker_backend.recurrent_transfers 
-- openapi-generated-code-end
LANGUAGE 'plpgsql' STABLE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  _account_id INT := btracker_backend.get_account_id("account-name", TRUE);
  _incoming_recurrent_transfers btracker_backend.incoming_recurrent_transfers[];
  _outgoing_recurrent_transfers btracker_backend.outgoing_recurrent_transfers[];
BEGIN
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

  _incoming_recurrent_transfers := array_agg(row ORDER BY row.trigger_date) FROM (
      SELECT 
        ba.from,
        ba.pair_id,
        ba.amount,
        ba.consecutive_failures,
        ba.remaining_executions,
        ba.recurrence,
        ba.memo,
        ba.trigger_date,
        ba.operation_id,
        ba.block_num
      FROM btracker_backend.incoming_recurrent_transfers(_account_id) ba
  ) row;

  _outgoing_recurrent_transfers := array_agg(row ORDER BY row.trigger_date) FROM (
      SELECT 
        ba.to,
        ba.pair_id,
        ba.amount,
        ba.consecutive_failures,
        ba.remaining_executions,
        ba.recurrence,
        ba.memo,
        ba.trigger_date,
        ba.operation_id,
        ba.block_num
      FROM btracker_backend.outgoing_recurrent_transfers(_account_id) ba
  ) row;

  RETURN (
    COALESCE(_outgoing_recurrent_transfers, '{}'::btracker_backend.outgoing_recurrent_transfers[]), 
    COALESCE(_incoming_recurrent_transfers, '{}'::btracker_backend.incoming_recurrent_transfers[])
  )::btracker_backend.recurrent_transfers; 
END
$$;

RESET ROLE;

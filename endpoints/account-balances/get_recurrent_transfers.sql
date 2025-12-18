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
/*
================================================================================
ENDPOINT: get_recurrent_transfers
================================================================================
PURPOSE:
  Returns all active recurrent (scheduled) transfers for an account, split into:
  - Incoming: Transfers scheduled TO this account from others
  - Outgoing: Transfers scheduled FROM this account to others

RECURRENT TRANSFER MECHANICS (HF25+):
  - User creates a recurrent transfer specifying: amount, recurrence (hours),
    and number of executions
  - System automatically executes transfers at the specified interval
  - If sender has insufficient funds, consecutive_failures increments
  - After 10 consecutive failures OR all executions complete, transfer is deleted

ARCHITECTURE:
  1. Resolve account name to account_id
  2. Query recurrent_transfers table via backend helpers
  3. Aggregate results into typed arrays, sorted by next trigger date
  4. Return composite type with both arrays

DATA SOURCE: recurrent_transfers table (via view)

SORTING:
  Results sorted by trigger_date (ascending) - soonest-to-execute first.
  This helps users see which transfers will fire next.

TRIGGER DATE CALCULATION:
  trigger_date = block_created_at + (recurrence_hours * '1 hour')
  This represents when the NEXT execution will occur.

FAILURE HANDLING:
  - consecutive_failures: Count of recent failed attempts (insufficient funds)
  - After 10 consecutive failures, the recurrent transfer is automatically cancelled
  - Successful execution resets consecutive_failures to 0

RETURN TYPE: btracker_backend.recurrent_transfers
  - outgoing_recurrent_transfers[]: Array of transfers FROM this account
  - incoming_recurrent_transfers[]: Array of transfers TO this account

EACH TRANSFER RECORD CONTAINS:
  - from/to: Counterparty account name
  - pair_id: Unique identifier for this sender-receiver pair
  - amount: {nai, amount, precision} object
  - consecutive_failures: Recent failure count
  - remaining_executions: How many transfers left
  - recurrence: Hours between executions
  - memo: Optional transfer memo
  - trigger_date: When next execution occurs
  - operation_id/block_num: Creation transaction reference
================================================================================
*/
DECLARE
  _account_id INT := btracker_backend.get_account_id("account-name", TRUE);
  _incoming_recurrent_transfers btracker_backend.incoming_recurrent_transfers[];
  _outgoing_recurrent_transfers btracker_backend.outgoing_recurrent_transfers[];
BEGIN
  -- Short cache - recurrent transfers execute frequently
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

  ---------------------------------------------------------------------------
  -- INCOMING RECURRENT TRANSFERS
  -- Query: What recurring payments is this account scheduled to RECEIVE?
  -- Sorted by trigger_date ascending (soonest first)
  ---------------------------------------------------------------------------
  _incoming_recurrent_transfers := array_agg(row ORDER BY row.trigger_date)
  FROM (
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

  ---------------------------------------------------------------------------
  -- OUTGOING RECURRENT TRANSFERS
  -- Query: What recurring payments is this account scheduled to SEND?
  -- Sorted by trigger_date ascending (soonest first)
  ---------------------------------------------------------------------------
  _outgoing_recurrent_transfers := array_agg(row ORDER BY row.trigger_date)
  FROM (
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

  ---------------------------------------------------------------------------
  -- RETURN COMPOSITE TYPE
  -- COALESCE ensures empty arrays instead of NULL for consistent JSON
  ---------------------------------------------------------------------------
  RETURN (
    COALESCE(_outgoing_recurrent_transfers, '{}'::btracker_backend.outgoing_recurrent_transfers[]),
    COALESCE(_incoming_recurrent_transfers, '{}'::btracker_backend.incoming_recurrent_transfers[])
  )::btracker_backend.recurrent_transfers;
END
$$;

RESET ROLE;

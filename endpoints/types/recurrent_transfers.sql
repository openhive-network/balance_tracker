SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_backend.amount:
  type: object
  properties:
    nai:
      type: string
    amount:
      type: string
    precision:
      type: integer
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.amount CASCADE;
CREATE TYPE btracker_backend.amount AS (
    "nai" TEXT,
    "amount" TEXT,
    "precision" INT
);
-- openapi-generated-code-end

/** openapi:components:schemas
btracker_backend.incoming_recurrent_transfers:
  type: object
  properties:
    from:
      type: string
      description: Account name of the sender
    pair_id:
      type: integer
      description: ID of the pair of accounts
    amount:
      $ref: '#/components/schemas/btracker_backend.amount'
      description: Amount of the transfer with NAI and precision
    consecutive_failures:
      type: integer
      description: amount of consecutive failures
    remaining_executions:
      type: integer
      description: Remaining executions
    recurrence:
      type: integer
      description: Recurrence in hours
    memo:
      type: string
      description: Memo message
    trigger_date:
      type: string
      format: date-time
      description: Date of the next trigger of the transfer
    operation_id:
      type: string
      description: >-
        Unique operation identifier with
        an encoded block number and operation type id
    block_num:
      type: integer
      description: Block number
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.incoming_recurrent_transfers CASCADE;
CREATE TYPE btracker_backend.incoming_recurrent_transfers AS (
    "from" TEXT,
    "pair_id" INT,
    "amount" btracker_backend.amount,
    "consecutive_failures" INT,
    "remaining_executions" INT,
    "recurrence" INT,
    "memo" TEXT,
    "trigger_date" TIMESTAMP,
    "operation_id" TEXT,
    "block_num" INT
);
-- openapi-generated-code-end

/** openapi:components:schemas
btracker_backend.outgoing_recurrent_transfers:
  type: object
  properties:
    to:
      type: string
      description: Account name of the receiver
    pair_id:
      type: integer
      description: ID of the pair of accounts
    amount:
      $ref: '#/components/schemas/btracker_backend.amount'
      description: Amount of the transfer with NAI and precision
    consecutive_failures:
      type: integer
      description: amount of consecutive failures
    remaining_executions:
      type: integer
      description: Remaining executions
    recurrence:
      type: integer
      description: Recurrence in hours
    memo:
      type: string
      description: Memo message
    trigger_date:
      type: string
      format: date-time
      description: Date of the next trigger of the transfer
    operation_id:
      type: string
      description: >-
        Unique operation identifier with
        an encoded block number and operation type id
    block_num:
      type: integer
      description: Block number
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.outgoing_recurrent_transfers CASCADE;
CREATE TYPE btracker_backend.outgoing_recurrent_transfers AS (
    "to" TEXT,
    "pair_id" INT,
    "amount" btracker_backend.amount,
    "consecutive_failures" INT,
    "remaining_executions" INT,
    "recurrence" INT,
    "memo" TEXT,
    "trigger_date" TIMESTAMP,
    "operation_id" TEXT,
    "block_num" INT
);
-- openapi-generated-code-end


/** openapi:components:schemas
btracker_backend.recurrent_transfers:
  type: object
  properties:
    outgoing_recurrent_transfers:
      type: array
      items:
        $ref: '#/components/schemas/btracker_backend.outgoing_recurrent_transfers'
      description: List of outgoing recurrent transfers from the account
    incoming_recurrent_transfers:
      type: array
      items:
        $ref: '#/components/schemas/btracker_backend.incoming_recurrent_transfers'
      description: List of incoming recurrent transfers to the account
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.recurrent_transfers CASCADE;
CREATE TYPE btracker_backend.recurrent_transfers AS (
    "outgoing_recurrent_transfers" btracker_backend.outgoing_recurrent_transfers[],
    "incoming_recurrent_transfers" btracker_backend.incoming_recurrent_transfers[]
);
-- openapi-generated-code-end


RESET ROLE;

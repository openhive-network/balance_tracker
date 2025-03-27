SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_backend.incoming_delegations:
  type: object
  properties:
    delegator:
      type: string
      description: account name of the delegator
    amount:
      type: string
      description: amount of vests delegated
    operation_id:
      type: string
      description: >-
        unique operation identifier with
        an encoded block number and operation type id
    block_num:
      type: integer
      description: block number
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.incoming_delegations CASCADE;
CREATE TYPE btracker_backend.incoming_delegations AS (
    "delegator" TEXT,
    "amount" TEXT,
    "operation_id" TEXT,
    "block_num" INT
);
-- openapi-generated-code-end

/** openapi:components:schemas
btracker_backend.outgoing_delegations:
  type: object
  properties:
    delegatee:
      type: string
      description: account name of the delegatee
    amount:
      type: string
      description: amount of vests delegated
    operation_id:
      type: string
      description: >-
        unique operation identifier with
        an encoded block number and operation type id
    block_num:
      type: integer
      description: block number
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.outgoing_delegations CASCADE;
CREATE TYPE btracker_backend.outgoing_delegations AS (
    "delegatee" TEXT,
    "amount" TEXT,
    "operation_id" TEXT,
    "block_num" INT
);
-- openapi-generated-code-end


/** openapi:components:schemas
btracker_backend.delegations:
  type: object
  properties:
    outgoing_delegations:
      type: array
      items:
        $ref: '#/components/schemas/btracker_backend.outgoing_delegations'
      description: List of outgoing delegations from the account
    incoming_delegations:
      type: array
      items:
        $ref: '#/components/schemas/btracker_backend.incoming_delegations'
      description: List of incoming delegations to the account
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.delegations CASCADE;
CREATE TYPE btracker_backend.delegations AS (
    "outgoing_delegations" btracker_backend.outgoing_delegations[],
    "incoming_delegations" btracker_backend.incoming_delegations[]
);
-- openapi-generated-code-end


RESET ROLE;

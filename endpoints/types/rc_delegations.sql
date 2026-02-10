SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_backend.incoming_rc_delegations:
  type: object
  properties:
    delegator:
      type: string
      description: account name of the delegator
    max_rc:
      type: string
      description: amount of RC delegated
    operation_id:
      type: string
      description: >-
        unique operation identifier with
        an encoded block number and operation position
    block_num:
      type: integer
      description: block number
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.incoming_rc_delegations CASCADE;
CREATE TYPE btracker_backend.incoming_rc_delegations AS (
    "delegator" TEXT,
    "max_rc" TEXT,
    "operation_id" TEXT,
    "block_num" INT
);
-- openapi-generated-code-end

/** openapi:components:schemas
btracker_backend.outgoing_rc_delegations:
  type: object
  properties:
    delegatee:
      type: string
      description: account name of the delegatee
    max_rc:
      type: string
      description: amount of RC delegated
    operation_id:
      type: string
      description: >-
        unique operation identifier with
        an encoded block number and operation position
    block_num:
      type: integer
      description: block number
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.outgoing_rc_delegations CASCADE;
CREATE TYPE btracker_backend.outgoing_rc_delegations AS (
    "delegatee" TEXT,
    "max_rc" TEXT,
    "operation_id" TEXT,
    "block_num" INT
);
-- openapi-generated-code-end


/** openapi:components:schemas
btracker_backend.rc_delegations:
  type: object
  properties:
    outgoing_delegations:
      type: array
      items:
        $ref: '#/components/schemas/btracker_backend.outgoing_rc_delegations'
      description: List of outgoing RC delegations from the account
    incoming_delegations:
      type: array
      items:
        $ref: '#/components/schemas/btracker_backend.incoming_rc_delegations'
      description: List of incoming RC delegations to the account
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.rc_delegations CASCADE;
CREATE TYPE btracker_backend.rc_delegations AS (
    "outgoing_delegations" btracker_backend.outgoing_rc_delegations[],
    "incoming_delegations" btracker_backend.incoming_rc_delegations[]
);
-- openapi-generated-code-end


RESET ROLE;

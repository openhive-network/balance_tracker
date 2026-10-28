SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_backend.proposal_payment_totals:
  type: object
  properties:
    proposal_id:
      type: integer
      description: id of the DHF proposal
    receiver:
      type: string
      description: account the proposal pays; null when the proposal has not been paid yet
    total_paid:
      $ref: '#/components/schemas/btracker_backend.amount'
      description: total HBD paid to the proposal so far (zero when never paid)
    payment_count:
      type: integer
      description: number of payments (proposal_pay_operation) so far
    first_payment_block:
      type: integer
      description: block of the first payment; null when never paid
    first_payment_time:
      type: string
      format: date-time
      description: timestamp of the first payment; null when never paid
    last_payment_block:
      type: integer
      description: block of the latest payment; null when never paid
    last_payment_time:
      type: string
      format: date-time
      description: timestamp of the latest payment; null when never paid
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.proposal_payment_totals CASCADE;
CREATE TYPE btracker_backend.proposal_payment_totals AS (
    "proposal_id" INT,
    "receiver" TEXT,
    "total_paid" btracker_backend.amount,
    "payment_count" INT,
    "first_payment_block" INT,
    "first_payment_time" TIMESTAMP,
    "last_payment_block" INT,
    "last_payment_time" TIMESTAMP
);
-- openapi-generated-code-end

/** openapi:components:schemas
btracker_backend.array_of_proposal_payment_totals:
  type: array
  items:
    $ref: '#/components/schemas/btracker_backend.proposal_payment_totals'
*/

/** openapi:components:schemas
btracker_backend.proposal_payment:
  type: object
  properties:
    block_num:
      type: integer
      description: block number of the payment
    operation_id:
      type: string
      description: unique operation identifier with encoded block number and position
    amount:
      $ref: '#/components/schemas/btracker_backend.amount'
      description: HBD paid
    timestamp:
      type: string
      format: date-time
      description: block timestamp
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.proposal_payment CASCADE;
CREATE TYPE btracker_backend.proposal_payment AS (
    "block_num" INT,
    "operation_id" TEXT,
    "amount" btracker_backend.amount,
    "timestamp" TIMESTAMP
);
-- openapi-generated-code-end

/** openapi:components:schemas
btracker_backend.proposal_payment_history:
  type: object
  properties:
    total_operations:
      type: integer
      description: Total number of payments to the proposal
    total_pages:
      type: integer
      description: Total number of pages
    operations_result:
      type: array
      items:
        $ref: '#/components/schemas/btracker_backend.proposal_payment'
      description: Payments on the requested page
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.proposal_payment_history CASCADE;
CREATE TYPE btracker_backend.proposal_payment_history AS (
    "total_operations" INT,
    "total_pages" INT,
    "operations_result" btracker_backend.proposal_payment[]
);
-- openapi-generated-code-end

RESET ROLE;

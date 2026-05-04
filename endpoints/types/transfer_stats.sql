SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_backend.transfer_stats:
  type: object
  properties:
    date:
      type: string
      format: date-time
      description: the time transfers were included in the blockchain
    total_transfer_amount:
      $ref: '#/components/schemas/btracker_backend.amount'
      description: sum of transferred tokens in the period (with NAI and precision)
    average_transfer_amount:
      $ref: '#/components/schemas/btracker_backend.amount'
      description: average amount of transferred tokens in the period (with NAI and precision)
    maximum_transfer_amount:
      $ref: '#/components/schemas/btracker_backend.amount'
      description: maximum amount of transferred tokens in the period (with NAI and precision)
    minimum_transfer_amount:
      $ref: '#/components/schemas/btracker_backend.amount'
      description: minimum amount of transferred tokens in the period (with NAI and precision)
    transfer_count:
      type: integer
      description: number of transfers in the period
    last_block_num:
      type: integer
      description: last block number in time range
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.transfer_stats CASCADE;
CREATE TYPE btracker_backend.transfer_stats AS (
    "date" TIMESTAMP,
    "total_transfer_amount" btracker_backend.amount,
    "average_transfer_amount" btracker_backend.amount,
    "maximum_transfer_amount" btracker_backend.amount,
    "minimum_transfer_amount" btracker_backend.amount,
    "transfer_count" INT,
    "last_block_num" INT
);
-- openapi-generated-code-end

/** openapi:components:schemas
btracker_backend.array_of_transfer_stats:
  type: array
  items:
    $ref: '#/components/schemas/btracker_backend.transfer_stats'
*/

RESET ROLE;

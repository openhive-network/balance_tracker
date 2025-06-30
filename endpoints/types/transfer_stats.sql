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
      type: string
      description: sum of a amount of transfered tokens in the period
    average_transfer_amount:
      type: string
      description: average amount of transfered tokens in the period
    maximum_transfer_amount:
      type: string
      description: maximum amount of transfered tokens in the period
    minimum_transfer_amount:
      type: string
      description: minimum amount of transfered tokens in the period
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
    "total_transfer_amount" TEXT,
    "average_transfer_amount" TEXT,
    "maximum_transfer_amount" TEXT,
    "minimum_transfer_amount" TEXT,
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

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
      type: integer
      x-sql-datatype: BIGINT
      description: sum of a amount of transfered tokens in the period
    average_transfer_amount:
      type: integer
      x-sql-datatype: BIGINT
      description: average amount of transfered tokens in the period
    maximum_transfer_amount:
      type: integer
      x-sql-datatype: BIGINT
      description: maximum amount of transfered tokens in the period
    minimum_transfer_amount:
      type: integer
      x-sql-datatype: BIGINT
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
    "total_transfer_amount" BIGINT,
    "average_transfer_amount" BIGINT,
    "maximum_transfer_amount" BIGINT,
    "minimum_transfer_amount" BIGINT,
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

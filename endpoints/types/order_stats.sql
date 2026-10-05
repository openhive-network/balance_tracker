SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_backend.order_stats:
  type: object
  description: |
    Limit-order creation cohort and its outcomes at the indexed block.
    Counts reconcile as created = filled + canceled + open. A partially filled
    order is open until fully filled or canceled; a partially filled order
    subsequently canceled contributes only to canceled.
  properties:
    created:
      type: integer
      format: int64
      x-sql-datatype: BIGINT
      description: Number of successfully created limit orders in the cohort
    filled:
      type: integer
      format: int64
      x-sql-datatype: BIGINT
      description: Number of cohort orders fully filled at the indexed block
    canceled:
      type: integer
      format: int64
      x-sql-datatype: BIGINT
      description: Number of cohort orders canceled at the indexed block, including expiration and account clearing
    open:
      type: integer
      format: int64
      x-sql-datatype: BIGINT
      description: Number of cohort orders still open, including partially filled orders
    fill_rate_pct:
      type: [number, 'null']
      x-sql-datatype: NUMERIC
      description: Fully filled orders divided by created orders, multiplied by 100 and rounded to three decimal places; null for an empty cohort
    indexed_through_block:
      type: integer
      description: Inclusive block number through which lifecycle outcomes have been indexed
    indexed_at:
      type: [string, 'null']
      format: date-time
      description: UTC timestamp of indexed_through_block; null before the first block has been indexed
    coverage_from_block:
      type: integer
      description: First block included in the complete lifecycle history; always 1
*/
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.order_stats CASCADE;
CREATE TYPE btracker_backend.order_stats AS (
    "created" BIGINT,
    "filled" BIGINT,
    "canceled" BIGINT,
    "open" BIGINT,
    "fill_rate_pct" NUMERIC,
    "indexed_through_block" INT,
    "indexed_at" TIMESTAMP,
    "coverage_from_block" INT
);
-- openapi-generated-code-end

RESET ROLE;

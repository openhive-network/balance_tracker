SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_backend.order_stats:
  type: object
  properties:
    created:
      type: integer
      description: limit orders created in the block range
    filled:
      type: integer
      description: of those, orders fully filled, whenever that happened
    cancelled:
      type: integer
      description: |
        of those, orders cancelled by their owner or on expiration, whenever that
        happened, including orders partially filled before
    open:
      type: integer
      description: of those, orders still open
    fill_rate_pct:
      type: number
      description: filled / created in percent, rounded to 2 decimal places; null when no order was created
*/
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.order_stats CASCADE;
CREATE TYPE btracker_backend.order_stats AS (
    "created" INT,
    "filled" INT,
    "cancelled" INT,
    "open" INT,
    "fill_rate_pct" FLOAT
);
-- openapi-generated-code-end

RESET ROLE;

SET ROLE btracker_owner;

/** openapi:paths
/order-stats:
  get:
    tags:
      - Orders
    summary: Global limit-order lifecycle statistics
    description: |
      Counts successful limit-order creations and their latest indexed outcomes.
      Optional UTC dates select orders by creation time, inclusively. Outcomes
      are evaluated at indexed_through_block, even when to-date is historical.
      Reused owner/order IDs represent separate order creations.

      filled counts only fully filled orders. Partial fills remain open until
      the order is fully filled or canceled. canceled includes explicit
      cancellations, expiration, and account clearing. Counts reconcile as
      created = filled + canceled + open.

      A complete index from block 1 through the application head is required.
      While lifecycle history is being backfilled or the index is behind the
      application head, this endpoint returns HTTP 503. Responses have a
      two-second cache because outcomes of historical creation cohorts change.

      SQL example
      * `SELECT * FROM btracker_endpoints.get_order_stats();`

      REST call example
      * `GET ''https://%1$s/balance-api/order-stats?from-date=2020-01-01T00:00:00&to-date=2020-12-31T23:59:59''`
    operationId: btracker_endpoints.get_order_stats
    parameters:
      - in: query
        name: from-date
        required: false
        schema:
          type: string
          format: date-time
          default: null
          nullable: true
        description: Inclusive UTC lower bound on order creation time; omitted or null leaves the lower bound unrestricted
      - in: query
        name: to-date
        required: false
        schema:
          type: string
          format: date-time
          default: null
          nullable: true
        description: Inclusive UTC upper bound on order creation time; outcomes are still evaluated at the indexed block
    responses:
      '200':
        description: Complete global creation cohort and outcomes
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.order_stats'
            example:
              {
                "created": 100,
                "filled": 75,
                "canceled": 20,
                "open": 5,
                "fill_rate_pct": 75.000,
                "indexed_through_block": 5000000,
                "indexed_at": "2016-09-15T19:47:21",
                "coverage_from_block": 1
              }
      '400':
        description: Invalid date or from-date is greater than to-date
      '503':
        description: Complete lifecycle history is not ready or has not reached the application head
*/
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_order_stats;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_order_stats(
    "from-date" TIMESTAMP = NULL,
    "to-date" TIMESTAMP = NULL
)
RETURNS btracker_backend.order_stats
-- openapi-generated-code-end
LANGUAGE plpgsql STABLE
SET jit = OFF
AS
$$
BEGIN
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

  RETURN btracker_backend.get_order_stats(NULL, "from-date", "to-date");
END
$$;

RESET ROLE;

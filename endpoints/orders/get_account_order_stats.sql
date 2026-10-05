SET ROLE btracker_owner;

/** openapi:paths
/accounts/{account}/order-stats:
  get:
    tags:
      - Accounts
      - Orders
    summary: Account limit-order lifecycle statistics
    description: |
      Counts successful limit orders created by the account and their outcomes
      at indexed_through_block. Optional UTC dates select creation times
      inclusively; to-date does not limit when an order is filled or canceled.
      Partially filled orders remain open until fully filled or canceled.
      Counts reconcile as created = filled + canceled + open.

      The account must exist. An existing account with no matching creations
      returns zero counts and a null fill_rate_pct. Complete lifecycle history
      from block 1 through the application head is required; an incomplete
      or lagging lifecycle index returns HTTP 503. Responses are cached for
      two seconds because historical creation cohorts can acquire new outcomes.

      SQL example
      * `SELECT * FROM btracker_endpoints.get_account_order_stats(''alice'');`

      REST call example
      * `GET ''https://%1$s/balance-api/accounts/alice/order-stats''`
    operationId: btracker_endpoints.get_account_order_stats
    parameters:
      - in: path
        name: account
        required: true
        schema:
          type: string
        description: Account that created the orders
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
        description: Complete account creation cohort and outcomes
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.order_stats'
      '400':
        description: Invalid date, inverted date bounds, or account does not exist
      '503':
        description: Complete lifecycle history is not ready or has not reached the application head
*/
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_order_stats;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_order_stats(
    "account" TEXT,
    "from-date" TIMESTAMP = NULL,
    "to-date" TIMESTAMP = NULL
)
RETURNS btracker_backend.order_stats
-- openapi-generated-code-end
LANGUAGE plpgsql STABLE
SET jit = OFF
AS
$$
DECLARE
  _account_id INT := btracker_backend.get_account_id("account", TRUE);
BEGIN
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

  RETURN btracker_backend.get_order_stats(_account_id, "from-date", "to-date");
END
$$;

RESET ROLE;

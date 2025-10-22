SET ROLE btracker_owner;

/** openapi:paths
/top-holders:
  get:
    tags:
      - Accounts
    summary: Top asset holders (with totals)
    description: |
      Lists top holders for a given coin with totals to support pagination.

      SQL example:
      * `SELECT * FROM btracker_endpoints.get_top_holders("HIVE","balance",1,100);`

      REST call example:
      * `GET "https://%1$s/balance-api/top-holders?coin-type=HIVE&balance-type=balance&page=1&page-size=100"
    operationId: btracker_endpoints.get_top_holders

    x-response-headers:
      - name: Cache-Control
        value: public, max-age=2

    parameters:
      - in: query
        name: coin-type
        required: true
        schema:
          $ref: '#/components/schemas/btracker_backend.nai_type'
        description: |
          * HBD  
          * HIVE  
          * VESTS

      - in: query
        name: balance-type
        required: false
        schema:
          $ref: '#/components/schemas/btracker_backend.balance_type'
          default: balance
        description: |
          `balance` or `savings_balance`  
          (`savings_balance` not allowed with `VESTS`).

      - in: query
        name: page
        required: false
        schema:
          type: integer
          minimum: 1
          default: 1
        description: 1-based page number.

      - in: query
        name: page-size
        required: false
        schema:
          type: integer
          minimum: 1
          default: 100
        description: Max results per page (capped by backend validator).

    responses:
      '200':
        description: Ranked holders with totals (typed envelope)
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.top_holders'
      '400':
        description: Unsupported parameter combination
*/
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_top_holders;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_top_holders(
    "coin-type" btracker_backend.nai_type,
    "balance-type" btracker_backend.balance_type = 'balance',
    "page" INT = 1,
    "page-size" INT = 100
)
RETURNS btracker_backend.top_holders 
-- openapi-generated-code-end

LANGUAGE plpgsql
STABLE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
  _coin_type_id INT := btracker_backend.get_nai_type("coin-type");
BEGIN
  -- Validate inputs (reuse existing backend validators)
  PERFORM btracker_backend.validate_negative_page("page");
  PERFORM btracker_backend.validate_negative_limit("page-size");
  PERFORM btracker_backend.validate_limit("page-size", 1000);  -- adjust cap as you prefer
  PERFORM btracker_backend.validate_balance_history("balance-type", "coin-type");

  -- Current balances are volatile → short cache
  PERFORM set_config('response.headers', '[{"Cache-Control":"public, max-age=2"}]', true);

  -- Delegate to unified backend function (returns totals + pages + rows[])
  RETURN btracker_backend.get_top_holders(
    _coin_type_id,
    "balance-type",
    COALESCE("page", 1),
    COALESCE("page-size", 100)
  );
END
$$;

RESET ROLE;

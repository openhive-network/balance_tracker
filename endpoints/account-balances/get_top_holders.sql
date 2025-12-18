SET ROLE btracker_owner;

/** openapi:paths
/top-holders:
  get:
    tags:
      - Accounts
    summary: Top asset holders with total number of account and pages.
    description: |
      Lists top holders for a given coin with Top asset holders with total number of account and pages to support pagination.

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
        description: Ranked holders with totals number of pages and acounts.
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
AS
$$
/*
================================================================================
ENDPOINT: get_top_holders
================================================================================
PURPOSE:
  Returns a paginated leaderboard of top asset holders for a specific coin type.
  Useful for whale watching, distribution analysis, and ecosystem monitoring.

PARAMETERS:
  - coin-type: Asset to rank (HBD, HIVE, or VESTS)
  - balance-type: 'balance' (liquid) or 'savings_balance' (savings accounts)
  - page: 1-based page number
  - page-size: Results per page (max 1000)

ARCHITECTURE:
  1. Validate inputs (page-size cap, VESTS+savings restriction)
  2. Convert coin-type enum to NAI integer
  3. Delegate to backend helper which performs:
     a. COUNT(*) for total accounts with positive balance
     b. Paginated query with OFFSET/LIMIT
     c. ROW_NUMBER() for global ranking

RANKING LOGIC:
  - Accounts sorted by balance DESC, then by name ASC (tiebreaker)
  - Rank is global (not per-page) - page 2 starts at rank 101 if page-size=100
  - Only accounts with balance > 0 are included

DATA SOURCES:
  - current_account_balances (via view) for liquid balances
  - account_savings (via view) for savings balances

PERFORMANCE CONSIDERATIONS:
  - Uses OFFSET/LIMIT pagination - efficient for first few pages
  - COUNT(*) runs separately to get total (required for pagination UI)
  - MATERIALIZED CTE prevents re-execution of sorted query
  - force_custom_plan prevents plan caching issues with varying parameters

VALIDATION:
  - page-size capped at 1000
  - page must be >= 1
  - VESTS + savings_balance is invalid (no such thing as VESTS savings)

USE CASES:
  - "Whale alert" monitoring tools
  - Wealth distribution analysis
  - Governance participation metrics (VESTS = voting power)
  - Exchange cold wallet identification

RETURN TYPE: btracker_backend.top_holders
  - total_accounts: Count of accounts with positive balance
  - total_pages: Calculated from total_accounts / page-size
  - holders_result[]: Array of (rank, account, value)
================================================================================
*/
DECLARE
  _coin_type_id INT := btracker_backend.get_nai_type("coin-type");
BEGIN
  ---------------------------------------------------------------------------
  -- INPUT VALIDATION
  ---------------------------------------------------------------------------
  PERFORM btracker_backend.validate_negative_page("page");
  PERFORM btracker_backend.validate_negative_limit("page-size");
  PERFORM btracker_backend.validate_limit("page-size", 1000);
  -- VESTS cannot have savings_balance (savings only holds HBD/HIVE)
  PERFORM btracker_backend.validate_balance_history("balance-type", "coin-type");

  ---------------------------------------------------------------------------
  -- CACHE HEADER
  -- Short cache (2 seconds) - balances change with every block
  ---------------------------------------------------------------------------
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

  ---------------------------------------------------------------------------
  -- DELEGATE TO BACKEND HELPER
  -- Returns composite type with totals and ranked holder array
  ---------------------------------------------------------------------------
  RETURN btracker_backend.get_top_holders(
    _coin_type_id,
    "balance-type",
    COALESCE("page", 1),
    COALESCE("page-size", 100)
  );
END
$$;

RESET ROLE;

SET ROLE btracker_owner;

/** openapi:paths
/total-value-locked:
  get:
    tags:
      - Other
    summary: Returns Total Value Locked (VESTS + HIVE savings + HBD savings)
    description: |
      Computes the chain totals of VESTS, HIVE savings, and HBD savings at the last-synced block.

      SQL example
      * `SELECT * FROM btracker_endpoints.get_total_value_locked();`

      REST call example
      * `GET "https://%1$s/balance-api/total-value-locked"`
    operationId: btracker_endpoints.get_total_value_locked
    responses:
      '200':
        description: TVL snapshot (block height is the app''s last-synced block)
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.total_value_locked'
            example:
              {
                "block_num": 50000000,
                "total_vests": "448144916705468383",
                "savings_hive": "0",
                "savings_hbd": "0"
              }
      '404':
        description: App last-synced block is undefined (TVL cannot be computed)
*/
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_total_value_locked;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_total_value_locked()
RETURNS btracker_backend.total_value_locked 
-- openapi-generated-code-end
LANGUAGE plpgsql STABLE
SET JIT = OFF
SET join_collapse_limit = 16
SET from_collapse_limit = 16
AS
$$
/*
================================================================================
ENDPOINT: get_total_value_locked
================================================================================
PURPOSE:
  Returns blockchain-wide totals for "locked" value - assets that are staked
  or in savings rather than liquid/tradeable. Useful for DeFi-style metrics.

WHAT IS "LOCKED" VALUE:
  1. VESTS (Vesting Shares / Hive Power):
     - Staked HIVE that provides voting power and curation rewards
     - Has 13-week power-down period to convert back to liquid HIVE
     - Represents long-term commitment to the network

  2. Savings (HIVE and HBD):
     - Funds in savings accounts have 3-day withdrawal delay
     - HBD savings earns interest (currently ~20% APR)
     - Provides security against account compromise

ARCHITECTURE:
  Simple pass-through to backend helper which aggregates:
  - SUM of all VESTS balances from current_account_balances
  - SUM of all HIVE savings from account_savings
  - SUM of all HBD savings from account_savings
  - Current block number from HAF context (snapshot reference)

USE CASES:
  - DeFi dashboards showing TVL metrics
  - Network health monitoring
  - Comparison with other blockchain TVL metrics
  - Staking ratio analysis (VESTS vs liquid HIVE)

CACHING:
  2 second cache - totals change with every block as users stake/unstake

PLANNER HINTS:
  - JIT = OFF: Aggregation query is simple, JIT overhead not worth it
  - collapse_limits = 16: Standard optimization for multi-table queries

RETURN TYPE: btracker_backend.total_value_locked
  - block_num: Head block at time of query
  - total_vests: Global VESTS sum (as TEXT for bigint safety)
  - savings_hive: Global HIVE savings sum (as TEXT)
  - savings_hbd: Global HBD savings sum (as TEXT)
================================================================================
*/
BEGIN
  -- Short cache - totals change frequently with every block
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

  RETURN btracker_backend.get_total_value_locked();
END
$$;

RESET ROLE;

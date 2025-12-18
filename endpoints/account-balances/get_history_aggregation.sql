SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_backend.array_of_aggregated_history:
  type: array
  items:
    $ref: '#/components/schemas/btracker_backend.aggregated_history'
*/

/** openapi:paths
/accounts/{account-name}/aggregated-history:
  get:
    tags:
      - Accounts
    summary: Aggregated account balance history
    description: |
      History of change of `coin-type` balance in given block range with granularity of day/month/year

      SQL example
      * `SELECT * FROM btracker_endpoints.get_balance_aggregation(''blocktrades'', ''VESTS'');`

      REST call example
      * `GET ''https://%1$s/balance-api/accounts/blocktrades/aggregated-history?coin-type=VESTS''`
    operationId: btracker_endpoints.get_balance_aggregation
    parameters:
      - in: path
        name: account-name
        required: true
        schema:
          type: string
        description: Name of the account
      - in: query
        name: coin-type
        required: true
        schema:
          $ref: '#/components/schemas/btracker_backend.nai_type'
        description: |
          Coin types:

          * HBD

          * HIVE

          * VESTS
      - in: query
        name: granularity
        required: false
        schema:
          $ref: '#/components/schemas/btracker_backend.granularity'
          default: yearly
        description: |
          granularity types:

          * daily

          * monthly

          * yearly
      - in: query
        name: direction
        required: false
        schema:
          $ref: '#/components/schemas/btracker_backend.sort_direction'
          default: desc
        description: |
          Sort order:

           * `asc` - Ascending, from oldest to newest

           * `desc` - Descending, from newest to oldest
      - in: query
        name: from-block
        required: false
        schema:
          type: string
          default: NULL
        description: |
          Lower limit of the block range, can be represented either by a block-number (integer) or a timestamp (in the format YYYY-MM-DD HH:MI:SS).

          The provided `timestamp` will be converted to a `block-num` by finding the first block
          where the block''s `created_at` is more than or equal to the given `timestamp` (i.e. `block''s created_at >= timestamp`).

          The function will interpret and convert the input based on its format, example input:

          * `2016-09-15 19:47:21`

          * `5000000`
      - in: query
        name: to-block
        required: false
        schema:
          type: string
          default: NULL
        description: |
          Similar to the from-block parameter, can either be a block-number (integer) or a timestamp (formatted as YYYY-MM-DD HH:MI:SS).

          The provided `timestamp` will be converted to a `block-num` by finding the first block
          where the block''s `created_at` is less than or equal to the given `timestamp` (i.e. `block''s created_at <= timestamp`).

          The function will convert the value depending on its format, example input:

          * `2016-09-15 19:47:21`

          * `5000000`
    responses:
      '200':
        description: |
          Balance change

          * Returns array of `btracker_backend.aggregated_history`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.array_of_aggregated_history'
            example:
              - {
                  "date": "2017-01-01T00:00:00",
                  "balance": {
                    "balance": "8172549681941451",
                    "savings_balance": "0"
                  },
                  "prev_balance": {
                    "balance": "0",
                    "savings_balance": "0"
                  },
                  "min_balance": {
                    "balance": "1000000000000",
                    "savings_balance": "0"
                  },
                  "max_balance": {
                    "balance": "8436182707535769",
                    "savings_balance": "0"
                  }
                }

      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_balance_aggregation;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_balance_aggregation(
    "account-name" TEXT,
    "coin-type" btracker_backend.nai_type,
    "granularity" btracker_backend.granularity = 'yearly',
    "direction" btracker_backend.sort_direction = 'desc',
    "from-block" TEXT = NULL,
    "to-block" TEXT = NULL
)
RETURNS SETOF btracker_backend.aggregated_history 
-- openapi-generated-code-end
LANGUAGE 'plpgsql' STABLE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
/*
================================================================================
ENDPOINT: get_balance_aggregation
================================================================================
PURPOSE:
  Returns aggregated balance history for an account at day/month/year granularity.
  Unlike get_balance_history which shows every operation, this endpoint provides
  period-end snapshots with min/max values for charting and analysis.

PARAMETERS:
  - account-name: Hive account name
  - coin-type: Asset type (HBD, HIVE, or VESTS)
  - granularity: 'daily', 'monthly', or 'yearly' aggregation level
  - direction: 'asc' (oldest first) or 'desc' (newest first)
  - from-block/to-block: Block range filter (accepts block number or timestamp)

ARCHITECTURE:
  1. Convert block/timestamp parameters to block range
  2. Validate inputs and set cache headers
  3. Delegate to backend aggregation function
  4. Return SETOF records (streaming result)

DATA FLOW (backend):
  balance_history_by_day/month tables
    -> generate_series() creates date buckets
    -> LEFT JOIN balance data to date series
    -> RECURSIVE CTE fills gaps with previous period's balance
    -> Return (date, balance, prev_balance, min_balance, max_balance)

AGGREGATION LOGIC:
  - balance: Closing balance at end of period
  - prev_balance: Balance at start of period (end of previous period)
  - min_balance: Lowest balance observed during period
  - max_balance: Highest balance observed during period

GAP FILLING:
  Uses RECURSIVE CTE to carry forward balances for periods with no activity.
  If an account had no transactions in March 2024, the March record shows
  the February closing balance as both opening and closing.

USE CASES:
  - Portfolio tracking charts (daily/monthly balance over time)
  - Historical analysis (yearly summaries)
  - Performance dashboards

CACHING STRATEGY:
  - Fully historical range (to-block irreversible): 1 year cache
  - Includes recent/live data: 2 second cache

RETURN TYPE: SETOF btracker_backend.aggregated_history
  - date: Period end timestamp
  - balance: {balance, savings_balance} at period end
  - prev_balance: {balance, savings_balance} at period start
  - min_balance: {balance, savings_balance} minimum during period
  - max_balance: {balance, savings_balance} maximum during period
================================================================================
*/
DECLARE
  _block_range hive.blocks_range := hive.convert_to_blocks_range("from-block", "to-block");
  _coin_type INT                 := btracker_backend.get_nai_type("coin-type");
  _account_id INT                := btracker_backend.get_account_id("account-name", TRUE);
BEGIN
  ---------------------------------------------------------------------------
  -- INPUT VALIDATION
  ---------------------------------------------------------------------------
  IF _block_range.first_block IS NOT NULL THEN
    PERFORM btracker_backend.validate_negative_limit(_block_range.first_block, 'from-block');
  END IF;

  IF _block_range.last_block IS NOT NULL THEN
    PERFORM btracker_backend.validate_negative_limit(_block_range.last_block, 'to-block');
  END IF;

  ---------------------------------------------------------------------------
  -- CACHE HEADER LOGIC
  -- Immutable historical data -> long cache (1 year)
  -- Live/recent data -> short cache (2 seconds)
  ---------------------------------------------------------------------------
  IF _block_range.last_block <= hive.app_get_irreversible_block() AND _block_range.last_block IS NOT NULL THEN
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=31536000"}]', true);
  ELSE
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);
  END IF;

  ---------------------------------------------------------------------------
  -- RETURN STREAMING RESULTS FROM BACKEND
  ---------------------------------------------------------------------------
  RETURN QUERY
  SELECT
    ah.date,
    ah.balance,
    ah.prev_balance,
    ah.min_balance,
    ah.max_balance
  FROM btracker_backend.get_balance_history_aggregation(
    _account_id,
    _coin_type,
    "granularity",
    "direction",
    _block_range.first_block,
    _block_range.last_block
  ) ah;
END
$$;

RESET ROLE;

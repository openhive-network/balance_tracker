SET ROLE btracker_owner;

/** openapi:paths
/transfer-statistics:
  get:
    tags:
      - Transfers
    summary: Aggregated transfer statistics
    description: |
      History of amount of transfers per hour, day, month or year.

      SQL example
      * `SELECT * FROM btracker_endpoints.get_transfer_statistics(''HBD'');`

      REST call example
      * `GET ''https://%1$s/balance-api/transfer-statistics?coin-type=HBD''`
    operationId: btracker_endpoints.get_transfer_statistics
    parameters:
      - in: query
        name: coin-type
        required: true
        schema:
          $ref: '#/components/schemas/btracker_backend.liquid_nai_type'
        description: |
          Coin types:

          * HBD

          * HIVE
      - in: query
        name: granularity
        required: false
        schema:
          $ref: '#/components/schemas/btracker_backend.granularity_hourly'
          default: yearly
        description: |
          granularity types:

          * hourly

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

          * Returns array of `btracker_backend.transfer_stats`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.array_of_transfer_stats'
            example: [
              {
                "date": "2017-01-01T00:00:00",
                "total_transfer_amount": "69611921266",
                "average_transfer_amount": "1302405",
                "maximum_transfer_amount": "18000000",
                "minimum_transfer_amount": "1",
                "transfer_count": 54665,
                "last_block_num": 5000000
              }
            ]
      '404':
        description: No transfer statistics found for the given parameters
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_transfer_statistics;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_transfer_statistics(
    "coin-type" btracker_backend.liquid_nai_type,
    "granularity" btracker_backend.granularity_hourly = 'yearly',
    "direction" btracker_backend.sort_direction = 'desc',
    "from-block" TEXT = NULL,
    "to-block" TEXT = NULL
)
RETURNS SETOF btracker_backend.transfer_stats 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
SET jit = OFF
AS
$$
/*
================================================================================
ENDPOINT: get_transfer_statistics
================================================================================
PURPOSE:
  Returns aggregated transfer volume statistics for HIVE or HBD at configurable
  time granularity (hourly, daily, monthly, yearly). Used for network activity
  analysis, volume trending, and exchange integration.

PARAMETERS:
  - coin-type: 'HBD' or 'HIVE' (VESTS not supported - not transferable)
  - granularity: 'hourly', 'daily', 'monthly', or 'yearly'
  - direction: 'asc' (oldest first) or 'desc' (newest first)
  - from-block/to-block: Block range filter (accepts block number or timestamp)

WHAT TRANSFERS ARE TRACKED:
  - transfer: Direct account-to-account transfers
  - fill_recurrent_transfer: Executed scheduled transfers
  - escrow_transfer: Funds locked in escrow agreements
  Note: Internal market trades are NOT included (those are orders, not transfers)

AGGREGATION METRICS:
  - total_transfer_amount: Sum of all transfer amounts in period
  - average_transfer_amount: Mean transfer size
  - maximum_transfer_amount: Largest single transfer in period
  - minimum_transfer_amount: Smallest single transfer in period
  - transfer_count: Number of transfer operations
  - last_block_num: Reference block for the period

DATA SOURCES:
  - transfer_stats_by_hour: Hourly pre-aggregated data
  - transfer_stats_by_day: Daily pre-aggregated data
  - transfer_stats_by_month: Monthly pre-aggregated data
  - Yearly data computed on-the-fly from monthly

GAP FILLING:
  Backend uses generate_series() to create continuous date buckets,
  filling gaps with zero values for periods with no transfers.

CACHING STRATEGY:
  - Fully historical range (to-block irreversible): 1 year cache
  - Includes recent/live data: 2 second cache

USE CASES:
  - Network activity dashboards
  - Volume analysis for trading decisions
  - Exchange integration (deposit/withdrawal monitoring)
  - Economic analysis and reporting

RETURN TYPE: SETOF btracker_backend.transfer_stats
  - date: Period end timestamp
  - total_transfer_amount: Sum as TEXT (bigint safety)
  - average_transfer_amount: Mean as TEXT
  - maximum/minimum_transfer_amount: Extremes as TEXT
  - transfer_count: Number of transfers (INT)
  - last_block_num: Block reference for period end
================================================================================
*/
DECLARE
  _block_range hive.blocks_range := hive.convert_to_blocks_range("from-block", "to-block");
  _coin_type INT                 := btracker_backend.get_nai_type("coin-type"::TEXT::btracker_backend.nai_type);
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
  -- All amounts cast to TEXT for JSON bigint safety
  ---------------------------------------------------------------------------
  RETURN QUERY
  SELECT
    ah.updated_at,
    ah.sum_transfer_amount::TEXT,
    ah.avg_transfer_amount::TEXT,
    ah.max_transfer_amount::TEXT,
    ah.min_transfer_amount::TEXT,
    ah.transfer_count,
    ah.last_block_num
  FROM btracker_backend.get_transfer_aggregation(
    _coin_type,
    "granularity",
    "direction",
    _block_range.first_block,
    _block_range.last_block
  ) ah;
END
$$;

/*
--------------------------------------------------------------------------------
OVERLOAD: get_transfer_statistics() with no parameters
--------------------------------------------------------------------------------
PURPOSE:
  Provides a clear error message when required coin-type parameter is missing.
  PostgREST would otherwise return a cryptic "function not found" error.
--------------------------------------------------------------------------------
*/
CREATE OR REPLACE FUNCTION btracker_endpoints.get_transfer_statistics()
RETURNS jsonb
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'Missing required parameter: "coin-type"';
END;
$$;

RESET ROLE;

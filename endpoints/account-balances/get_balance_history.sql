SET ROLE btracker_owner;

/** openapi:paths
/accounts/{account-name}/balance-history:
  get:
    tags:
      - Accounts
    summary: Historical balance change
    description: |
      History of change of `coin-type` balance in given block range

      SQL example
      * `SELECT * FROM btracker_endpoints.get_balance_history(''blocktrades'', ''VESTS'', ''balance'', 1, 2);`

      REST call example
      * `GET ''https://%1$s/balance-api/accounts/blocktrades/balance-history?coin-type=VESTS&page-size=2''`
    operationId: btracker_endpoints.get_balance_history
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
        name: balance-type
        required: false
        schema:
          $ref: '#/components/schemas/btracker_backend.balance_type'
          default: balance
        description: |
          Balance types:

          * balance

          * savings_balance
      - in: query
        name: page
        required: false
        schema:
          type: integer
          default: NULL
        description: |
          Return page on `page` number, default null due to reversed order of pages,
          the first page is the oldest,
          example: first call returns the newest page and total_pages is 100 - the newest page is number 100, next 99 etc.
      - in: query
        name: page-size
        required: false
        schema:
          type: integer
          default: 100
        description: Return max `page-size` operations per page, defaults to `100`
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

          * Returns `btracker_backend.operation_history`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.operation_history'
            example: {
                  "total_operations": 188291,
                  "total_pages": 94146,
                  "operations_result": [
                    {
                      "block_num": 4999992,
                      "operation_id": "21474802120262208",
                      "op_type_id": 64,
                      "balance": "8172549681941451",
                      "prev_balance": "8172546678091286",
                      "balance_change": "3003850165",
                      "timestamp": "2016-09-15T19:46:57"
                    },
                    {
                      "block_num": 4999959,
                      "operation_id": "21474660386343488",
                      "op_type_id": 64,
                      "balance": "8172546678091286",
                      "prev_balance": "8172543674223181",
                      "balance_change": "3003868105",
                      "timestamp": "2016-09-15T19:45:12"
                    }
                  ]
                }

      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_balance_history;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_balance_history(
    "account-name" TEXT,
    "coin-type" btracker_backend.nai_type,
    "balance-type" btracker_backend.balance_type = 'balance',
    "page" INT = NULL,
    "page-size" INT = 100,
    "direction" btracker_backend.sort_direction = 'desc',
    "from-block" TEXT = NULL,
    "to-block" TEXT = NULL
)
RETURNS btracker_backend.operation_history 
-- openapi-generated-code-end
LANGUAGE 'plpgsql' STABLE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
SET plan_cache_mode = 'force_custom_plan'
AS
$$
/*
================================================================================
ENDPOINT: get_balance_history
================================================================================
PURPOSE:
  Returns paginated history of balance changes for a specific account and asset
  type. Each record shows the balance after an operation, the previous balance,
  and the change amount.

PARAMETERS:
  - account-name: Hive account name
  - coin-type: Asset type (HBD, HIVE, or VESTS)
  - balance-type: 'balance' for liquid, 'savings_balance' for savings
  - page: Page number (NULL = newest page, pages numbered from oldest=1)
  - page-size: Results per page (max 1000)
  - direction: 'asc' (oldest first) or 'desc' (newest first)
  - from-block/to-block: Block range filter (accepts block number or timestamp)

ARCHITECTURE:
  1. Convert timestamp strings to block numbers via hive.convert_to_blocks_range()
  2. Validate all input parameters
  3. Set appropriate cache headers based on data finality
  4. Delegate to backend helper for actual query

DATA FLOW:
  account_balance_history OR account_savings_history
    -> balance_history_range() [get sequence numbers in range]
    -> calculate_pages() [pagination math]
    -> liquid_balance_history() OR savings_history() [fetch page]
    -> LAG() window function [compute prev_balance]
    -> operation_history composite return

PAGINATION LOGIC:
  - Uses balance_seq_no (monotonic sequence per account+nai) for efficient paging
  - Page numbers are "reversed": page 1 = oldest, page N = newest
  - When page=NULL and direction='desc', returns the newest page first

CACHING STRATEGY:
  - If to-block is in irreversible range: Cache for 1 year (immutable data)
  - Otherwise: Cache for 2 seconds (live data may change)

VALIDATION:
  - page-size must be 1-1000
  - page must be positive (if specified)
  - VESTS cannot be used with savings_balance (no such thing)
  - Block numbers must be positive

PLANNER HINTS:
  - force_custom_plan: Prevents plan caching issues with varying parameters
  - jit = OFF: Faster for simple queries
  - collapse_limits = 16: Better join optimization

RETURN TYPE: btracker_backend.operation_history
  - total_operations: Total matching records
  - total_pages: Total pages available
  - operations_result[]: Array of balance_history records
================================================================================
*/
DECLARE
  -- Convert from-block/to-block (which may be timestamps) to block numbers
  _block_range hive.blocks_range := hive.convert_to_blocks_range("from-block", "to-block");
  _account_id INT                := btracker_backend.get_account_id("account-name", TRUE);
  _coin_type INT                 := btracker_backend.get_nai_type("coin-type");
BEGIN
  ---------------------------------------------------------------------------
  -- INPUT VALIDATION
  ---------------------------------------------------------------------------
  PERFORM btracker_backend.validate_limit("page-size", 1000);
  PERFORM btracker_backend.validate_negative_limit("page-size");
  PERFORM btracker_backend.validate_negative_page("page");
  -- VESTS + savings_balance is invalid (savings only holds HBD/HIVE)
  PERFORM btracker_backend.validate_balance_history("balance-type", "coin-type");

  IF _block_range.first_block IS NOT NULL THEN
    PERFORM btracker_backend.validate_negative_limit(_block_range.first_block, 'from-block');
  END IF;

  IF _block_range.last_block IS NOT NULL THEN
    PERFORM btracker_backend.validate_negative_limit(_block_range.last_block, 'to-block');
  END IF;

  ---------------------------------------------------------------------------
  -- CACHE HEADER LOGIC
  -- Irreversible blocks are immutable -> long cache (1 year)
  -- Reversible blocks may change -> short cache (2 seconds)
  ---------------------------------------------------------------------------
  IF _block_range.last_block <= hive.app_get_irreversible_block() AND _block_range.last_block IS NOT NULL THEN
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=31536000"}]', true);
  ELSE
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);
  END IF;

  ---------------------------------------------------------------------------
  -- DELEGATE TO BACKEND HELPER
  -- Routes to liquid_balance_history() or savings_history() based on balance-type
  ---------------------------------------------------------------------------
  RETURN btracker_backend.balance_history(
    _account_id,
    _coin_type,
    "balance-type",
    "page",
    "page-size",
    "direction",
    _block_range.first_block,
    _block_range.last_block
  );
END
$$;

RESET ROLE;

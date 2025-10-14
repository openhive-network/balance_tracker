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
      * `SELECT * FROM btracker_endpoints.get_balance_history(''blocktrades'', 37, 1 ,2);`

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
SET plan_cache_mode='force_custom_plan'
AS
$$
DECLARE
  _block_range hive.blocks_range := hive.convert_to_blocks_range("from-block","to-block");
  _account_id INT                := btracker_backend.get_account_id("account-name", TRUE);
  _coin_type INT                 := btracker_backend.get_nai_type("coin-type");
BEGIN
  PERFORM btracker_backend.validate_limit("page-size", 1000);
  PERFORM btracker_backend.validate_negative_limit("page-size");
  PERFORM btracker_backend.validate_negative_page("page");
  PERFORM btracker_backend.validate_balance_history("balance-type", "coin-type");
  IF _block_range.first_block IS NOT NULL THEN
    PERFORM btracker_backend.validate_negative_limit(_block_range.first_block, 'from-block');
  END IF;

  IF _block_range.last_block IS NOT NULL THEN
    PERFORM btracker_backend.validate_negative_limit(_block_range.last_block, 'to-block');
  END IF;

  IF _block_range.last_block <= hive.app_get_irreversible_block() AND _block_range.last_block IS NOT NULL THEN
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=31536000"}]', true);
  ELSE
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);
  END IF;
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

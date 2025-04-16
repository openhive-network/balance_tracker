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
      * `GET ''https://%1$s/balance-api/accounts/blocktrades/balance-history?coin-type=VEST&page-size=2''`
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
        required: true
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
          default: 1
        description: |
          Return page on `page` number, defaults to `1`
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
    "page" INT = 1,
    "page-size" INT = 100,
    "direction" btracker_backend.sort_direction = 'desc',
    "from-block" TEXT = NULL,
    "to-block" TEXT = NULL
)
RETURNS btracker_backend.operation_history 
-- openapi-generated-code-end
LANGUAGE 'plpgsql' STABLE
SET from_collapse_limit = 1
SET join_collapse_limit = 1
SET jit = OFF
AS
$$
DECLARE
  _block_range hive.blocks_range := hive.convert_to_blocks_range("from-block","to-block");
  _account_id INT = (SELECT av.id FROM hive.accounts_view av WHERE av.name = "account-name");
  _coin_type INT := (CASE WHEN "coin-type" = 'HBD' THEN 13 WHEN "coin-type" = 'HIVE' THEN 21 ELSE 37 END);
  _result btracker_backend.balance_history[];

  _ops_count INT;
  _from_op BIGINT;
  _to_op BIGINT;
  __total_pages INT;
BEGIN
  PERFORM btracker_backend.validate_limit("page-size", 1000);
  PERFORM btracker_backend.validate_negative_limit("page-size");
  PERFORM btracker_backend.validate_negative_page("page");

  IF _account_id IS NULL THEN
    PERFORM btracker_backend.rest_raise_missing_account("account-name");
  END IF;

  IF "balance-type" = 'savings_balance' AND _coin_type = 37 THEN
    PERFORM btracker_backend.rest_raise_vest_saving_balance("balance-type", "coin-type");
  END IF;

  IF _block_range.last_block <= hive.app_get_irreversible_block() AND _block_range.last_block IS NOT NULL THEN
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=31536000"}]', true);
  ELSE
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);
  END IF;

  SELECT count, from_op, to_op 
  INTO _ops_count, _from_op, _to_op
  FROM btracker_backend.balance_history_range(
    _account_id,
    _coin_type,
    "balance-type",
    _block_range.first_block, 
    _block_range.last_block
  );

  __total_pages := (
    CASE 
      WHEN (_ops_count % "page-size") = 0 THEN 
        _ops_count/"page-size" 
      ELSE 
        (_ops_count/"page-size") + 1
    END
  );

  PERFORM btracker_backend.validate_page("page", __total_pages);

    _result := CASE 
      WHEN "balance-type" = 'balance' THEN
        (
          SELECT array_agg(row ORDER BY
            (CASE WHEN "direction" = 'desc' THEN row.operation_id::BIGINT ELSE NULL END) DESC,
            (CASE WHEN "direction" = 'asc' THEN row.operation_id::BIGINT ELSE NULL END) ASC
          ) FROM (
            SELECT 
              ba.block_num,
              ba.operation_id,
              ba.op_type_id,
              ba.balance,
              ba.prev_balance,
              ba.balance_change,
              ba.timestamp
            FROM btracker_backend.balance_history(
              _account_id,
              _coin_type,
              "page",
              "page-size",
              "direction",
              _from_op,
              _to_op
            ) ba
          ) row
        )
      ELSE
        (
          SELECT array_agg(row ORDER BY
            (CASE WHEN "direction" = 'desc' THEN row.operation_id::BIGINT ELSE NULL END) DESC,
            (CASE WHEN "direction" = 'asc' THEN row.operation_id::BIGINT ELSE NULL END) ASC
          ) FROM (
            SELECT 
              ba.block_num,
              ba.operation_id,
              ba.op_type_id,
              ba.balance,
              ba.prev_balance,
              ba.balance_change,
              ba.timestamp
            FROM btracker_backend.savings_history(
              _account_id,
              _coin_type,
              "page",
              "page-size",
              "direction",
              _from_op,
              _to_op
            ) ba
          ) row
        )
      END;

  RETURN (
    COALESCE(_ops_count, 0),
    COALESCE(__total_pages, 0),
    COALESCE(_result, '{}'::btracker_backend.balance_history[])
  )::btracker_backend.operation_history;

END
$$;

RESET ROLE;

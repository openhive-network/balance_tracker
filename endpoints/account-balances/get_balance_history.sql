SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_endpoints.balance_history CASCADE;
CREATE TYPE btracker_endpoints.balance_history AS (
    "block_num" INT,
    "operation_id" TEXT,
    "op_type_id" INT,
    "balance" TEXT,
    "prev_balance" TEXT,
    "balance_change" TEXT,
    "timestamp" TIMESTAMP
);

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
          $ref: '#/components/schemas/btracker_endpoints.nai_type'
        description: |
          Coin types:

          * HBD

          * HIVE

          * VESTS
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
          $ref: '#/components/schemas/btracker_endpoints.sort_direction'
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
        content:
          application/json:
            schema:
              type: string
              x-sql-datatype: JSON
            example: 
              - {
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
    "coin-type" btracker_endpoints.nai_type,
    "page" INT = 1,
    "page-size" INT = 100,
    "direction" btracker_endpoints.sort_direction = 'desc',
    "from-block" TEXT = NULL,
    "to-block" TEXT = NULL
)
RETURNS JSON 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
SET from_collapse_limit = 1
SET join_collapse_limit = 1
SET jit = OFF
AS
$$
DECLARE
  _block_range hive.blocks_range := hive.convert_to_blocks_range("from-block","to-block");
  _account_id INT = (SELECT av.id FROM hive.accounts_view av WHERE av.name = "account-name");
  _offset INT := ((("page" - 1) * "page-size") + 1);
  _is_desc_direction BOOLEAN := ("direction" = 'desc');
  _coin_type INT;
  _to_block_filter BOOLEAN := FALSE;
  _from_block_filter BOOLEAN := FALSE;
  _from_op BIGINT;
  _to_op BIGINT;
  _ops_count INT;
  _calculate_total_pages INT;
BEGIN
  PERFORM validate_limit("page-size", 1000);
  PERFORM validate_negative_limit("page-size");
  PERFORM validate_negative_page("page");

  IF _block_range.last_block <= hive.app_get_irreversible_block() AND _block_range.last_block IS NOT NULL THEN
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=31536000"}]', true);
  ELSE
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);
  END IF;

  IF _block_range.last_block IS NULL THEN
    _to_block_filter := TRUE;
  ELSE
    SELECT ov.id INTO _to_op
		FROM hive.operations_view ov
		WHERE ov.block_num <= _block_range.last_block
		ORDER BY ov.block_num DESC, ov.id DESC
		LIMIT 1;
  END IF;

  IF _block_range.first_block IS NULL THEN
    _from_block_filter := TRUE;
  ELSE
    SELECT ov.id INTO _from_op
		FROM hive.operations_view ov
		WHERE ov.block_num >= _block_range.first_block
		ORDER BY ov.block_num, ov.id
		LIMIT 1;
  END IF;

  _coin_type := (CASE WHEN "coin-type" = 'HBD' THEN 13 WHEN "coin-type" = 'HIVE' THEN 21 ELSE 37 END);

  --count for given parameters 
  SELECT COUNT(*) INTO _ops_count
  FROM account_balance_history 
  WHERE 
    account = _account_id AND nai = _coin_type AND
    (_to_block_filter OR source_op < _to_op) AND
    (_from_block_filter OR source_op >= _from_op);

  --amount of pages
  SELECT 
    (
      CASE WHEN (_ops_count % "page-size") = 0 THEN 
        _ops_count/"page-size" 
      ELSE ((_ops_count/"page-size") + 1) 
      END
    )::INT INTO _calculate_total_pages;

  PERFORM validate_page("page", _calculate_total_pages);

  RETURN (
    SELECT json_build_object(
      'total_operations', _ops_count,
      'total_pages', _calculate_total_pages,
      'operations_result', (
        SELECT to_json(array_agg(row)) FROM (
          WITH ranked_rows AS
          (
          ---------paging----------
            SELECT 
              source_op_block,
              source_op, 
              ROW_NUMBER() OVER ( ORDER BY
                (CASE WHEN _is_desc_direction THEN source_op ELSE NULL END) DESC,
                (CASE WHEN NOT _is_desc_direction THEN source_op ELSE NULL END) ASC) as row_num
            FROM account_balance_history
            WHERE 
              account = _account_id AND nai = _coin_type AND
              (_to_block_filter OR source_op < _to_op) AND
              (_from_block_filter OR source_op >= _from_op) 
          ),
          filter_params AS MATERIALIZED
          (
            SELECT 
              source_op AS filter_op
            FROM ranked_rows
            WHERE row_num = _offset
          ),
          --------------------------
          --history with extra row--
          gather_page AS MATERIALIZED
          (
            SELECT 
              ab.account,
              ab.nai,
              ab.balance,
              ab.source_op,
              ab.source_op_block,
              ROW_NUMBER() OVER (ORDER BY
                (CASE WHEN _is_desc_direction THEN ab.source_op ELSE NULL END) DESC,
                (CASE WHEN NOT _is_desc_direction THEN ab.source_op ELSE NULL END) ASC) as row_num
            FROM
              account_balance_history ab, filter_params fp
            WHERE 
              ab.account = _account_id AND 
              ab.nai = _coin_type AND
              (NOT _is_desc_direction OR ab.source_op <= fp.filter_op) AND
              (_is_desc_direction OR ab.source_op >= fp.filter_op) AND
              (_to_block_filter OR ab.source_op < _to_op) AND
              (_from_block_filter OR ab.source_op >= _from_op)
            LIMIT "page-size" + 1
          ),
          --------------------------
          --calculate prev balance--
          join_prev_balance AS MATERIALIZED
          (
            SELECT 
              ab.source_op_block,
              ab.source_op,
              ab.balance,
              jab.balance AS prev_balance
            FROM gather_page ab
            LEFT JOIN gather_page jab ON 
              (CASE WHEN _is_desc_direction THEN jab.row_num - 1 ELSE jab.row_num + 1 END) = ab.row_num
            LIMIT "page-size" 
          ),
          add_prevbal_to_row_over_range AS
          (
            SELECT 
              jpb.source_op_block,
              jpb.source_op,
              jpb.balance,
              COALESCE(
                (
                  CASE WHEN jpb.prev_balance IS NULL THEN
                    (
                      SELECT abh.balance FROM account_balance_history abh
                      WHERE 
                        abh.source_op < jpb.source_op AND
                        abh.account = _account_id AND 
                        abh.nai = _coin_type 
                      ORDER BY abh.source_op DESC
                      LIMIT 1
                    )
                  ELSE
                    jpb.prev_balance
                  END
                ), 0
              ) AS prev_balance
            FROM join_prev_balance jpb
          )
          --------------------------
          SELECT 
            (ab.source_op_block,
            ab.source_op::TEXT,
            ov.op_type_id,
            ab.balance::TEXT,
            ab.prev_balance::TEXT,
            (ab.balance - ab.prev_balance)::TEXT,
            bv.created_at)::btracker_endpoints.balance_history
          FROM add_prevbal_to_row_over_range ab
          JOIN hive.blocks_view bv ON bv.num = ab.source_op_block
          JOIN hive.operations_view ov ON ov.id = ab.source_op
      ) row)
    )
  ); 
END
$$;

RESET ROLE;

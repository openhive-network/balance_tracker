SET ROLE btracker_owner;

/** openapi:paths
/accounts/{account-name}/aggregated-history:
  get:
    tags:
      - Accounts
    summary: Aggregated account balance history
    description: |
      History of change of `coin-type` balance in given block range

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
          $ref: '#/components/schemas/btracker_endpoints.nai_type'
        description: |
          Coin types:

          * HBD

          * HIVE

          * VESTS
      - in: query
        name: granularity
        required: false
        schema:
          $ref: '#/components/schemas/btracker_endpoints.granularity'
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
                  "date":"2016-12-31T23:59:59",
                  "prev_balance":"0",
                  "balance":"8172549681941451",
                  "min_balance":"1000000000000",
                  "max_balance":"8436182707535769"
                }
            
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_balance_aggregation;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_balance_aggregation(
    "account-name" TEXT,
    "coin-type" btracker_endpoints.nai_type,
    "granularity" btracker_endpoints.granularity = 'yearly',
    "direction" btracker_endpoints.sort_direction = 'desc',
    "from-block" TEXT = NULL,
    "to-block" TEXT = NULL
)
RETURNS JSON 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
SET jit = OFF
AS
$$
DECLARE
  _block_range hive.blocks_range := hive.convert_to_blocks_range("from-block","to-block");
  _coin_type INT := (CASE WHEN "coin-type" = 'HBD' THEN 13 WHEN "coin-type" = 'HIVE' THEN 21 ELSE 37 END);
  _account_id INT = (SELECT av.id FROM hive.accounts_view av WHERE av.name = "account-name");
BEGIN

  IF _block_range.last_block <= hive.app_get_irreversible_block() AND _block_range.last_block IS NOT NULL THEN
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=31536000"}]', true);
  ELSE
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);
  END IF;

  RETURN COALESCE((
    SELECT to_json(array_agg(row)) FROM (
      SELECT 
        fb.date,
        fb.prev_balance::TEXT,
        fb.balance::TEXT,
        fb.min_balance::TEXT,
        fb.max_balance::TEXT
      FROM get_balance_history_aggregation(
        _account_id,
        _coin_type,
        "granularity",
        "direction",
        _block_range.first_block,
        _block_range.last_block
      ) fb
    ) row
  ), '[]'); 
END
$$;

RESET ROLE;

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
DECLARE
  _block_range hive.blocks_range := hive.convert_to_blocks_range("from-block","to-block");
  _coin_type INT                 := btracker_backend.get_liquid_nai_type("coin-type");
BEGIN
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

CREATE OR REPLACE FUNCTION btracker_endpoints.get_transfer_statistics()
RETURNS jsonb
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'Missing required parameter: "coin-type"';
END;
$$;

RESET ROLE;

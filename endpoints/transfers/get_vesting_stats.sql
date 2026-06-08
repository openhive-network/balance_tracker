SET ROLE btracker_owner;

/** openapi:paths
/vesting-stats:
  get:
    tags:
      - Transfers
    summary: Aggregated power-up / power-down statistics
    description: |
      History of vesting (power-up / power-down) activity per day, month or year.

      Tracks three counts and amounts:

      * `power_up`: HIVE moved into VESTS via `transfer_to_vesting`

      * `power_down_init`: VESTS scheduled for withdrawal via `withdraw_vesting`
        (cancellations excluded)

      * `power_down_fill`: VESTS / HIVE realised by `fill_vesting_withdraw`
        (weekly tranches; HIVE excludes routed-to-VESTS portions)

      SQL example
      * `SELECT * FROM btracker_endpoints.get_vesting_stats(''daily'');`

      REST call example
      * `GET ''https://%1$s/balance-api/vesting-stats?granularity=monthly''`
    operationId: btracker_endpoints.get_vesting_stats
    parameters:
      - in: query
        name: granularity
        required: false
        schema:
          $ref: '#/components/schemas/btracker_backend.granularity'
          default: daily
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
      - in: query
        name: to-block
        required: false
        schema:
          type: string
          default: NULL
        description: |
          Upper limit of the block range, accepts either a block-number (integer) or a timestamp (YYYY-MM-DD HH:MI:SS).
    responses:
      '200':
        description: |
          Aggregated vesting statistics

          * Returns array of `btracker_backend.vesting_stats`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.array_of_vesting_stats'
            example: [
              {
                "date": "2016-09-16T00:00:00",
                "power_up_count": 12,
                "power_up_hive": {"nai": "@@000000021", "amount": "1500000", "precision": 3},
                "power_down_init_count": 4,
                "power_down_init_vests": {"nai": "@@000000037", "amount": "9876543210000", "precision": 6},
                "power_down_fill_count": 80,
                "power_down_fill_vests": {"nai": "@@000000037", "amount": "1234567890", "precision": 6},
                "power_down_fill_hive": {"nai": "@@000000021", "amount": "412345", "precision": 3},
                "last_block_num": 4999990
              }
            ]
      '404':
        description: No vesting statistics found for the given parameters
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_vesting_stats;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_vesting_stats(
    "granularity" btracker_backend.granularity = 'daily',
    "direction"   btracker_backend.sort_direction = 'desc',
    "from-block"  TEXT = NULL,
    "to-block"    TEXT = NULL
)
RETURNS SETOF btracker_backend.vesting_stats
-- openapi-generated-code-end
LANGUAGE 'plpgsql' STABLE
SET jit = OFF
AS
$$
DECLARE
  _block_range hive.blocks_range := hive.convert_to_blocks_range("from-block", "to-block");
  _nai_hive    INT := btracker_backend.nai_hive();
  _nai_vests   INT := btracker_backend.nai_vests();
BEGIN
  IF _block_range.first_block IS NOT NULL THEN
    PERFORM btracker_backend.validate_negative_limit(_block_range.first_block, 'from-block');
  END IF;
  IF _block_range.last_block IS NOT NULL THEN
    PERFORM btracker_backend.validate_negative_limit(_block_range.last_block, 'to-block');
  END IF;

  -- Long cache for fully-irreversible ranges, short cache for live data.
  IF _block_range.last_block <= hive.app_get_irreversible_block() AND _block_range.last_block IS NOT NULL THEN
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=31536000"}]', true);
  ELSE
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);
  END IF;

  RETURN QUERY
  SELECT
    a.updated_at,
    a.power_up_count,
    btracker_backend.create_amount_object(_nai_hive,  a.power_up_hive),
    a.power_down_init_count,
    btracker_backend.create_amount_object(_nai_vests, a.power_down_init_vests),
    a.power_down_fill_count,
    btracker_backend.create_amount_object(_nai_vests, a.power_down_fill_vests),
    btracker_backend.create_amount_object(_nai_hive,  a.power_down_fill_hive),
    a.power_down_route_received_count,
    btracker_backend.create_amount_object(_nai_hive,  a.power_down_route_received_hive),
    btracker_backend.create_amount_object(_nai_vests, a.power_down_route_received_vests),
    a.last_block_num
  FROM btracker_backend.get_vesting_aggregation(
    "granularity", "direction", _block_range.first_block, _block_range.last_block
  ) a;
END
$$;

RESET ROLE;

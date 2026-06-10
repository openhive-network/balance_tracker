SET ROLE btracker_owner;

/** openapi:paths
/accounts/{account-name}/vesting-stats:
  get:
    tags:
      - Accounts
    summary: Per-account aggregated power-up / power-down statistics
    description: |
      Vesting (power-up / power-down) activity for one account, aggregated
      per day, month or year. Same response shape as `/vesting-stats`,
      computed on the fly from the account''s operation stream.

      SQL example
      * `SELECT * FROM btracker_endpoints.get_account_vesting_stats(''blocktrades'');`

      REST call example
      * `GET ''https://%1$s/balance-api/accounts/blocktrades/vesting-stats?granularity=monthly''`
    operationId: btracker_endpoints.get_account_vesting_stats
    parameters:
      - in: path
        name: account-name
        required: true
        schema:
          type: string
        description: Name of the account
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
        description: Lower limit of the block range (block-number or YYYY-MM-DD HH:MI:SS).
      - in: query
        name: to-block
        required: false
        schema:
          type: string
          default: NULL
        description: Upper limit of the block range (block-number or YYYY-MM-DD HH:MI:SS).
    responses:
      '200':
        description: |
          Per-account aggregated vesting statistics.

          * Returns array of `btracker_backend.vesting_stats`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.array_of_vesting_stats'
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_vesting_stats;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_vesting_stats(
    "account-name" TEXT,
    "granularity" btracker_backend.granularity = 'daily',
    "direction" btracker_backend.sort_direction = 'desc',
    "from-block" TEXT = NULL,
    "to-block" TEXT = NULL
)
RETURNS SETOF btracker_backend.vesting_stats 
-- openapi-generated-code-end
LANGUAGE 'plpgsql' STABLE
SET jit = OFF
AS
$$
DECLARE
  _block_range hive.blocks_range := hive.convert_to_blocks_range("from-block", "to-block");
  _account_id  INT               := btracker_backend.get_account_id("account-name", TRUE);
  _nai_hive    INT               := btracker_backend.nai_hive();
  _nai_vests   INT               := btracker_backend.nai_vests();
BEGIN
  IF _block_range.first_block IS NOT NULL THEN
    PERFORM btracker_backend.validate_negative_limit(_block_range.first_block, 'from-block');
  END IF;
  IF _block_range.last_block IS NOT NULL THEN
    PERFORM btracker_backend.validate_negative_limit(_block_range.last_block, 'to-block');
  END IF;

  PERFORM btracker_backend.set_history_cache_headers(_block_range.last_block);

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
    a.last_block_num
  FROM btracker_backend.get_account_vesting_aggregation(
    _account_id, "granularity", "direction", _block_range.first_block, _block_range.last_block
  ) a;
END
$$;

RESET ROLE;

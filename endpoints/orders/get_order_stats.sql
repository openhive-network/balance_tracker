SET ROLE btracker_owner;

/** openapi:paths
/order-stats:
  get:
    tags:
      - Orders
    summary: Lifecycle of all limit orders
    description: |
      How the internal-market limit orders created in a block range, by every
      account, ended: fully filled, cancelled (by the owner or on expiration,
      including orders partially filled first) or still open, with the fill rate.
      Outcomes count whenever they happened, also after the range, so
      `created` = `filled` + `cancelled` + `open`.

      Order history is written by block processing only: on an install upgraded
      from a version without it, orders created before the upgrade are counted
      only after a resync.

      SQL example
      * `SELECT * FROM btracker_endpoints.get_order_stats(''4000000'', ''4500000'');`

      REST call example
      * `GET ''https://%1$s/balance-api/order-stats?from-block=4000000&to-block=4500000''`

      * `GET ''https://%1$s/balance-api/order-stats?from-block=2016-09-01 00:00:00&to-block=2016-09-30 23:59:59''`
    operationId: btracker_endpoints.get_order_stats
    parameters:
      - in: query
        name: from-block
        required: false
        schema:
          type: string
          default: NULL
        description: |
          First block of the range in which the orders were created, either a block-number
          or a date (YYYY-MM-DD HH:MI:SS). Without it, from the first block.
      - in: query
        name: to-block
        required: false
        schema:
          type: string
          default: NULL
        description: |
          Last block of the range in which the orders were created, either a block-number
          or a date (YYYY-MM-DD HH:MI:SS). Without it, up to the head block.
    responses:
      '200':
        description: |
          Order lifecycle counts

          * Returns `btracker_backend.order_stats`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.order_stats'
            example: {
              "created": 39382,
              "filled": 8280,
              "cancelled": 31071,
              "open": 31,
              "fill_rate_pct": 21.02
            }
      '400':
        description: A block or date that is not valid, or from-block after to-block
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_order_stats;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_order_stats(
    "from-block" TEXT = NULL,
    "to-block" TEXT = NULL
)
RETURNS btracker_backend.order_stats 
-- openapi-generated-code-end
LANGUAGE 'plpgsql' STABLE
SET jit = OFF
AS
$$
DECLARE
  _block_range hive.blocks_range := hive.convert_to_blocks_range("from-block", "to-block");
BEGIN
  IF _block_range.first_block IS NOT NULL THEN
    PERFORM btracker_backend.validate_negative_limit(_block_range.first_block, 'from-block');
  END IF;
  IF _block_range.last_block IS NOT NULL THEN
    PERFORM btracker_backend.validate_negative_limit(_block_range.last_block, 'to-block');
  END IF;

  -- Orders of a past range can still fill or be cancelled.
  PERFORM btracker_backend.set_history_cache_headers(NULL);

  RETURN btracker_backend.get_order_stats(NULL, _block_range.first_block, _block_range.last_block);
END
$$;

RESET ROLE;

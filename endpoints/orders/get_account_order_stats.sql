SET ROLE btracker_owner;

/** openapi:paths
/accounts/{account-name}/order-stats:
  get:
    tags:
      - Orders
    summary: Lifecycle of the limit orders of one account
    description: |
      How the internal-market limit orders the account created in a block range
      ended: fully filled, cancelled (by the owner or on expiration, including
      orders partially filled first) or still open, with the fill rate. Outcomes
      count whenever they happened, also after the range, so
      `created` = `filled` + `cancelled` + `open`.

      Order history is written by block processing only: on an install upgraded
      from a version without it, orders created before the upgrade are counted
      only after a resync.

      SQL example
      * `SELECT * FROM btracker_endpoints.get_account_order_stats(''enki'');`

      * `SELECT * FROM btracker_endpoints.get_account_order_stats(''enki'', ''4000000'', ''4500000'');`

      REST call example
      * `GET ''https://%1$s/balance-api/accounts/enki/order-stats''`

      * `GET ''https://%1$s/balance-api/accounts/enki/order-stats?from-block=4000000&to-block=4500000''`
    operationId: btracker_endpoints.get_account_order_stats
    parameters:
      - in: path
        name: account-name
        required: true
        schema:
          type: string
        description: Name of the account
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
              "created": 11486,
              "filled": 11210,
              "cancelled": 267,
              "open": 9,
              "fill_rate_pct": 97.6
            }
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_order_stats;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_order_stats(
    "account-name" TEXT,
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
  _account_id  INT               := btracker_backend.get_account_id("account-name", TRUE);
BEGIN
  IF _block_range.first_block IS NOT NULL THEN
    PERFORM btracker_backend.validate_negative_limit(_block_range.first_block, 'from-block');
  END IF;
  IF _block_range.last_block IS NOT NULL THEN
    PERFORM btracker_backend.validate_negative_limit(_block_range.last_block, 'to-block');
  END IF;

  -- Orders of a past range can still fill or be cancelled.
  PERFORM btracker_backend.set_history_cache_headers(NULL);

  RETURN btracker_backend.get_order_stats(_account_id, _block_range.first_block, _block_range.last_block);
END
$$;

RESET ROLE;

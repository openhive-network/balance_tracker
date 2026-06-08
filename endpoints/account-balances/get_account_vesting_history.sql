SET ROLE btracker_owner;

/** openapi:paths
/accounts/{account-name}/vesting-history:
  get:
    tags:
      - Accounts
    summary: Per-account power-up / power-down event history
    description: |
      Paginated list of vesting events for an account. Four event kinds are
      surfaced via the `direction` field on each row:

      * `power_up` — `transfer_to_vesting`

      * `power_down_init` — `withdraw_vesting` (cancellations excluded)

      * `power_down_fill` — `fill_vesting_withdraw`, the account's OWN power-down

      * `power_down_route_received` — `fill_vesting_withdraw` routed to this account from
        another account's power-down (to_account<>from_account). Shares op_type_id 56 with
        `power_down_fill`; the recipient gains the deposited asset (HIVE, or VESTS for auto_vest).

      The `filter` query parameter narrows the result to one kind, or `all`.

      SQL example
      * `SELECT * FROM btracker_endpoints.get_account_vesting_history(''blocktrades'');`

      REST call example
      * `GET ''https://%1$s/balance-api/accounts/blocktrades/vesting-history?filter=power_down_fill&page-size=50''`
    operationId: btracker_endpoints.get_account_vesting_history
    parameters:
      - in: path
        name: account-name
        required: true
        schema:
          type: string
        description: Name of the account
      - in: query
        name: filter
        required: false
        schema:
          $ref: '#/components/schemas/btracker_backend.vesting_filter'
          default: all
        description: |
          Restrict to one event kind (or `all`):

          * all

          * power_up

          * power_down_init

          * power_down_fill

          * power_down_route_received
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
        description: Items per page (max 1000)
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
          Paginated vesting event history.

          * Returns `btracker_backend.vesting_history`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.vesting_history'
            example: {
              "total_operations": 47,
              "total_pages": 1,
              "operations_result": [
                {
                  "block_num": 4999990,
                  "operation_id": "21474797825294908",
                  "op_type_id": 64,
                  "direction": "power_down_fill",
                  "amount_hive": {"nai": "@@000000021", "amount": "412345", "precision": 3},
                  "amount_vests": {"nai": "@@000000037", "amount": "1234567890", "precision": 6},
                  "timestamp": "2016-09-15T19:46:57"
                }
              ]
            }
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_vesting_history;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_vesting_history(
    "account-name" TEXT,
    "filter"       btracker_backend.vesting_filter = 'all',
    "page"         INT = NULL,
    "page-size"    INT = 100,
    "direction"    btracker_backend.sort_direction = 'desc',
    "from-block"   TEXT = NULL,
    "to-block"     TEXT = NULL
)
RETURNS btracker_backend.vesting_history
-- openapi-generated-code-end
LANGUAGE 'plpgsql' STABLE
SET jit = OFF
AS
$$
DECLARE
  _block_range hive.blocks_range := hive.convert_to_blocks_range("from-block", "to-block");
  _account_id  INT               := btracker_backend.get_account_id("account-name", TRUE);
BEGIN
  PERFORM btracker_backend.validate_limit("page-size", 1000);
  PERFORM btracker_backend.validate_negative_limit("page-size");
  PERFORM btracker_backend.validate_negative_page("page");

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

  RETURN btracker_backend.get_account_vesting_history(
    _account_id,
    "filter",
    "page",
    "page-size",
    "direction",
    _block_range.first_block,
    _block_range.last_block
  );
END
$$;

RESET ROLE;

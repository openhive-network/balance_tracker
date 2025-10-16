SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_backend.array_of_ranked_holder:
  type: array
  items:
    $ref: '#/components/schemas/btracker_backend.ranked_holder'
*/

/** openapi:paths
/top-holders:
  get:
    tags:
      - Accounts
    summary: Top asset holders (with totals)
    description: |
      Lists top holders for a given coin with totals to support pagination.
      Default 100 results per page.

      SQL example:
      * `SELECT * FROM btracker_endpoints.get_top_holders(''HIVE'',''balance'',1,100);`

      REST call example:
      * `GET ''https://%1$s/balance-api/top-holders?coin-type=HIVE&balance-type=balance&page=1&page-size=100''`

    operationId: btracker_endpoints.get_top_holders

    x-response-headers:
      - name: Cache-Control
        value: public, max-age=2

    parameters:
      - in: query
        name: coin-type
        required: true
        schema:
          $ref: '#/components/schemas/btracker_backend.nai_type'
        description: |
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
          `balance` or `savings_balance`  
          (`savings_balance` not allowed with `VESTS`).

      - in: query
        name: page
        required: false
        schema:
          type: integer
          minimum: 1
          default: 1
        description: Page number (1-based).

      - in: query
        name: page-size
        required: false
        schema:
          type: integer
          minimum: 1
          default: 100
        description: Results per page (default `100`, max `1000`).

    responses:
      '200':
        description: Ranked holders with totals (pagination envelope)
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.top_holders'
            example:
              total_accounts: 123456
              total_pages: 1235
              holders_result:
                - rank: 1
                  account: "alice"
                  value: "22691157033541080"
                - rank: 2
                  account: "bob"
                  value: "21900123456789000"
      '400':
        description: Unsupported parameter combination
*/
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_top_holders;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_top_holders(
    "coin-type"    btracker_backend.nai_type,
    "balance-type" btracker_backend.balance_type = 'balance',
    "page"         INT = 1,
    "page-size"    INT = 100
)
RETURNS btracker_backend.top_holders
-- openapi-generated-code-end

LANGUAGE plpgsql
STABLE
SET jit = OFF
AS $$
DECLARE
  _page_num     INT  := COALESCE("page", 1);
  _page_size    INT  := COALESCE("page-size", 100);
  _coin_type_id INT  := btracker_backend.get_nai_type("coin-type");

  _total BIGINT := 0;
  _calc  btracker_backend.calculate_pages_return;
  _rows  btracker_backend.ranked_holder[];
BEGIN
  -- Validate first - then we can cache the response
  PERFORM btracker_backend.validate_negative_page(_page_num);
  PERFORM btracker_backend.validate_negative_limit(_page_size);
  PERFORM btracker_backend.validate_limit(_page_size, 1000);
  PERFORM btracker_backend.validate_balance_history("balance-type", "coin-type");

  -- Cache header (2s)
  PERFORM set_config('response.headers','[{"Cache-Control":"public, max-age=2"}]', true);

  -- Totals (inline, mirrors pager sources)
  IF "balance-type" = 'balance' THEN
    SELECT COUNT(*)::bigint
      INTO _total
    FROM btracker_backend.current_account_balances_view src
    WHERE src.nai = _coin_type_id AND src.balance > 0;
  ELSE
    SELECT COUNT(*)::bigint
      INTO _total
    FROM btracker_backend.account_savings_view src
    WHERE src.nai = _coin_type_id AND src.balance > 0;
  END IF;

  -- Page math: use 'asc' so page=1 => ranks 1..N
  _calc := btracker_backend.calculate_pages(
             COALESCE(_total,0)::int,
             _page_num,
             'asc',
             _page_size
           );

  -- Page slice via existing backend pager; cast value -> TEXT; pack into array
  SELECT COALESCE(
           array_agg(
             (ROW(r.rank, r.account, r.value::text))::btracker_backend.ranked_holder
             ORDER BY r.rank
           ),
           ARRAY[]::btracker_backend.ranked_holder[]
         )
    INTO _rows
  FROM btracker_backend.get_top_holders(
         _coin_type_id,
         "balance-type",
         _calc.page_num,
         _calc.limit_filter
       ) AS r;

  -- Return a typed envelope (same style as your other envelope type)
  RETURN (
    COALESCE(_total, 0)::int,
    COALESCE(_calc.total_pages, 0)::int,
    _rows
  )::btracker_backend.top_holders;
END;
$$;

RESET ROLE;

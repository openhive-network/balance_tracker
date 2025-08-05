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
    summary: Top 100 asset holders
    description: |
      Lists the top 100 accounts holding a given coin, 100 results per page.

      SQL example:
      * `SELECT * FROM btracker_endpoints.get_top_holders(''HIVE'',''balance'',1);`

      REST call example:
      * `GET ''https://%1$s/balance-api/top-holders?coin-type=HIVE&balance-type=balance&page=1''`

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
        description: 100 results per page (default `1`).

    responses:
      '200':
        description: Ranked list of holders
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.array_of_ranked_holder'
      '400':
        description: Unsupported parameter combination
*/
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_top_holders;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_top_holders(
    "coin-type" btracker_backend.nai_type,
    "balance-type" btracker_backend.balance_type = 'balance',
    "page" INT = 1
)
RETURNS SETOF btracker_backend.ranked_holder 
-- openapi-generated-code-end

LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  _page_num INT  := COALESCE("page", 1);
  _coin_type INT := btracker_backend.get_nai_type("coin-type");
BEGIN
  -- Validate first - then we can cache the response
  PERFORM btracker_backend.validate_negative_page(_page_num);
  PERFORM btracker_backend.validate_balance_history("balance-type", "coin-type");

  -- Cache header (2s)
  PERFORM set_config(
    'response.headers',
    '[{"Cache-Control":"public, max-age=2"}]',
    true
  );

  -- Return exactly the composite type fields
  RETURN QUERY
    SELECT
      r.rank,
      r.account,
      r.value::TEXT
    FROM btracker_backend.get_top_holders(
           _coin_type,
           "balance-type",
           _page_num
         ) AS r;
END;
$$;

RESET ROLE;

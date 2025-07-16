SET ROLE btracker_owner;

/** openapi:paths
/top-holders:
  get:
    tags:
      - Accounts
    summary: Top 100 asset holders
    description: |
      Lists the top 100 accounts holding a given asset (HIVE, HBD, VESTS, HIVESAVING, HBDSAVING), paged 100 results per page.

      SQL example
      * `SELECT * FROM btracker_endpoints.get_top_holders(''HIVE'', 1);`

      REST call example
      * `GET ''https://%1$s/balance-api/top-holders?kind=HIVE&page_num=1''`
    operationId: btracker_endpoints.get_top_holders
    parameters:
      - in: query
        name: kind
        required: true
        schema:
          type: string
          enum: [HIVE, HBD, VESTS, HIVESAVING, HBDSAVING]
        description: Asset kind
      - in: query
        name: page_num
        schema:
          type: integer
          minimum: 1
          default: 1
        description: Page number (100 results per page)
    responses:
      '200':
        description: Ranked list of holders
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.get_top_holders'
            example:
              - rank: 1
                account: steemit
                value: 4778859891
      '400':
        description: Unsupported kind parameter
*/
-- openapi-generated-code-begin
CREATE OR REPLACE FUNCTION btracker_endpoints.get_top_holders(
    kind      TEXT,
    page_num  INT   DEFAULT 1
)
RETURNS TABLE(
    rank  INT,
    account  TEXT,
    value NUMERIC
)
LANGUAGE plpgsql
STABLE
AS
$$
BEGIN
  -- 0) page_num must be ≥ 1
  PERFORM btracker_backend.validate_negative_page(page_num);

  -- 1) validate 'kind' is one of the supported values
  IF UPPER(kind) NOT IN ('HIVE','HBD','VESTS','HIVESAVING','HBDSAVING') THEN
    RAISE EXCEPTION 'Unsupported kind parameter: %, must be one of HIVE, HBD, VESTS, HIVESAVING, HBDSAVING', kind;
  END IF;

  -- 2) delegate to your existing backend function
  RETURN QUERY
    SELECT *
      FROM btracker_backend.get_top_holders(kind, page_num);
END;
$$;

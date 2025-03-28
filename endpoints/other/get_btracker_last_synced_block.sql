SET ROLE btracker_owner;

DO $$
DECLARE 
  __schema_name VARCHAR;
BEGIN
SHOW SEARCH_PATH INTO __schema_name;
EXECUTE format( 
$BODY$

/** openapi:paths
/last-synced-block:
  get:
    tags:
      - Other
    summary: Get last block number synced by balance tracker
    description: |
      Get the block number of the last block synced by balance tracker.

      SQL example
      * `SELECT * FROM btracker_endpoints.get_btracker_last_synced_block();`
      
      REST call example
      * `GET ''https://%1$s/balance-api/last-synced-block''`
    operationId: btracker_endpoints.get_btracker_last_synced_block
    responses:
      '200':
        description: |
          Last synced block by balance tracker
          
          * Returns `INT`
        content:
          application/json:
            schema:
              type: integer
            example: 5000000
      '404':
        description: No blocks synced
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_btracker_last_synced_block;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_btracker_last_synced_block()
RETURNS INT 
-- openapi-generated-code-end
LANGUAGE 'plpgsql' STABLE
AS
$pb$
BEGIN
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=0"}]', true);
  RETURN current_block_num FROM hafd.contexts WHERE name = '%s';
END
$pb$;

$BODY$,
__schema_name, __schema_name);
END
$$;

RESET ROLE;

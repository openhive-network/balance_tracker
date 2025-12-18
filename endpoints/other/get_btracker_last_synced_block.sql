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
/*
================================================================================
ENDPOINT: get_btracker_last_synced_block
================================================================================
PURPOSE:
  Returns the highest block number that Balance Tracker has processed.
  Critical for sync status monitoring and data freshness verification.

DATA SOURCE:
  hafd.contexts table - HAF application framework tracks sync progress
  The context name is dynamically inserted during function creation.

SYNC STATUS INTERPRETATION:
  - Compare with hive.app_get_irreversible_block() to check if fully synced
  - Lag = irreversible_block - last_synced_block
  - Lag > 0 means app is still catching up

CACHING:
  No cache (max-age=0) because:
  - Block number changes with every processed block
  - Used for sync status monitoring (needs real-time accuracy)
  - Health checks need current values

USE CASES:
  - Monitoring dashboards (is the app synced?)
  - API client freshness checks
  - Load balancer health endpoints
  - Data consistency verification

DYNAMIC SQL:
  Uses DO block with format() to inject schema name at creation time.
  This allows the function to find the correct HAF context regardless
  of the schema name used during installation.

RETURN: INT containing the last processed block number
================================================================================
*/
BEGIN
  -- No cache - sync status needs real-time accuracy
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=0"}]', true);

  RETURN current_block_num FROM hafd.contexts WHERE name = '%s';
END
$pb$;

$BODY$,
__schema_name, __schema_name);
END
$$;

RESET ROLE;

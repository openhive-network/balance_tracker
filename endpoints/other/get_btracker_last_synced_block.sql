SET ROLE btracker_owner;

/** openapi:paths
/last-synced-block:
  get:
    tags:
      - Other
    summary: Get last block number synced by balance tracker (deprecated)
    deprecated: true
    description: |
      **Deprecated** — superseded by `/sync-status`, which returns the block
      number together with its timestamp (enabling single-call staleness
      checks). This endpoint remains for backward compatibility.

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
$$
/*
================================================================================
ENDPOINT: get_btracker_last_synced_block
================================================================================
PURPOSE:
  Returns the highest block number that Balance Tracker has processed.
  Critical for sync status monitoring and data freshness verification.

DATA SOURCE:
  btracker_backend.last_synced_block() — reads current_block_num from the HAF
  context (the single place that knows the app's context/schema name).

SYNC STATUS INTERPRETATION:
  - Compare with hive.app_get_irreversible_block() to check if fully synced
  - Lag = irreversible_block - last_synced_block
  - Lag > 0 means app is still catching up

CACHING:
  No cache (max-age=0) because the block number changes with every processed
  block and this is used for real-time sync-status / health monitoring.

RETURN: INT containing the last processed block number
================================================================================
*/
BEGIN
  -- No cache - sync status needs real-time accuracy
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=0"}]', true);

  RETURN btracker_backend.last_synced_block();
END
$$;

RESET ROLE;

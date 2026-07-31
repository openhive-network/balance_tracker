SET ROLE btracker_owner;

/** openapi:paths
/sync-status:
  get:
    tags:
      - Other
    summary: Get balance tracker''s sync status
    description: |
      Get the last block processed by balance tracker as an object containing
      both the block number and its timestamp (UTC). This is the uniform
      HAF-app sync/health endpoint: the timestamp lets a consumer compute
      staleness with a single call (`age = now() - last_block_time`) without
      needing a separate head-block reference. Supersedes the deprecated
      `/last-synced-block`.

      SQL example
      * `SELECT * FROM btracker_endpoints.get_btracker_sync_status();`

      REST call example
      * `GET ''https://%1$s/balance-api/sync-status''`
    operationId: btracker_endpoints.get_btracker_sync_status
    responses:
      '200':
        description: |
          Last block processed by balance tracker and its timestamp.
          `last_block_time` is null if no block has been processed yet.
      While the HAF instance is still in massive sync (indexes not yet
      built) the call fails fast with an error rather than executing an
      unindexed lookup.

          * Returns `JSON`
        content:
          application/json:
            schema:
              type: object
              properties:
                last_block_num:
                  type: integer
                  description: highest block number processed by the app
                last_block_time:
                  type: string
                  format: date-time
                  description: UTC timestamp of that block
            example:
              last_block_num: 5000000
              last_block_time: '2016-09-15T19:47:21'
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_btracker_sync_status;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_btracker_sync_status()
RETURNS JSON
-- openapi-generated-code-end
LANGUAGE 'plpgsql' STABLE
AS
$$
/*
================================================================================
ENDPOINT: get_btracker_sync_status
================================================================================
PURPOSE:
  Returns the app's last processed block number together with that block's
  timestamp, so monitors and health checks can judge freshness in one call.
  This is the HAF-wide uniform sync-status shape (see also the deprecated
  /last-synced-block, which returns only the bare block number).

DATA SOURCE:
  btracker_backend.sync_status() — reads current_block_num from the HAF
  context and joins hafd.blocks for the block's created_at.

CACHING:
  No cache (max-age=0): used for real-time sync-status / health monitoring.

RETURN: JSON {"last_block_num": INT, "last_block_time": TEXT|null}
================================================================================
*/
BEGIN
  -- No cache - sync status needs real-time accuracy
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=0"}]', true);

  RETURN btracker_backend.sync_status();
END
$$;

RESET ROLE;

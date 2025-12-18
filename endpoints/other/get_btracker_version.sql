SET ROLE btracker_owner;

/** openapi:paths
/version:
  get:
    tags:
      - Other
    summary: Get Balance tracker''s version
    description: |
      Get Balance tracker''s last commit hash (versions set by by hash value).

      SQL example
      * `SELECT * FROM btracker_endpoints.get_btracker_version();`

      REST call example
      * `GET ''https://%1$s/balance-api/version''`
    operationId: btracker_endpoints.get_btracker_version
    responses:
      '200':
        description: |
          Balance tracker version

          * Returns `TEXT`
        content:
          application/json:
            schema:
              type: string
            example: c2fed8958584511ef1a66dab3dbac8c40f3518f0
      '404':
        description: App not installed
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_btracker_version;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_btracker_version()
RETURNS TEXT 
-- openapi-generated-code-end
LANGUAGE 'plpgsql' STABLE
AS
$$
/*
================================================================================
ENDPOINT: get_btracker_version
================================================================================
PURPOSE:
  Returns the Git commit hash of the deployed Balance Tracker version.
  Used for debugging, deployment verification, and API compatibility checks.

DATA SOURCE:
  version table - Contains git_hash column populated during installation

VERSION FORMAT:
  40-character Git SHA-1 hash (e.g., "c2fed8958584511ef1a66dab3dbac8c40f3518f0")

CACHING:
  Long cache (100,000 seconds / ~27 hours) because:
  - Version only changes on deployment
  - Safe to cache aggressively
  - Reduces database load for health-check endpoints

USE CASES:
  - Deployment verification (did the new version deploy?)
  - API client compatibility checks
  - Debug logging (which version is running?)
  - Monitoring dashboards

RETURN: TEXT containing the Git commit hash
================================================================================
*/
BEGIN
  -- Long cache - version only changes on deployment
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=100000"}]', true);

  RETURN git_hash FROM version;
END
$$;

RESET ROLE;

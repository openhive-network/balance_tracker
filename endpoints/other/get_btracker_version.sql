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
BEGIN
--100000s because version of hafbe doesn't change as often, but it may change
PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=100000"}]', true);

RETURN git_hash FROM version;

END
$$;

RESET ROLE;

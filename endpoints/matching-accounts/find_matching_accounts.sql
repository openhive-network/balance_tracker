SET ROLE btracker_owner;

/** openapi:paths
/matching-accounts/{partial-account-name}:
  get:
    tags:
      - Matching-accounts
    summary: Similar accounts
    description: |
      Returns list of similarly named accounts 

      SQL example
      * `SELECT * FROM btracker_endpoints.find_matching_accounts(''blocktrade'');`

      * `SELECT * FROM btracker_endpoints.find_matching_accounts(''initmi'');`

      REST call example
      * `GET https://{btracker-host}/%1$s/matching-accounts/blocktrade`
      
      * `GET https://{btracker-host}/%1$s/matching-accounts/initmi`
    operationId: btracker_endpoints.find_matching_accounts
    parameters:
      - in: path
        name: partial-account-name
        required: true
        schema:
          type: string
        description: Name of the account
    responses:
      '200':
        description: |
          List of accounts
        content:
          application/json:
            schema:
              type: string
            example: ["blocktrade", "blocktrades"]
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.find_matching_accounts;
CREATE OR REPLACE FUNCTION btracker_endpoints.find_matching_accounts(
    "partial-account-name" TEXT
)
RETURNS TEXT 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
AS
$$
DECLARE 
  __partial_account_name VARCHAR = LOWER("partial-account-name" || '%');
BEGIN

  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

  RETURN json_agg(account_query.accounts
    ORDER BY
      account_query.name_lengths,
      account_query.accounts)
  FROM (
    SELECT
      ha.name AS accounts,
      LENGTH(ha.name) AS name_lengths
    FROM
      hive.accounts_view ha
    WHERE
      ha.name LIKE __partial_account_name
    ORDER BY
      accounts,
      name_lengths
    LIMIT 50
  ) account_query;
END
$$;

RESET ROLE;

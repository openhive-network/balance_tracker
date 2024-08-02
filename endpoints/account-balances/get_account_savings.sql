SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_endpoints.savings:
  type: object
  properties:
    hbd_savings:
      type: integer
      x-sql-datatype: BIGINT
      description: saving balance of HIVE backed dollars
    hive_savings:
      type: integer
      x-sql-datatype: BIGINT
      description: HIVE saving balance
    savings_withdraw_requests:
      type: integer
      description: >-
        number representing how many payouts are pending 
        from user''s saving balance
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_endpoints.savings CASCADE;
CREATE TYPE btracker_endpoints.savings AS (
    "hbd_savings" BIGINT,
    "hive_savings" BIGINT,
    "savings_withdraw_requests" INT
);
-- openapi-generated-code-end

/** openapi:paths
/account-balances/{account-name}/savings:
  get:
    tags:
      - Account-balances
    summary: Account saving balances
    description: |
      Returns current hbd and hive savings

      SQL example
      * `SELECT * FROM btracker_endpoints.get_account_savings(''blocktrades'');`

      REST call example
      * `GET ''https://%2$s/%1$s/account-balances/blocktrades/savings''`
    operationId: btracker_endpoints.get_account_savings
    parameters:
      - in: path
        name: account-name
        required: true
        schema:
          type: string
        description: Name of the account
    responses:
      '200':
        description: |
          Account saving balance
          (VEST balances are represented as string due to json limitations)

          * Returns `btracker_endpoints.savings`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_endpoints.savings'
            example:
              - hbd_savings: 0
                hive_savings: 0
                savings_withdraw_requests: 0
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_savings;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_savings(
    "account-name" TEXT
)
RETURNS btracker_endpoints.savings 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

  RETURN (
    WITH get_account_id AS MATERIALIZED
    (
      SELECT av.id FROM hive.accounts_view av WHERE av.name = "account-name" 
    ),
    get_balances AS MATERIALIZED
    (
      SELECT  
        MAX(CASE WHEN ats.nai = 13 THEN ats.saving_balance END) AS hbd_savings,
        MAX(CASE WHEN ats.nai = 21 THEN ats.saving_balance END) AS hive_savings
      FROM account_savings ats
      WHERE ats.account= (SELECT id FROM get_account_id)
    ),
    get_withdraw_requests AS
    (
      SELECT SUM(ats.savings_withdraw_requests) AS total
      FROM account_savings ats
      WHERE ats.account= (SELECT id FROM get_account_id)
    )
    SELECT (
      COALESCE(gb.hbd_savings,0),
      COALESCE(gb.hive_savings ,0),
      COALESCE(gwr.total ,0))::btracker_endpoints.savings
    FROM 
      get_balances gb,
      get_withdraw_requests gwr
  );
END
$$;

RESET ROLE;

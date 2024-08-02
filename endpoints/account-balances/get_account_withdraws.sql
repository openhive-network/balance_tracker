SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_endpoints.withdrawals:
  type: object
  properties:
    vesting_withdraw_rate:
      type: string
      description: >-
        received until the withdrawal is complete, 
        with each installment amounting to 1/13 of the withdrawn total
    to_withdraw:
      type: string
      description: the remaining total VESTS needed to complete withdrawals
    withdrawn:
      type: string
      description: the total VESTS already withdrawn from active withdrawals
    withdraw_routes:
      type: integer
      description: list of account receiving the part of a withdrawal
    delayed_vests:
      type: string
      description: blocked VESTS by a withdrawal
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_endpoints.withdrawals CASCADE;
CREATE TYPE btracker_endpoints.withdrawals AS (
    "vesting_withdraw_rate" TEXT,
    "to_withdraw" TEXT,
    "withdrawn" TEXT,
    "withdraw_routes" INT,
    "delayed_vests" TEXT
);
-- openapi-generated-code-end

/** openapi:paths
/account-balances/{account-name}/withdrawals:
  get:
    tags:
      - Account-balances
    summary: Account withdrawals
    description: |
      Returns current vest withdrawals, amount withdrawn, amount left to withdraw and 
      rate of a withdrawal 

      SQL example
      * `SELECT * FROM btracker_endpoints.get_account_withdraws(''blocktrades'');`

      REST call example
      * `GET ''https://%2$s/%1$s/account-balances/blocktrades/withdrawals''`
    operationId: btracker_endpoints.get_account_withdraws
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
          Account withdrawals
          (VEST balances are represented as string due to json limitations)

          * Returns `btracker_endpoints.withdrawals`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_endpoints.withdrawals'
            example:
              - vesting_withdraw_rate: "80404818220529"
                to_withdraw: "8362101094935031"
                withdrawn: "804048182205290"
                withdraw_routes: 4
                delayed_vests: "0"
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_withdraws;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_withdraws(
    "account-name" TEXT
)
RETURNS btracker_endpoints.withdrawals 
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
    )
    SELECT 
      (COALESCE(aw.vesting_withdraw_rate::TEXT, '0'),
      COALESCE(aw.to_withdraw::TEXT, '0'),
      COALESCE(aw.withdrawn::TEXT, '0'),
      COALESCE(aw.withdraw_routes, 0),
      COALESCE(aw.delayed_vests::TEXT, '0'))::btracker_endpoints.withdrawals 
    FROM (
      SELECT 
        NULL::TEXT AS vesting_withdraw_rate,
        NULL::TEXT AS to_withdraw,
        NULL::TEXT AS withdrawn,
        NULL::INT AS withdraw_routes,
        NULL::TEXT AS delayed_vests
    ) default_values
    LEFT JOIN get_account_id gai ON true
    LEFT JOIN account_withdraws aw ON aw.account = gai.id
  );
END
$$;

RESET ROLE;

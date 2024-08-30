SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_endpoints.balances:
  type: object
  properties:
    hbd_balance:
      type: integer
      x-sql-datatype: BIGINT
      description: number of HIVE backed dollars the account has
    hive_balance:
      type: integer
      x-sql-datatype: BIGINT
      description: account''s HIVE balance
    vesting_shares:
      type: string
      description: account''s VEST balance
    vesting_balance_hive:
      type: integer
      x-sql-datatype: BIGINT
      description: >- 
        the VEST balance, presented in HIVE, 
        is calculated based on the current HIVE price
    post_voting_power_vests:
      type: string
      description: >-
        account''s VEST balance - delegated VESTs + reveived VESTs,
        presented in HIVE,calculated based on the current HIVE price
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_endpoints.balances CASCADE;
CREATE TYPE btracker_endpoints.balances AS (
    "hbd_balance" BIGINT,
    "hive_balance" BIGINT,
    "vesting_shares" TEXT,
    "vesting_balance_hive" BIGINT,
    "post_voting_power_vests" TEXT
);
-- openapi-generated-code-end

/** openapi:paths
/account-balances/{account-name}:
  get:
    tags:
      - Account-balances
    summary: Account balances
    description: |
      Lists account hbd, hive and vest balances

      SQL example
      * `SELECT * FROM btracker_endpoints.get_account_balances(''blocktrades'');`

      REST call example
      * `GET ''https://%1$s/balance-api/account-balances/blocktrades''`
    operationId: btracker_endpoints.get_account_balances
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
          Account balances 
          (VEST balances are represented as string due to json limitations)

          * Returns `btracker_endpoints.balances`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_endpoints.balances'
            example:
              - hbd_balance: 77246982
                hive_balance: 29594875
                vesting_shares: "8172549681941451"
                vesting_balance_hive: 2720696229
                post_voting_power_vests: "8172549681941451"
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_account_balances;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_balances(
    "account-name" TEXT
)
RETURNS btracker_endpoints.balances 
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
        MAX(CASE WHEN cab.nai = 13 THEN cab.balance END) AS hbd_balance,
        MAX(CASE WHEN cab.nai = 21 THEN cab.balance END) AS hive_balance, 
        MAX(CASE WHEN cab.nai = 37 THEN cab.balance END) AS vesting_shares 
      FROM current_account_balances cab
      WHERE cab.account= (SELECT id FROM get_account_id)
    ),
    get_vest_balance AS
    (
      SELECT hive.get_vesting_balance(bv.num, gb.vesting_shares) AS vesting_balance_hive
      FROM get_balances gb
      JOIN LATERAL (
        SELECT num FROM hive.blocks_view ORDER BY num DESC LIMIT 1
      ) bv ON TRUE
    ),
    get_post_voting_power AS
    (
      SELECT (
        COALESCE(gb.vesting_shares, 0) - gad.delegated_vests::BIGINT + gad.received_vests::BIGINT
      ) AS post_voting_power_vests
      FROM get_balances gb, btracker_endpoints.get_account_delegations("account-name") gad
    )
    SELECT (
      COALESCE(gb.hbd_balance,0),
      COALESCE(gb.hive_balance ,0),
      COALESCE(gb.vesting_shares::TEXT ,'0'),
      COALESCE(gvb.vesting_balance_hive ,0),
      COALESCE(gpvp.post_voting_power_vests::TEXT ,'0'))::btracker_endpoints.balances
    FROM 
      get_balances gb,
      get_vest_balance gvb,
      get_post_voting_power gpvp
  );
END
$$;

RESET ROLE;

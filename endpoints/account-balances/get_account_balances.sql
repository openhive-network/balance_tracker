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
    delegated_vests:
      type: string
      description: >-
        VESTS delegated to another user, 
        account''s power is lowered by delegated VESTS
    received_vests:
      type: string
      description: >-
        VESTS received from another user, 
        account''s power is increased by received VESTS
    curation_rewards:
      type: string
      description: rewards obtained by posting and commenting expressed in VEST
    posting_rewards:
      type: string
      description: curator''s reward expressed in VEST
    hbd_rewards:
      type: integer
      x-sql-datatype: BIGINT
      description: >-
        not yet claimed HIVE backed dollars 
        stored in hbd reward balance
    hive_rewards:
      type: integer
      x-sql-datatype: BIGINT
      description: >-
        not yet claimed HIVE 
        stored in hive reward balance
    vests_rewards:
      type: string
      description: >-
        not yet claimed VESTS 
        stored in vest reward balance
    hive_vesting_rewards:
      type: integer
      x-sql-datatype: BIGINT
      description: >-
        the reward vesting balance, denominated in HIVE, 
        is determined by the prevailing HIVE price at the time of reward reception
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
DROP TYPE IF EXISTS btracker_endpoints.balances CASCADE;
CREATE TYPE btracker_endpoints.balances AS (
    "hbd_balance" BIGINT,
    "hive_balance" BIGINT,
    "vesting_shares" TEXT,
    "vesting_balance_hive" BIGINT,
    "post_voting_power_vests" TEXT,
    "delegated_vests" TEXT,
    "received_vests" TEXT,
    "curation_rewards" TEXT,
    "posting_rewards" TEXT,
    "hbd_rewards" BIGINT,
    "hive_rewards" BIGINT,
    "vests_rewards" TEXT,
    "hive_vesting_rewards" BIGINT,
    "hbd_savings" BIGINT,
    "hive_savings" BIGINT,
    "savings_withdraw_requests" INT,
    "vesting_withdraw_rate" TEXT,
    "to_withdraw" TEXT,
    "withdrawn" TEXT,
    "withdraw_routes" INT,
    "delayed_vests" TEXT
);
-- openapi-generated-code-end

/** openapi:paths
/accounts/{account-name}/balances:
  get:
    tags:
      - Accounts
    summary: Account balances
    description: |
      Lists account hbd, hive and vest balances

      SQL example
      * `SELECT * FROM btracker_endpoints.get_account_balances(''blocktrades'');`

      REST call example
      * `GET ''https://%1$s/balance-api/accounts/blocktrades/balances''`
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
                delegated_vests: "0"
                received_vests: "0"
                curation_rewards: "196115157"
                posting_rewards: "65916519"
                hbd_rewards: 0
                hive_rewards: 0
                vests_rewards: "0"
                hive_vesting_rewards: 0
                hbd_savings: 0
                hive_savings: 0
                savings_withdraw_requests: 0
                vesting_withdraw_rate: "80404818220529"
                to_withdraw: "8362101094935031"
                withdrawn: "804048182205290"
                withdraw_routes: 4
                delayed_vests: "0"
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

  --balance
  RETURN (
    WITH get_account_id AS MATERIALIZED
    (
      SELECT av.id FROM hive.accounts_view av WHERE av.name = "account-name" 
    ),
    get_delegations AS
    (
      SELECT 
        COALESCE(ad.delegated_vests::TEXT, '0') AS delegated_vests,
        COALESCE(ad.received_vests::TEXT, '0') AS received_vests
      FROM (
        SELECT 
          NULL::TEXT AS delegated_vests,
          NULL::TEXT AS received_vests
      ) default_values
      LEFT JOIN get_account_id gai ON true
      LEFT JOIN account_delegations ad ON ad.account = gai.id
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
      FROM get_balances gb, get_delegations gad
    ),
    get_info_rewards AS
    (
      SELECT 
        COALESCE(air.curation_rewards::TEXT, '0') AS curation_rewards,
        COALESCE(air.posting_rewards::TEXT, '0') AS posting_rewards
      FROM (
        SELECT 
          NULL::TEXT AS curation_rewards,
          NULL::TEXT AS posting_rewards
      ) default_values
      LEFT JOIN get_account_id gai ON true
      LEFT JOIN account_info_rewards air ON air.account = gai.id
    ),
    calculate_rewards AS MATERIALIZED
    (
      SELECT  
        MAX(CASE WHEN ar.nai = 13 THEN ar.balance END) AS hbd_rewards,
        MAX(CASE WHEN ar.nai = 21 THEN ar.balance END) AS hive_rewards,
        MAX(CASE WHEN ar.nai = 37 THEN ar.balance END) AS vests_rewards,
        MAX(CASE WHEN ar.nai = 38 THEN ar.balance END) AS hive_vesting_rewards
      FROM account_rewards ar
      WHERE ar.account= (SELECT id FROM get_account_id)
    ),
    get_rewards AS 
    (
      SELECT 
        COALESCE(gr.hbd_rewards,0) AS hbd_rewards,
        COALESCE(gr.hive_rewards ,0) AS hive_rewards,
        COALESCE(gr.vests_rewards::TEXT ,'0') AS vests_rewards,
        COALESCE(gr.hive_vesting_rewards, 0) AS hive_vesting_rewards
      FROM 
        calculate_rewards gr
    ),
    calculate_savings AS MATERIALIZED
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
    ),
    get_savings AS
    (
      SELECT 
        COALESCE(cs.hbd_savings,0) AS hbd_savings,
        COALESCE(cs.hive_savings ,0) AS hive_savings,
        COALESCE(gwr.total ,0) AS total
      FROM 
        calculate_savings cs,
        get_withdraw_requests gwr
    ),
    get_withdraws AS
    (
      SELECT 
        COALESCE(aw.vesting_withdraw_rate::TEXT, '0') AS vesting_withdraw_rate,
        COALESCE(aw.to_withdraw::TEXT, '0') AS to_withdraw,
        COALESCE(aw.withdrawn::TEXT, '0') AS withdrawn,
        COALESCE(aw.withdraw_routes, 0) AS withdraw_routes,
        COALESCE(aw.delayed_vests::TEXT, '0') AS delayed_vests
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
    )
    SELECT 
    (
      COALESCE(gb.hbd_balance,0),
      COALESCE(gb.hive_balance ,0),
      COALESCE(gb.vesting_shares::TEXT ,'0'),
      COALESCE(gvb.vesting_balance_hive ,0),
      COALESCE(gpvp.post_voting_power_vests::TEXT ,'0'),
      gd.delegated_vests,
      gd.received_vests,
      gir.curation_rewards,
      gir.posting_rewards,
      gr.hbd_rewards,
      gr.hive_rewards,
      gr.vests_rewards,
      gr.hive_vesting_rewards,
      gs.hbd_savings,
      gs.hive_savings,
      gs.total,
      gw.vesting_withdraw_rate,
      gw.to_withdraw,
      gw.withdrawn,
      gw.withdraw_routes,
      gw.delayed_vests
    )::btracker_endpoints.balances
    FROM 
      get_balances gb,
      get_vest_balance gvb,
      get_post_voting_power gpvp,
      get_delegations gd,
      get_info_rewards gir,
      get_rewards gr,
      get_savings gs,
      get_withdraws gw
  );

END
$$;

RESET ROLE;

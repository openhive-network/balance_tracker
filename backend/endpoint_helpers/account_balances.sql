SET ROLE btracker_owner;

/*
Backend helper for the get_account_balances endpoint.
Called by: btracker_endpoints.get_account_balances()
*/
CREATE OR REPLACE FUNCTION btracker_backend.get_account_balances(
    _account_id INT
)
RETURNS btracker_backend.balance
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
  result_row btracker_backend.balance;
  -- Cache NAI values to avoid repeated function calls per row in CASE expressions
  _nai_hbd   CONSTANT SMALLINT := btracker_backend.nai_hbd();
  _nai_hive  CONSTANT SMALLINT := btracker_backend.nai_hive();
  _nai_vests CONSTANT SMALLINT := btracker_backend.nai_vests();
  _nai_vests_as_hive CONSTANT SMALLINT := btracker_backend.nai_vests_as_hive();
BEGIN
  WITH
    -- Get delegation summary (VESTS delegated to/received from other accounts)
    -- Uses dummy table pattern to ensure a row is returned even with no delegations
    get_delegations AS (
      SELECT
        COALESCE(ad.delegated_vests, 0)::BIGINT AS delegated_vests,
        COALESCE(ad.received_vests, 0)::BIGINT  AS received_vests
      FROM (VALUES(1)) AS dummy(_)
      LEFT JOIN account_delegations ad
        ON ad.account = _account_id
    ),

    -- Get current liquid balances for all asset types
    -- MATERIALIZED: vesting_shares is referenced by multiple downstream CTEs
    -- Conditional aggregation pivots NAI rows (one per asset) into named columns
    get_balances AS MATERIALIZED (
      SELECT
        COALESCE(MAX(CASE WHEN cab.nai = _nai_hbd THEN cab.balance END), 0)::BIGINT AS hbd_balance,
        COALESCE(MAX(CASE WHEN cab.nai = _nai_hive THEN cab.balance END), 0)::BIGINT AS hive_balance,
        COALESCE(MAX(CASE WHEN cab.nai = _nai_vests THEN cab.balance END), 0)::BIGINT AS vesting_shares
      FROM btracker_backend.current_account_balances_view cab
      WHERE cab.account = _account_id
    ),

    -- Convert VESTS to HIVE equivalent using current blockchain exchange rate
    -- Uses hive.get_vesting_balance() which calculates: vests * (total_fund_hive / total_vests)
    get_vest_balance AS (
      SELECT
        COALESCE(
          hive.get_vesting_balance(bv.num, gb.vesting_shares),
          0
        )::BIGINT AS vesting_balance_hive
      FROM get_balances gb
      CROSS JOIN LATERAL (
        SELECT num
        FROM hive.blocks_view
        ORDER BY num DESC
        LIMIT 1
      ) bv
    ),

    -- Calculate effective voting power after delegations
    -- Formula: owned_vests - delegated_out + received_in
    get_post_voting_power AS (
      SELECT
        (gb.vesting_shares - gd.delegated_vests + gd.received_vests)::BIGINT AS post_voting_power_vests
      FROM get_balances gb
      CROSS JOIN get_delegations gd
    ),

    -- Get lifetime accumulated rewards (curation + posting)
    -- Uses dummy table pattern to return zeros if account has no reward history
    get_info_rewards AS (
      SELECT
        COALESCE(air.curation_rewards, 0)::BIGINT AS curation_rewards,
        COALESCE(air.posting_rewards, 0)::BIGINT  AS posting_rewards
      FROM (VALUES(1)) AS dummy(_)
      LEFT JOIN account_info_rewards air
        ON air.account = _account_id
    ),

    -- Get pending unclaimed rewards for all asset types
    -- NAI 38 (vests_as_hive) is a virtual type: VESTS valued at HIVE rate when reward was earned
    calculate_rewards AS MATERIALIZED (
      SELECT
        COALESCE(MAX(CASE WHEN ar.nai = _nai_hbd THEN ar.balance END), 0)::BIGINT AS hbd_rewards,
        COALESCE(MAX(CASE WHEN ar.nai = _nai_hive THEN ar.balance END), 0)::BIGINT AS hive_rewards,
        COALESCE(MAX(CASE WHEN ar.nai = _nai_vests THEN ar.balance END), 0)::BIGINT AS vests_rewards,
        COALESCE(MAX(CASE WHEN ar.nai = _nai_vests_as_hive THEN ar.balance END), 0)::BIGINT AS hive_vesting_rewards
      FROM btracker_backend.account_rewards_view ar
      WHERE ar.account = _account_id
    ),

    -- Get savings account balances and pending withdrawal request count
    -- Savings have a 3-day withdrawal delay for security
    get_savings AS (
      SELECT
        COALESCE(MAX(CASE WHEN ats.nai = _nai_hbd THEN ats.balance END), 0)::BIGINT AS hbd_savings,
        COALESCE(MAX(CASE WHEN ats.nai = _nai_hive THEN ats.balance END), 0)::BIGINT AS hive_savings,
        COALESCE(SUM(ats.savings_withdraw_requests), 0)::INT                  AS savings_withdraw_requests
      FROM btracker_backend.account_savings_view ats
      WHERE ats.account = _account_id
    ),

    -- Get power-down (vesting withdrawal) state
    -- vesting_withdraw_rate = weekly payout (total / 13 weeks)
    -- delayed_vests = VESTS blocked during 30-day voting delay (HF24+)
    get_withdraws AS (
      SELECT
        COALESCE(aw.vesting_withdraw_rate, 0)::BIGINT AS vesting_withdraw_rate,
        COALESCE(aw.to_withdraw, 0)::BIGINT           AS to_withdraw,
        COALESCE(aw.withdrawn, 0)::BIGINT             AS withdrawn,
        COALESCE(aw.withdraw_routes, 0)::INT          AS withdraw_routes,
        COALESCE(aw.delayed_vests, 0)::BIGINT         AS delayed_vests
      FROM (VALUES(1)) AS dummy(_)
      LEFT JOIN account_withdraws aw
        ON aw.account = _account_id
    ),

    -- Get pending HBD<->HIVE conversion requests
    -- Conversions have a 3.5-day delay before completion
    conv_pivot AS (
      SELECT
        COALESCE(SUM(cs.remaining) FILTER (WHERE cs.nai = _nai_hbd), 0)::BIGINT AS conversion_pending_amount_hbd,
        COUNT(*) FILTER (WHERE cs.nai = _nai_hbd)::INT                          AS conversion_pending_count_hbd,
        COALESCE(SUM(cs.remaining) FILTER (WHERE cs.nai = _nai_hive), 0)::BIGINT AS conversion_pending_amount_hive,
        COUNT(*) FILTER (WHERE cs.nai = _nai_hive)::INT                          AS conversion_pending_count_hive
      FROM convert_state cs
      WHERE cs.owner_id = _account_id
    ),

    -- Get open limit orders on the internal HBD/HIVE market
    open_orders AS (
      SELECT
        COUNT(*) FILTER (WHERE s.nai = _nai_hbd)::INT                          AS open_orders_hbd_count,
        COUNT(*) FILTER (WHERE s.nai = _nai_hive)::INT                          AS open_orders_hive_count,
        COALESCE(SUM(s.remaining) FILTER (WHERE s.nai = _nai_hive), 0)::BIGINT  AS open_orders_hive_amount,
        COALESCE(SUM(s.remaining) FILTER (WHERE s.nai = _nai_hbd), 0)::BIGINT  AS open_orders_hbd_amount
      FROM order_state s
      WHERE s.owner_id = _account_id
    ),

    -- Get amounts in 3-day savings withdrawal delay
    -- These funds have left savings but not yet arrived in liquid balance
    savings_pivot AS (
      SELECT
        COALESCE(SUM(tsi.balance) FILTER (WHERE tsi.nai = _nai_hbd), 0)::BIGINT AS savings_pending_amount_hbd,
        COALESCE(SUM(tsi.balance) FILTER (WHERE tsi.nai = _nai_hive), 0)::BIGINT AS savings_pending_amount_hive
      FROM transfer_saving_id tsi
      WHERE tsi.account = _account_id
    ),

    -- Get active escrow amounts where this account is the sender
    escrow_pivot AS (
      SELECT
        COALESCE(SUM(es.hbd_amount), 0)::BIGINT  AS escrow_pending_amount_hbd,
        COALESCE(SUM(es.hive_amount), 0)::BIGINT AS escrow_pending_amount_hive,
        COUNT(*)::INT                            AS escrow_pending_count
      FROM escrow_state es
      WHERE es.from_id = _account_id
    )

  -- Assemble all CTEs into final result via CROSS JOIN (each CTE returns exactly one row)
  SELECT
    gb.hbd_balance,
    gb.hive_balance,
    gb.vesting_shares,
    gvb.vesting_balance_hive,
    gpv.post_voting_power_vests,
    gd.delegated_vests,
    gd.received_vests,
    ir.curation_rewards,
    ir.posting_rewards,
    cr.hbd_rewards,
    cr.hive_rewards,
    cr.vests_rewards,
    cr.hive_vesting_rewards,
    sv.hbd_savings,
    sv.hive_savings,
    sv.savings_withdraw_requests,
    wd.vesting_withdraw_rate,
    wd.to_withdraw,
    wd.withdrawn,
    wd.withdraw_routes,
    wd.delayed_vests,
    cp.conversion_pending_amount_hbd,
    cp.conversion_pending_count_hbd,
    cp.conversion_pending_amount_hive,
    cp.conversion_pending_count_hive,
    oo.open_orders_hbd_count,
    oo.open_orders_hive_count,
    oo.open_orders_hive_amount,
    oo.open_orders_hbd_amount,
    sp.savings_pending_amount_hbd,
    sp.savings_pending_amount_hive,
    ep.escrow_pending_amount_hbd,
    ep.escrow_pending_amount_hive,
    ep.escrow_pending_count
  INTO result_row
  FROM get_balances          gb
  CROSS JOIN get_vest_balance       gvb
  CROSS JOIN get_post_voting_power  gpv
  CROSS JOIN get_delegations        gd
  CROSS JOIN get_info_rewards       ir
  CROSS JOIN calculate_rewards      cr
  CROSS JOIN get_savings            sv
  CROSS JOIN get_withdraws          wd
  CROSS JOIN conv_pivot             cp
  CROSS JOIN open_orders            oo
  CROSS JOIN savings_pivot          sp
  CROSS JOIN escrow_pivot           ep;

  RETURN result_row;
END;
$BODY$;

RESET ROLE;

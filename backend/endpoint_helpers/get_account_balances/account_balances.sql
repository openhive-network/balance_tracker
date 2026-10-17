SET ROLE btracker_owner;

/*
Backend helper for the get_account_balances endpoint.
Called by: btracker_endpoints.get_account_balances()

Each source table is read by its own small statement (all are primary-key or
owner lookups for _account_id that return exactly one row). One statement joining
all of them cost 8-12 ms to plan for well under 1 ms of execution, and that plan
was rebuilt on every call while plpgsql kept custom plans. Small statements are
cheap to plan, and force_generic_plan plans each of them once per session: every
predicate is an equality on the account, so the generic plan is the index lookup.
*/
CREATE OR REPLACE FUNCTION btracker_backend.get_account_balances(
    _account_id INT
)
RETURNS btracker_backend.balance
LANGUAGE plpgsql
STABLE
SET plan_cache_mode = 'force_generic_plan'
AS $BODY$
DECLARE
  result_row btracker_backend.balance;
  _vesting_shares BIGINT;
  _delegated_vests BIGINT;
  _received_vests BIGINT;
  -- Cache NAI values to avoid repeated function calls per row in CASE expressions
  _nai_hbd   CONSTANT SMALLINT := btracker_backend.nai_hbd();
  _nai_hive  CONSTANT SMALLINT := btracker_backend.nai_hive();
  _nai_vests CONSTANT SMALLINT := btracker_backend.nai_vests();
  _nai_vests_as_hive CONSTANT SMALLINT := btracker_backend.nai_vests_as_hive();
BEGIN
  -- Current liquid balances for all asset types
  -- Conditional aggregation pivots NAI rows (one per asset) into named columns
  SELECT
    COALESCE(MAX(CASE WHEN cab.nai = _nai_hbd THEN cab.balance END), 0)::BIGINT,
    COALESCE(MAX(CASE WHEN cab.nai = _nai_hive THEN cab.balance END), 0)::BIGINT,
    COALESCE(MAX(CASE WHEN cab.nai = _nai_vests THEN cab.balance END), 0)::BIGINT
  INTO result_row.hbd_balance, result_row.hive_balance, _vesting_shares
  FROM btracker_backend.current_account_balances_view cab
  WHERE cab.account = _account_id;

  -- Convert VESTS to HIVE equivalent using current blockchain exchange rate
  -- Uses hive.get_vesting_balance() which calculates: vests * (total_fund_hive / total_vests)
  SELECT
    COALESCE(
      hive.get_vesting_balance(bv.num, _vesting_shares),
      0
    )::BIGINT
  INTO result_row.vesting_balance_hive
  FROM (
    SELECT num
    FROM hive.blocks_view
    ORDER BY num DESC
    LIMIT 1
  ) bv;

  -- Delegation summary (VESTS delegated to/received from other accounts)
  -- Uses dummy table pattern to ensure a row is returned even with no delegations
  SELECT
    COALESCE(ad.delegated_vests, 0)::BIGINT,
    COALESCE(ad.received_vests, 0)::BIGINT
  INTO _delegated_vests, _received_vests
  FROM (VALUES(1)) AS dummy(_)
  LEFT JOIN account_delegations ad
    ON ad.account = _account_id;

  result_row.vesting_shares          := _vesting_shares;
  result_row.delegated_vests         := _delegated_vests;
  result_row.received_vests          := _received_vests;
  -- Effective voting power after delegations: owned_vests - delegated_out + received_in
  result_row.post_voting_power_vests := (_vesting_shares - _delegated_vests + _received_vests)::BIGINT;

  -- Lifetime accumulated rewards (curation + posting)
  -- Uses dummy table pattern to return zeros if account has no reward history
  SELECT
    COALESCE(air.curation_rewards, 0)::BIGINT,
    COALESCE(air.posting_rewards, 0)::BIGINT
  INTO result_row.curation_rewards, result_row.posting_rewards
  FROM (VALUES(1)) AS dummy(_)
  LEFT JOIN account_info_rewards air
    ON air.account = _account_id;

  -- Pending unclaimed rewards for all asset types
  -- NAI 38 (vests_as_hive) is a virtual type: VESTS valued at HIVE rate when reward was earned
  SELECT
    COALESCE(MAX(CASE WHEN ar.nai = _nai_hbd THEN ar.balance END), 0)::BIGINT,
    COALESCE(MAX(CASE WHEN ar.nai = _nai_hive THEN ar.balance END), 0)::BIGINT,
    COALESCE(MAX(CASE WHEN ar.nai = _nai_vests THEN ar.balance END), 0)::BIGINT,
    COALESCE(MAX(CASE WHEN ar.nai = _nai_vests_as_hive THEN ar.balance END), 0)::BIGINT
  INTO result_row.hbd_rewards, result_row.hive_rewards, result_row.vests_rewards, result_row.hive_vesting_rewards
  FROM btracker_backend.account_rewards_view ar
  WHERE ar.account = _account_id;

  -- Savings account balances and pending withdrawal request count
  -- Savings have a 3-day withdrawal delay for security
  SELECT
    COALESCE(MAX(CASE WHEN ats.nai = _nai_hbd THEN ats.balance END), 0)::BIGINT,
    COALESCE(MAX(CASE WHEN ats.nai = _nai_hive THEN ats.balance END), 0)::BIGINT,
    COALESCE(SUM(ats.savings_withdraw_requests), 0)::INT
  INTO result_row.hbd_savings, result_row.hive_savings, result_row.savings_withdraw_requests
  FROM btracker_backend.account_savings_view ats
  WHERE ats.account = _account_id;

  -- Power-down (vesting withdrawal) state
  -- vesting_withdraw_rate = weekly payout (total / 13 weeks)
  -- delayed_vests = VESTS blocked during 30-day voting delay (HF24+)
  SELECT
    COALESCE(aw.vesting_withdraw_rate, 0)::BIGINT,
    COALESCE(aw.to_withdraw, 0)::BIGINT,
    COALESCE(aw.withdrawn, 0)::BIGINT,
    COALESCE(aw.withdraw_routes, 0)::INT,
    COALESCE(aw.delayed_vests, 0)::BIGINT
  INTO result_row.vesting_withdraw_rate, result_row.to_withdraw, result_row.withdrawn,
       result_row.withdraw_routes, result_row.delayed_vests
  FROM (VALUES(1)) AS dummy(_)
  LEFT JOIN account_withdraws aw
    ON aw.account = _account_id;

  -- Pending HBD<->HIVE conversion requests
  -- Conversions have a 3.5-day delay before completion
  SELECT
    COALESCE(SUM(cs.remaining) FILTER (WHERE cs.nai = _nai_hbd), 0)::BIGINT,
    COUNT(*) FILTER (WHERE cs.nai = _nai_hbd)::INT,
    COALESCE(SUM(cs.remaining) FILTER (WHERE cs.nai = _nai_hive), 0)::BIGINT,
    COUNT(*) FILTER (WHERE cs.nai = _nai_hive)::INT
  INTO result_row.conversion_pending_amount_hbd, result_row.conversion_pending_count_hbd,
       result_row.conversion_pending_amount_hive, result_row.conversion_pending_count_hive
  FROM convert_state cs
  WHERE cs.owner_id = _account_id;

  -- Open limit orders on the internal HBD/HIVE market
  SELECT
    COUNT(*) FILTER (WHERE s.nai = _nai_hbd)::INT,
    COUNT(*) FILTER (WHERE s.nai = _nai_hive)::INT,
    COALESCE(SUM(s.remaining) FILTER (WHERE s.nai = _nai_hive), 0)::BIGINT,
    COALESCE(SUM(s.remaining) FILTER (WHERE s.nai = _nai_hbd), 0)::BIGINT
  INTO result_row.open_orders_hbd_count, result_row.open_orders_hive_count,
       result_row.open_orders_hive_amount, result_row.open_orders_hbd_amount
  FROM order_state s
  WHERE s.owner_id = _account_id;

  -- Amounts in 3-day savings withdrawal delay
  -- These funds have left savings but not yet arrived in liquid balance
  SELECT
    COALESCE(-SUM(tsi.balance) FILTER (WHERE tsi.nai = _nai_hbd), 0)::BIGINT,
    COALESCE(-SUM(tsi.balance) FILTER (WHERE tsi.nai = _nai_hive), 0)::BIGINT
  INTO result_row.savings_pending_amount_hbd, result_row.savings_pending_amount_hive
  FROM transfer_saving_id tsi
  WHERE tsi.account = _account_id;

  -- Active escrow amounts where this account is the sender
  SELECT
    COALESCE(SUM(es.hbd_amount), 0)::BIGINT,
    COALESCE(SUM(es.hive_amount), 0)::BIGINT,
    COUNT(*)::INT
  INTO result_row.escrow_pending_amount_hbd, result_row.escrow_pending_amount_hive, result_row.escrow_pending_count
  FROM escrow_state es
  WHERE es.from_id = _account_id;

  RETURN result_row;
END;
$BODY$;

RESET ROLE;

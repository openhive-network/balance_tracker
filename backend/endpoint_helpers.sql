SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_backend.get_account_balances(
    _account_id INT
)
RETURNS btracker_backend.balance
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
  result_row btracker_backend.balance;
BEGIN
  WITH
    -- 1) Delegations
    get_delegations AS (
      SELECT
        COALESCE(ad.delegated_vests,0)::BIGINT AS delegated_vests,
        COALESCE(ad.received_vests,0)::BIGINT   AS received_vests
      FROM (VALUES(1)) AS dummy(_)
      LEFT JOIN account_delegations ad
        ON ad.account = _account_id
    ),

    -- 2) Liquid balances & vesting shares
    get_balances AS MATERIALIZED (
      SELECT
        COALESCE(MAX(CASE WHEN cab.nai=13 THEN cab.balance END),0)::BIGINT AS hbd_balance,
        COALESCE(MAX(CASE WHEN cab.nai=21 THEN cab.balance END),0)::BIGINT AS hive_balance,
        COALESCE(MAX(CASE WHEN cab.nai=37 THEN cab.balance END),0)::BIGINT AS vesting_shares
      FROM btracker_backend.current_account_balances_view cab
      WHERE cab.account = _account_id
    ),

    -- 3) Convert vesting_shares → hive‐vest balance
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

    -- 4) Post‐voting power
    get_post_voting_power AS (
      SELECT
        (gb.vesting_shares
         - gd.delegated_vests
         + gd.received_vests
        )::BIGINT AS post_voting_power_vests
      FROM get_balances gb
      CROSS JOIN get_delegations gd
    ),

    -- 5) Curation & posting rewards
    get_info_rewards AS (
      SELECT
        COALESCE(air.curation_rewards,0)::BIGINT AS curation_rewards,
        COALESCE(air.posting_rewards,0)::BIGINT AS posting_rewards
      FROM (VALUES(1)) AS dummy(_)
      LEFT JOIN account_info_rewards air
        ON air.account = _account_id
    ),

    -- 6) Other rewards
    calculate_rewards AS MATERIALIZED (
      SELECT
        COALESCE(MAX(CASE WHEN ar.nai=13 THEN ar.balance END),0)::BIGINT AS hbd_rewards,
        COALESCE(MAX(CASE WHEN ar.nai=21 THEN ar.balance END),0)::BIGINT AS hive_rewards,
        COALESCE(MAX(CASE WHEN ar.nai=37 THEN ar.balance END),0)::BIGINT AS vests_rewards,
        COALESCE(MAX(CASE WHEN ar.nai=38 THEN ar.balance END),0)::BIGINT AS hive_vesting_rewards
      FROM btracker_backend.account_rewards_view ar
      WHERE ar.account = _account_id
    ),

    -- 7) Savings + withdraw‐requests count
    get_savings AS (
      SELECT
        COALESCE(MAX(CASE WHEN ats.nai=13 THEN ats.balance END),0)::BIGINT AS hbd_savings,
        COALESCE(MAX(CASE WHEN ats.nai=21 THEN ats.balance END),0)::BIGINT AS hive_savings,
        COALESCE(SUM(ats.savings_withdraw_requests),0)::INT              AS savings_withdraw_requests
      FROM btracker_backend.account_savings_view ats
      WHERE ats.account = _account_id
    ),

    -- 8) Vesting‐withdraw info
    get_withdraws AS (
      SELECT
        COALESCE(aw.vesting_withdraw_rate,0)::BIGINT AS vesting_withdraw_rate,
        COALESCE(aw.to_withdraw,0)::BIGINT             AS to_withdraw,
        COALESCE(aw.withdrawn,0)::BIGINT               AS withdrawn,
        COALESCE(aw.withdraw_routes,0)::INT            AS withdraw_routes,
        COALESCE(aw.delayed_vests,0)::BIGINT           AS delayed_vests
      FROM (VALUES(1)) AS dummy(_)
      LEFT JOIN account_withdraws aw
        ON aw.account = _account_id
    ),

   -- 9) Pending conversions (BIGINT satoshis) — from convert_state
conv_pivot AS (
  SELECT
    COALESCE(SUM(cs.remaining) FILTER (WHERE cs.nai = 13), 0)::BIGINT AS conversion_pending_amount_hbd,
    COUNT(*)                      FILTER (WHERE cs.nai = 13)::INT      AS conversion_pending_count_hbd,
    COALESCE(SUM(cs.remaining) FILTER (WHERE cs.nai = 21), 0)::BIGINT AS conversion_pending_amount_hive,
    COUNT(*)                      FILTER (WHERE cs.nai = 21)::INT      AS conversion_pending_count_hive
  FROM convert_state cs
  WHERE cs.owner_id = _account_id
),

-- 10) Open orders summary (BIGINT satoshis) — from order_state
open_orders AS (
  SELECT
    COUNT(*)                                           FILTER (WHERE s.nai = 13)::INT      AS open_orders_hbd_count,
    COUNT(*)                                           FILTER (WHERE s.nai = 21)::INT      AS open_orders_hive_count,
    COALESCE(SUM(s.remaining) FILTER (WHERE s.nai = 21), 0)::BIGINT AS open_orders_hive_amount,
    COALESCE(SUM(s.remaining) FILTER (WHERE s.nai = 13), 0)::BIGINT AS open_orders_hbd_amount
  FROM order_state s
  WHERE s.owner_id = _account_id
),
-- 11) Pending Savings → liquid (from transfer_saving_id)
savings_pivot AS (
  SELECT
    COALESCE(SUM(tsi.balance) FILTER (WHERE tsi.nai = 13), 0)::BIGINT AS savings_pending_amount_hbd,
    COALESCE(SUM(tsi.balance) FILTER (WHERE tsi.nai = 21), 0)::BIGINT AS savings_pending_amount_hive
  FROM transfer_saving_id tsi
  WHERE tsi.account = _account_id
),

    -- 13) Escrows (INCOMING: you are the beneficiary)
  escrow_pivot AS (
    SELECT
      COALESCE(SUM(es.remaining) FILTER (WHERE es.nai = 13), 0)::BIGINT AS escrow_pending_amount_hbd,
      COALESCE(SUM(es.remaining) FILTER (WHERE es.nai = 21), 0)::BIGINT AS escrow_pending_amount_hive,
      COUNT(*)::INT AS escrow_pending_count
    FROM escrow_state es
    WHERE es.from_id = _account_id
)


  -- Final assembly
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
  FROM get_balances     gb
  CROSS JOIN get_vest_balance      gvb
  CROSS JOIN get_post_voting_power gpv
  CROSS JOIN get_delegations       gd
  CROSS JOIN get_info_rewards      ir
  CROSS JOIN calculate_rewards     cr
  CROSS JOIN get_savings           sv
  CROSS JOIN get_withdraws         wd
  CROSS JOIN conv_pivot            cp
  CROSS JOIN open_orders           oo
  CROSS JOIN savings_pivot         sp   
  CROSS JOIN escrow_pivot   ep;        

  RETURN result_row;


END;
$BODY$;

CREATE OR REPLACE FUNCTION btracker_backend.get_top_holders(
    _coin_type    INT,
    _balance_type btracker_backend.balance_type,
    _page         INT,
    _limit        INT
)
RETURNS btracker_backend.top_holders
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  _safe_page    INT := GREATEST(COALESCE(_page, 1), 1);
  _safe_ps      INT := GREATEST(COALESCE(_limit, 100), 1);
  _ofs          INT := (_safe_page - 1) * _safe_ps;

  _total        INT := 0;
  _total_pages  INT := 0;
  _rows         btracker_backend.ranked_holder[];
  _sql_tot      TEXT;
  _sql_rows     TEXT;
BEGIN
  IF _balance_type = 'balance' THEN
    -- totals (balance)
    _sql_tot := format(
      'SELECT COUNT(*) FROM %I.%I WHERE nai = $1 AND balance > 0',
      'btracker_backend','current_account_balances_view'
    );
    EXECUTE _sql_tot INTO _total USING _coin_type;

    -- pages
    IF _total = 0 THEN
      _total_pages := 0;
      _safe_page := 1;
      _ofs := 0;
      _rows := ARRAY[]::btracker_backend.ranked_holder[];
    ELSE
      _total_pages := CEIL(_total::numeric / _safe_ps)::INT;
      _safe_page   := LEAST(_safe_page, _total_pages);
      _ofs         := (_safe_page - 1) * _safe_ps;

      -- rows (balance)
      _sql_rows := format($q$
        WITH ordered_holders AS MATERIALIZED (
          SELECT av.name, src.balance
          FROM %I.%I AS src
          JOIN hive.accounts_view av ON av.id = src.account
          WHERE src.nai = $1
          ORDER BY src.balance DESC, av.name ASC
          OFFSET $2
          LIMIT $3
        ),
        ranked AS (
          SELECT
            (ROW_NUMBER() OVER (ORDER BY o.balance DESC, o.name ASC) + $2)::INT AS rank,
            o.name::TEXT    AS account,
            o.balance::NUMERIC(38,0) AS value
          FROM ordered_holders o
        )
        SELECT COALESCE(
                 array_agg(
                   (ROW(r.rank, r.account, r.value::text))::btracker_backend.ranked_holder
                   ORDER BY r.rank
                 ),
                 ARRAY[]::btracker_backend.ranked_holder[]
               )
        FROM ranked r
      $q$, 'btracker_backend','current_account_balances_view');

      EXECUTE _sql_rows INTO _rows USING _coin_type, _ofs, _safe_ps;
    END IF;

  ELSIF _balance_type = 'savings_balance' THEN
    -- totals (savings)
    _sql_tot := format(
      'SELECT COUNT(*) FROM %I.%I WHERE nai = $1 AND balance > 0',
      'btracker_backend','account_savings_view'
    );
    EXECUTE _sql_tot INTO _total USING _coin_type;

    -- pages
    IF _total = 0 THEN
      _total_pages := 0;
      _safe_page := 1;
      _ofs := 0;
      _rows := ARRAY[]::btracker_backend.ranked_holder[];
    ELSE
      _total_pages := CEIL(_total::numeric / _safe_ps)::INT;
      _safe_page   := LEAST(_safe_page, _total_pages);
      _ofs         := (_safe_page - 1) * _safe_ps;

      -- rows (savings)
      _sql_rows := format($q$
        WITH ordered_holders AS MATERIALIZED (
          SELECT av.name, src.balance
          FROM %I.%I AS src
          JOIN hive.accounts_view av ON av.id = src.account
          WHERE src.nai = $1
          ORDER BY src.balance DESC, av.name ASC
          OFFSET $2
          LIMIT $3
        ),
        ranked AS (
          SELECT
            (ROW_NUMBER() OVER (ORDER BY o.balance DESC, o.name ASC) + $2)::INT AS rank,
            o.name::TEXT    AS account,
            o.balance::NUMERIC(38,0) AS value
          FROM ordered_holders o
        )
        SELECT COALESCE(
                 array_agg(
                   (ROW(r.rank, r.account, r.value::text))::btracker_backend.ranked_holder
                   ORDER BY r.rank
                 ),
                 ARRAY[]::btracker_backend.ranked_holder[]
               )
        FROM ranked r
      $q$, 'btracker_backend','account_savings_view');

      EXECUTE _sql_rows INTO _rows USING _coin_type, _ofs, _safe_ps;
    END IF;

  ELSE
    RAISE EXCEPTION 'Unsupported balance type: %', _balance_type;
  END IF;

  RETURN (_total, _total_pages, _rows)::btracker_backend.top_holders;
END;
$$;


CREATE OR REPLACE FUNCTION btracker_backend.incoming_delegations(IN _account_id INT)
RETURNS SETOF btracker_backend.incoming_delegations
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN QUERY (
    SELECT 
      (SELECT av.name FROM hive.accounts_view av WHERE av.id = d.delegator)::TEXT AS delegator,
      d.balance::TEXT AS amount,
      d.source_op::TEXT AS operation_id,
      d.source_op_block AS block_num 
    FROM btracker_backend.current_accounts_delegations_view d
    WHERE delegatee = _account_id
  );
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.outgoing_delegations(IN _account_id INT)
RETURNS SETOF btracker_backend.outgoing_delegations
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN QUERY (
    SELECT 
      (SELECT av.name FROM hive.accounts_view av WHERE av.id = d.delegatee)::TEXT AS delegatee,
      d.balance::TEXT AS amount,
      d.source_op::TEXT AS operation_id,
      d.source_op_block AS block_num 
    FROM btracker_backend.current_accounts_delegations_view d
    WHERE delegator = _account_id
  );
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.incoming_recurrent_transfers(IN _account_id INT)
RETURNS SETOF btracker_backend.incoming_recurrent_transfers
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN QUERY (
    SELECT 
      (SELECT av.name FROM hive.accounts_view av WHERE av.id = d.from_account)::TEXT AS from,
      d.transfer_id AS pair_id,
      btracker_backend.amount_object(d.nai, d.amount) AS amount,
      d.consecutive_failures AS consecutive_failures,
      d.remaining_executions AS remaining_executions,
      d.recurrence AS recurrence,
      d.memo AS memo,
      bv.created_at + (d.recurrence::TEXT || 'hour')::INTERVAL  AS trigger_date,
      d.source_op::TEXT AS operation_id,
      d.source_op_block AS block_num
    FROM btracker_backend.recurrent_transfers_view d
    JOIN hive.blocks_view bv ON bv.num = d.source_op_block
    WHERE to_account = _account_id
  );
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.outgoing_recurrent_transfers(IN _account_id INT)
RETURNS SETOF btracker_backend.outgoing_recurrent_transfers
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN QUERY (
    SELECT 
      (SELECT av.name FROM hive.accounts_view av WHERE av.id = d.to_account)::TEXT AS to,
      d.transfer_id AS pair_id,
      btracker_backend.amount_object(d.nai, d.amount) AS amount,
      d.consecutive_failures AS consecutive_failures,
      d.remaining_executions AS remaining_executions,
      d.recurrence AS recurrence,
      d.memo AS memo,
      bv.created_at + (d.recurrence::TEXT || 'hour')::INTERVAL  AS trigger_date,
      d.source_op::TEXT AS operation_id,
      d.source_op_block AS block_num 
    FROM btracker_backend.recurrent_transfers_view d
    JOIN hive.blocks_view bv ON bv.num = d.source_op_block
    WHERE from_account = _account_id
  );
END
$$;

RESET ROLE;

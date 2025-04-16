SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_backend.get_acc_balances(IN _account_id INT)
RETURNS btracker_backend.balance
LANGUAGE plpgsql
STABLE
AS
$BODY$
BEGIN
  RETURN (
    WITH get_delegations AS
    (
      SELECT 
        COALESCE(ad.delegated_vests::TEXT, '0') AS delegated_vests,
        COALESCE(ad.received_vests::TEXT, '0') AS received_vests
      FROM (
        SELECT 
          NULL::TEXT AS delegated_vests,
          NULL::TEXT AS received_vests
      ) default_values
      LEFT JOIN account_delegations ad ON ad.account = _account_id
    ),
    get_balances AS MATERIALIZED
    (
      SELECT  
        MAX(CASE WHEN cab.nai = 13 THEN cab.balance END) AS hbd_balance,
        MAX(CASE WHEN cab.nai = 21 THEN cab.balance END) AS hive_balance, 
        MAX(CASE WHEN cab.nai = 37 THEN cab.balance END) AS vesting_shares 
      FROM current_account_balances cab
      WHERE cab.account= _account_id
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
      LEFT JOIN account_info_rewards air ON air.account = _account_id
    ),
    calculate_rewards AS MATERIALIZED
    (
      SELECT  
        MAX(CASE WHEN ar.nai = 13 THEN ar.balance END) AS hbd_rewards,
        MAX(CASE WHEN ar.nai = 21 THEN ar.balance END) AS hive_rewards,
        MAX(CASE WHEN ar.nai = 37 THEN ar.balance END) AS vests_rewards,
        MAX(CASE WHEN ar.nai = 38 THEN ar.balance END) AS hive_vesting_rewards
      FROM account_rewards ar
      WHERE ar.account= _account_id
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
      WHERE ats.account= _account_id
    ),
    get_withdraw_requests AS
    (
      SELECT SUM(ats.savings_withdraw_requests) AS total
      FROM account_savings ats
      WHERE ats.account= _account_id
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
      LEFT JOIN account_withdraws aw ON aw.account = _account_id
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
    )::btracker_backend.balance
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

END;
$BODY$;

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
    FROM current_accounts_delegations d
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
    FROM current_accounts_delegations d
    WHERE delegator = _account_id
  );
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.balance_history(
    _account_id INT,
    _coin_type INT,
    _page INT,
    _page_size INT,
    _order_is btracker_backend.sort_direction,
    _from_op BIGINT,
    _to_op BIGINT
)
RETURNS SETOF btracker_backend.balance_history
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  _offset INT := (((_page - 1) * _page_size) + 1);
BEGIN
  RETURN QUERY (
    WITH ranked_rows AS
    (
    ---------paging----------
      SELECT 
        source_op_block,
        source_op, 
        ROW_NUMBER() OVER ( ORDER BY
          (CASE WHEN _order_is = 'desc' THEN source_op ELSE NULL END) DESC,
          (CASE WHEN _order_is = 'asc' THEN source_op ELSE NULL END) ASC
        ) as row_num
      FROM account_balance_history
      WHERE 
        account = _account_id AND nai = _coin_type AND
        source_op < _to_op AND
        source_op >= _from_op
    ),
    filter_params AS MATERIALIZED
    (
      SELECT 
        source_op AS filter_op
      FROM ranked_rows
      WHERE row_num = _offset
    ),
    --------------------------
    --history with extra row--
    gather_page AS MATERIALIZED
    (
      SELECT 
        ab.account,
        ab.nai,
        ab.balance,
        ab.source_op,
        ab.source_op_block,
        ROW_NUMBER() OVER (ORDER BY
          (CASE WHEN _order_is = 'desc' THEN ab.source_op ELSE NULL END) DESC,
          (CASE WHEN _order_is = 'asc' THEN ab.source_op ELSE NULL END) ASC
        ) as row_num
      FROM
        account_balance_history ab, filter_params fp
      WHERE 
        ab.account = _account_id AND 
        ab.nai = _coin_type AND
        (_order_is = 'asc' OR ab.source_op <= fp.filter_op) AND
        (_order_is = 'desc' OR ab.source_op >= fp.filter_op) AND
        ab.source_op < _to_op AND
        ab.source_op >= _from_op
      LIMIT _page_size + 1
    ),
    --------------------------
    --calculate prev balance--
    join_prev_balance AS MATERIALIZED
    (
      SELECT 
        ab.source_op_block,
        ab.source_op,
        ab.balance,
        jab.balance AS prev_balance
      FROM gather_page ab
      LEFT JOIN gather_page jab ON 
        (CASE WHEN _order_is = 'desc' THEN jab.row_num - 1 ELSE jab.row_num + 1 END) = ab.row_num
      LIMIT _page_size 
    ),
    add_prevbal_to_row_over_range AS
    (
      SELECT 
        jpb.source_op_block,
        jpb.source_op,
        jpb.balance,
        COALESCE(
          (
            CASE WHEN jpb.prev_balance IS NULL THEN
              (
                SELECT abh.balance FROM account_balance_history abh
                WHERE 
                  abh.source_op < jpb.source_op AND
                  abh.account = _account_id AND 
                  abh.nai = _coin_type 
                ORDER BY abh.source_op DESC
                LIMIT 1
              )
            ELSE
              jpb.prev_balance
            END
          ), 0
        ) AS prev_balance
      FROM join_prev_balance jpb
    )
    --------------------------
    SELECT 
      ab.source_op_block,
      ab.source_op::TEXT,
      ov.op_type_id,
      ab.balance::TEXT,
      ab.prev_balance::TEXT,
      (ab.balance - ab.prev_balance)::TEXT,
      bv.created_at
    FROM add_prevbal_to_row_over_range ab
    JOIN hive.blocks_view bv ON bv.num = ab.source_op_block
    JOIN hive.operations_view ov ON ov.id = ab.source_op
  );
END
$$;

RESET ROLE;

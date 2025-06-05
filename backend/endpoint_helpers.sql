SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_backend.get_acc_balances(
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
    get_delegations AS (
      SELECT
        COALESCE(ad.delegated_vests, 0)   AS delegated_vests,
        COALESCE(ad.received_vests,  0)   AS received_vests
      FROM (VALUES (1)) AS dummy(_)
      LEFT JOIN account_delegations ad
        ON ad.account = _account_id
    ),

    get_balances AS MATERIALIZED (
      SELECT
        MAX(CASE WHEN cab.nai = 13 THEN cab.balance END)::BIGINT   AS hbd_balance,
        MAX(CASE WHEN cab.nai = 21 THEN cab.balance END)::BIGINT   AS hive_balance,
        MAX(CASE WHEN cab.nai = 37 THEN cab.balance END)::TEXT     AS vesting_shares
      FROM current_account_balances cab
      WHERE cab.account = _account_id
    ),

    get_vest_balance AS (
      SELECT
        hive.get_vesting_balance(
          bv.num::INTEGER,
          (gb.vesting_shares)::BIGINT
        )::BIGINT AS vesting_balance_hive
      FROM get_balances gb
      JOIN LATERAL (
        SELECT num
        FROM hive.blocks_view
        ORDER BY num DESC
        LIMIT 1
      ) bv ON TRUE
    ),

    get_post_voting_power AS (
      SELECT
        (
          gb.vesting_shares::NUMERIC
          - gd.delegated_vests::NUMERIC
          + gd.received_vests::NUMERIC
        )::TEXT AS post_voting_power_vests
      FROM get_balances gb
      CROSS JOIN get_delegations gd
    ),

    get_info_rewards AS (
      SELECT
        COALESCE(air.curation_rewards, 0)::TEXT AS curation_rewards,
        COALESCE(air.posting_rewards, 0)::TEXT AS posting_rewards
      FROM (VALUES (1)) AS dummy(_)
      LEFT JOIN account_info_rewards air
        ON air.account = _account_id
    ),

    calculate_rewards AS MATERIALIZED (
      SELECT
        MAX(CASE WHEN ar.nai = 13 THEN ar.balance END)::BIGINT   AS hbd_rewards,
        MAX(CASE WHEN ar.nai = 21 THEN ar.balance END)::BIGINT   AS hive_rewards,
        MAX(CASE WHEN ar.nai = 37 THEN ar.balance END)::TEXT     AS vests_rewards,
        MAX(CASE WHEN ar.nai = 38 THEN ar.balance END)::BIGINT   AS hive_vesting_rewards
      FROM account_rewards ar
      WHERE ar.account = _account_id
    ),
    get_rewards AS (
      SELECT
        COALESCE(cr.hbd_rewards, 0)        AS hbd_rewards,
        COALESCE(cr.hive_rewards, 0)       AS hive_rewards,
        COALESCE(cr.vests_rewards, '0')    AS vests_rewards,
        COALESCE(cr.hive_vesting_rewards, 0)::BIGINT AS hive_vesting_rewards
      FROM calculate_rewards cr
    ),

    calculate_savings AS MATERIALIZED (
      SELECT
        MAX(CASE WHEN ats.nai = 13 THEN ats.saving_balance END)::BIGINT AS hbd_savings,
        MAX(CASE WHEN ats.nai = 21 THEN ats.saving_balance END)::BIGINT AS hive_savings
      FROM account_savings ats
      WHERE ats.account = _account_id
    ),
    get_withdraw_requests AS (
      SELECT
        COALESCE(SUM(ats.savings_withdraw_requests), 0)::INT AS total
      FROM account_savings ats
      WHERE ats.account = _account_id
    ),
    get_savings AS (
      SELECT
        COALESCE(cs.hbd_savings, 0)               AS hbd_savings,
        COALESCE(cs.hive_savings, 0)              AS hive_savings,
        COALESCE(gwr.total, 0)                    AS savings_withdraw_requests
      FROM calculate_savings cs
      CROSS JOIN get_withdraw_requests gwr
    ),

    get_withdraws AS (
      SELECT
        COALESCE(aw.vesting_withdraw_rate::TEXT, '')   AS vesting_withdraw_rate,
        COALESCE(aw.to_withdraw::TEXT, '')             AS to_withdraw,
        COALESCE(aw.withdrawn::TEXT, '')               AS withdrawn,
        COALESCE(aw.withdraw_routes, 0)::INT           AS withdraw_routes,
        COALESCE(aw.delayed_vests::TEXT, '')           AS delayed_vests
      FROM (VALUES (1)) AS dummy(_)
      LEFT JOIN account_withdraws aw
        ON aw.account = _account_id
    ),

    conv_pivot AS (
      SELECT
        COALESCE(MAX(CASE WHEN asset = 'HBD' THEN total_amount END), 0)::NUMERIC   AS conversion_pending_amount_hbd,
        COALESCE(MAX(CASE WHEN asset = 'HBD' THEN request_count END), 0)::INT      AS conversion_pending_count_hbd,
        COALESCE(MAX(CASE WHEN asset = 'HIVE' THEN total_amount END), 0)::NUMERIC  AS conversion_pending_amount_hive,
        COALESCE(MAX(CASE WHEN asset = 'HIVE' THEN request_count END), 0)::INT     AS conversion_pending_count_hive
      FROM btracker_app.account_pending_converts apc
      WHERE apc.account_name = (
        SELECT name
        FROM hive.accounts_view
        WHERE id = _account_id
      )
    ),

    open_orders AS (
      SELECT
        COALESCE(MAX(aoos.open_orders_hbd_count),  0)::INT    AS open_orders_hbd_count,
        COALESCE(MAX(aoos.open_orders_hive_count), 0)::INT    AS open_orders_hive_count,
        COALESCE(MAX(aoos.open_orders_hive_amount),0)::NUMERIC AS open_orders_hive_amount,
        COALESCE(MAX(aoos.open_orders_hbd_amount), 0)::NUMERIC AS open_orders_hbd_amount
      FROM btracker_app.account_open_orders_summary aoos
      WHERE aoos.account_name = (
        SELECT name
        FROM hive.accounts_view
        WHERE id = _account_id
      )
    )

  SELECT
    gb.hbd_balance,
    gb.hive_balance,
    gb.vesting_shares,
    gvb.vesting_balance_hive,
    gpvp.post_voting_power_vests,
    gd.delegated_vests,
    gd.received_vests,
    ir.curation_rewards,
    ir.posting_rewards,
    gr.hbd_rewards,
    gr.hive_rewards,
    gr.vests_rewards,
    gr.hive_vesting_rewards,
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
    oo.open_orders_hbd_amount
  INTO result_row
  FROM get_balances       gb
  CROSS JOIN get_vest_balance      gvb
  CROSS JOIN get_post_voting_power gpvp
  CROSS JOIN get_delegations       gd
  CROSS JOIN get_info_rewards      ir
  CROSS JOIN get_rewards           gr
  CROSS JOIN get_savings           sv
  CROSS JOIN get_withdraws         wd
  CROSS JOIN conv_pivot            cp
  CROSS JOIN open_orders           oo;

  RETURN result_row;
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
    FROM recurrent_transfers d
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
    FROM recurrent_transfers d
    JOIN hive.blocks_view bv ON bv.num = d.source_op_block
    WHERE from_account = _account_id
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

CREATE OR REPLACE FUNCTION btracker_backend.savings_history(
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
      FROM account_savings_history
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
        ab.saving_balance,
        ab.source_op,
        ab.source_op_block,
        ROW_NUMBER() OVER (ORDER BY
          (CASE WHEN _order_is = 'desc' THEN ab.source_op ELSE NULL END) DESC,
          (CASE WHEN _order_is = 'asc' THEN ab.source_op ELSE NULL END) ASC
        ) as row_num
      FROM
        account_savings_history ab, filter_params fp
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
        ab.saving_balance,
        jab.saving_balance AS prev_balance
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
        jpb.saving_balance,
        COALESCE(
          (
            CASE WHEN jpb.prev_balance IS NULL THEN
              (
                SELECT abh.saving_balance FROM account_savings_history abh
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
      ab.saving_balance::TEXT,
      ab.prev_balance::TEXT,
      (ab.saving_balance - ab.prev_balance)::TEXT,
      bv.created_at
    FROM add_prevbal_to_row_over_range ab
    JOIN hive.blocks_view bv ON bv.num = ab.source_op_block
    JOIN hive.operations_view ov ON ov.id = ab.source_op
  );
END
$$;



RESET ROLE;

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

    -- 9) Pending‐converts summary, dynamic truncation
    conv_pivot AS (
      WITH
        ops AS (
          SELECT block_num, op_type, request_id, nai, amount
          FROM account_convert_operations
          WHERE account_name = (
            SELECT name::TEXT FROM hive.accounts_view WHERE id = _account_id
          )
        ),
        last_fill AS (
          SELECT
            request_id,
            MAX(block_num) AS last_fill_block
          FROM ops
          WHERE op_type = 'fill'
          GROUP BY request_id
        ),
        requests AS (
          SELECT
            request_id,
            block_num AS request_block,
            nai,
            amount
          FROM ops
          WHERE op_type = 'convert'
        ),
        open_requests AS (
          SELECT r.*
          FROM requests r
          LEFT JOIN last_fill lf USING (request_id)
          WHERE r.request_block > COALESCE(lf.last_fill_block,0)
        )
      SELECT
        -- dynamically drop trailing zeros from the text repr,
        -- then cast back to numeric
        COALESCE(
          TRIM(TRAILING '.' 
            FROM TRIM(TRAILING '0'
              FROM (SUM(amount) FILTER (WHERE nai='@@000000013'))::TEXT
            )
          )::NUMERIC,
        0) AS conversion_pending_amount_hbd,
        COALESCE(
          COUNT(*) FILTER (WHERE nai='@@000000013'),
        0)::INT AS conversion_pending_count_hbd,
        COALESCE(
          TRIM(TRAILING '.'
            FROM TRIM(TRAILING '0'
              FROM (SUM(amount) FILTER (WHERE nai='@@000000021'))::TEXT
            )
          )::NUMERIC,
        0) AS conversion_pending_amount_hive,
        COALESCE(
          COUNT(*) FILTER (WHERE nai='@@000000021'),
        0)::INT AS conversion_pending_count_hive
      FROM open_requests
    ),

    -- 10) Open‐orders summary, dynamic truncation
    open_orders AS (
  WITH
    /* 1) pull only create / fill / cancel rows for this account */
    ops AS (
      SELECT block_num, op_type, order_id, nai, amount
      FROM account_operations
      WHERE account_name = (
              SELECT name::TEXT
              FROM hive.accounts_view
              WHERE id = _account_id
            )
        AND op_type IN ('create','fill','cancel')
    ),

    /* 2) last cancel per order */
    last_cancel AS (
      SELECT order_id,
             MAX(block_num) AS last_cancel_block
      FROM ops
      WHERE op_type = 'cancel'
      GROUP BY order_id
    ),

    /* 3) creates and fills */
    creates AS (
      SELECT order_id,
             block_num AS create_block,
             amount    AS create_amount,
             nai
      FROM ops
      WHERE op_type = 'create'
    ),
    fills AS (
      SELECT order_id,
             amount    AS fill_amt,
             block_num,
             nai
      FROM ops
      WHERE op_type = 'fill'
    ),

    /* 4) only creates that have not been cancelled after their block */
    live_creates AS (
      SELECT c.*
      FROM creates c
      LEFT JOIN last_cancel lc USING (order_id)
      WHERE c.create_block > COALESCE(lc.last_cancel_block, 0)
    ),

    /* 5) sum fills that happened after the last cancel */
    agg_fills AS (
      SELECT f.order_id,
             f.nai,
             SUM(f.fill_amt) AS total_filled
      FROM fills f
      LEFT JOIN last_cancel lc USING (order_id)
      WHERE f.block_num > COALESCE(lc.last_cancel_block, 0)
      GROUP BY f.order_id, f.nai
    ),

    /* 6) create-vs-fill delta per order/asset */
    cri AS (
      SELECT
        lc.order_id,
        lc.create_block,
        lc.create_amount,
        COALESCE(af.total_filled, 0)                    AS total_filled,
        lc.create_amount - COALESCE(af.total_filled, 0) AS amount_remaining,
        lc.nai
      FROM live_creates lc
      LEFT JOIN agg_fills af
        ON af.order_id = lc.order_id
       AND af.nai      = lc.nai
    )

  /* 7) final per-account summary  */
  SELECT
    /* counts (positive remainders only) */
    COUNT(*) FILTER (
      WHERE cri.amount_remaining > 0
        AND cri.nai = '@@000000013'
    )::INT                                          AS open_orders_hbd_count,

    COUNT(*) FILTER (
      WHERE cri.amount_remaining > 0
        AND cri.nai = '@@000000021'
    )::INT                                          AS open_orders_hive_count,

    /* HIVE amount (positive remainders only) */
    COALESCE(
      CASE
        WHEN (SUM(cri.amount_remaining)
               FILTER (WHERE cri.amount_remaining > 0
                        AND cri.nai = '@@000000021')
             )::TEXT LIKE '%.%'
        THEN
          TRIM(TRAILING '.'
            FROM TRIM(TRAILING '0'
              FROM (SUM(cri.amount_remaining)
                     FILTER (WHERE cri.amount_remaining > 0
                              AND cri.nai = '@@000000021')
                   )::TEXT
            )
          )::NUMERIC
        ELSE
          SUM(cri.amount_remaining)
            FILTER (WHERE cri.amount_remaining > 0
                     AND cri.nai = '@@000000021')
      END,
      0
    )                                              AS open_orders_hive_amount,

    /* HBD amount (positive remainders only) */
    COALESCE(
      CASE
        WHEN (SUM(cri.amount_remaining)
               FILTER (WHERE cri.amount_remaining > 0
                        AND cri.nai = '@@000000013')
             )::TEXT LIKE '%.%'
        THEN
          TRIM(TRAILING '.'
            FROM TRIM(TRAILING '0'
              FROM (SUM(cri.amount_remaining)
                     FILTER (WHERE cri.amount_remaining > 0
                              AND cri.nai = '@@000000013')
                   )::TEXT
            )
          )::NUMERIC
        ELSE
          SUM(cri.amount_remaining)
            FILTER (WHERE cri.amount_remaining > 0
                     AND cri.nai = '@@000000013')
      END,
      0
    )                                              AS open_orders_hbd_amount
  FROM cri
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
    oo.open_orders_hbd_amount
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
  CROSS JOIN open_orders           oo;

  RETURN result_row;
END;
$BODY$;

RESET ROLE;

CREATE OR REPLACE FUNCTION btracker_backend.get_top_holders(
    _coin_type    INT,
    _balance_type btracker_backend.balance_type,
    _page         INT,
    _limit        INT
)
RETURNS TABLE(
    rank    INT,
    account TEXT,
    value   NUMERIC(38,0)        -- zero scale numeric
)
LANGUAGE plpgsql
STABLE
AS
$$
DECLARE
  _offset INT := (_page - 1) * _limit;
BEGIN
  IF _balance_type = 'balance' THEN
    RETURN QUERY
      --first gather top holders ordered by balance and name
      --this query uses index (nai, balance) to speed up the process
      WITH ordered_holders AS MATERIALIZED (
        SELECT
          av.name,
          src.balance
        FROM btracker_backend.current_account_balances_view AS src
        JOIN hive.accounts_view av ON av.id = src.account
        WHERE src.nai = _coin_type
        ORDER BY src.balance DESC, av.name ASC
        OFFSET _offset
        LIMIT _limit
      )
      --to already found records add row number
      SELECT
        (
          ROW_NUMBER() OVER (ORDER BY o.balance DESC, o.name ASC) + _offset
        )::INT AS rank,
        o.name::TEXT,
        o.balance::NUMERIC(38,0)
      FROM ordered_holders o;

  ELSIF _balance_type = 'savings_balance' THEN
    RETURN QUERY
      WITH ordered_holders AS MATERIALIZED (
        SELECT
          av.name,
          src.balance
        FROM btracker_backend.account_savings_view AS src
        JOIN hive.accounts_view av ON av.id = src.account
        WHERE src.nai = _coin_type
        ORDER BY src.balance DESC, av.name ASC
        OFFSET _offset
        LIMIT _limit
      )
      SELECT
        (
          ROW_NUMBER() OVER (ORDER BY o.balance DESC, o.name ASC) + _offset
        )::INT AS rank,
        o.name::TEXT,
        o.balance::NUMERIC(38,0)
      FROM ordered_holders o;

  ELSE
    RAISE EXCEPTION 'Unsupported balance type: %', _balance_type;
  END IF;
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

RESET ROLE;

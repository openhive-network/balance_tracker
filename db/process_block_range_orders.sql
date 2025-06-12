-- 1) Drop any existing version
DROP FUNCTION IF EXISTS btracker_app.process_block_range_orders(INT, INT) CASCADE;

-- 2) Create the corrected “delta” version in place of the old function
CREATE OR REPLACE FUNCTION btracker_app.process_block_range_orders(
    IN _from_block INT,
    IN _to_block   INT
)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
  __upserted INT;
  __closed   INT;
BEGIN
  WITH raw_ops AS (
    SELECT
      ov.body  ::jsonb AS body,
      ot.name         AS op_name
    FROM hive.operations_view ov
    JOIN hafd.operation_types ot
      ON ot.id = ov.op_type_id
    WHERE ov.block_num BETWEEN _from_block AND _to_block
      AND ot.name IN (
        'hive::protocol::limit_order_create_operation',
        'hive::protocol::fill_order_operation',
        'hive::protocol::limit_order_cancelled_operation'
      )
  ),

  -- 1) New opens in this slice
  adds AS (
    SELECT
      (body->'value'->>'owner')::TEXT           AS account_name,
      (body->'value'->>'orderid')::BIGINT       AS order_id,
      (body->'value'->'amount_to_sell'->>'nai') AS nai,
      (body->'value'->'amount_to_sell'->>'amount')::NUMERIC
        / POWER(10, (body->'value'->'amount_to_sell'->>'precision')::INT)
        AS amount
    FROM raw_ops
    WHERE op_name = 'hive::protocol::limit_order_create_operation'
      AND (body->'value'->>'orderid') IS NOT NULL
      -- exclude any that get closed in this same slice
      AND NOT EXISTS (
        SELECT 1
          FROM raw_ops r2
         WHERE r2.op_name IN (
           'hive::protocol::fill_order_operation',
           'hive::protocol::limit_order_cancelled_operation'
         )
           AND (
             (r2.body->'value'->>'open_orderid')     = (body->'value'->>'orderid')
             OR (r2.body->'value'->>'current_orderid')= (body->'value'->>'orderid')
             OR (r2.body->'value'->>'orderid')        = (body->'value'->>'orderid')
           )
      )
  ),

  -- 2) All fills/cancels in this slice that close any order
  closes AS (
    SELECT
      COALESCE(
        (body->'value'->>'open_orderid'),
        (body->'value'->>'current_orderid'),
        (body->'value'->>'orderid')
      )::BIGINT AS order_id,
      COALESCE(
        (body->'value'->>'open_owner'),
        (body->'value'->>'current_owner'),
        (body->'value'->>'seller')
      )::TEXT AS account_name,
      COALESCE(
        (body->'value'->'open_pays'   ->>'nai'),
        (body->'value'->'current_pays'->>'nai'),
        '@@000000013'
      ) AS nai,
      0::NUMERIC AS amount
    FROM raw_ops
    WHERE op_name IN (
      'hive::protocol::fill_order_operation',
      'hive::protocol::limit_order_cancelled_operation'
    )
  ),

  -- 3) Net delta per account
  delta AS (
    SELECT
      account_name,
      SUM(CASE WHEN nai = '@@000000013' THEN  1 ELSE 0 END) AS hbd_cnt_delta,
      SUM(CASE WHEN nai = '@@000000013' THEN amount ELSE 0 END) AS hbd_amt_delta,
      SUM(CASE WHEN nai = '@@000000021' THEN  1 ELSE 0 END) AS hive_cnt_delta,
      SUM(CASE WHEN nai = '@@000000021' THEN amount ELSE 0 END) AS hive_amt_delta
    FROM (
      SELECT * FROM adds
      UNION ALL
      SELECT * FROM closes
    ) u
    GROUP BY account_name
  ),

  -- 4) Ensure each account exists
  upsert AS (
    INSERT INTO btracker_app.account_open_orders_summary (
      account_name,
      open_orders_hbd_count,
      open_orders_hbd_amount,
      open_orders_hive_count,
      open_orders_hive_amount
    )
    SELECT account_name, 0, 0, 0, 0
      FROM delta
    ON CONFLICT (account_name) DO NOTHING
    RETURNING 1
  ),

  -- 5) Apply the delta
  apply AS (
    UPDATE btracker_app.account_open_orders_summary dst
      SET
        open_orders_hbd_count   = GREATEST(0, dst.open_orders_hbd_count   + d.hbd_cnt_delta),
        open_orders_hbd_amount  = GREATEST(0, dst.open_orders_hbd_amount  + d.hbd_amt_delta),
        open_orders_hive_count  = GREATEST(0, dst.open_orders_hive_count  + d.hive_cnt_delta),
        open_orders_hive_amount = GREATEST(0, dst.open_orders_hive_amount + d.hive_amt_delta)
     FROM delta d
    WHERE dst.account_name = d.account_name
    RETURNING 1
  )

  -- 6) Capture results into PL/pgSQL vars
  SELECT
    (SELECT COUNT(*) FROM upsert),
    (SELECT COUNT(*) FROM apply)
  INTO __upserted, __closed;

  RAISE NOTICE
    'Blocks %–%: new accounts %, updated accounts %',
    _from_block, _to_block, __upserted, __closed;
END;
$$;

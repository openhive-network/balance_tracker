SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_app.process_block_range_open_orders(
  IN _from_block INT,
  IN _to_block   INT
)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
  c_inserts        INT;
  c_closed_cancels INT;
  c_closed_fills   INT;
  c_summary        INT;
  c_purged         INT;
BEGIN
  WITH raw_ops AS MATERIALIZED (
    SELECT
      ov.body::jsonb   AS body,
      ot.name          AS op_name
    FROM hive.operations_view ov
    JOIN hafd.operation_types ot
      ON ot.id = ov.op_type_id
    WHERE ov.block_num BETWEEN _from_block AND _to_block
      AND ot.name IN (
        'hive::protocol::limit_order_create_operation',
        'hive::protocol::fill_order_operation',
        'hive::protocol::limit_order_cancel_operation',
        'hive::protocol::limit_order_cancelled_operation'
      )
  ),
  creates AS MATERIALIZED (
    SELECT
      (body->'value'->>'owner')::TEXT    AS acct,
      (body->'value'->>'orderid')::BIGINT AS id,
      (body->'value'->'amount_to_sell'->>'nai')   AS nai,
      ((body->'value'->'amount_to_sell'->>'amount')::NUMERIC
         / POWER(10,
             (body->'value'->'amount_to_sell'->>'precision')::INT
           )
      )                                           AS amt
    FROM raw_ops
    WHERE op_name = 'hive::protocol::limit_order_create_operation'
      AND (body->'value'->>'orderid') IS NOT NULL
  ),
  cancels AS MATERIALIZED (
    SELECT
      (body->'value'->>'orderid')::BIGINT           AS id,
      COALESCE(
        body->'value'->>'seller',
        body->'value'->>'owner'
      )::TEXT                                        AS acct
    FROM raw_ops
    WHERE op_name IN (
      'hive::protocol::limit_order_cancel_operation',
      'hive::protocol::limit_order_cancelled_operation'
    )
      AND (body->'value'->>'orderid') IS NOT NULL
  ),
  fills AS MATERIALIZED (
    SELECT
      (body->'value'->>'open_owner')   ::TEXT   AS acct,
      (body->'value'->>'open_orderid') ::BIGINT AS id,
      (body->'value'->'pays'->>'nai')           AS nai
    FROM raw_ops
    WHERE op_name = 'hive::protocol::fill_order_operation'
      AND (body->'value'->>'open_orderid') IS NOT NULL

    UNION ALL

    SELECT
      (body->'value'->>'current_owner')   ::TEXT   AS acct,
      (body->'value'->>'current_orderid') ::BIGINT AS id,
      (body->'value'->'receives'->>'nai')       AS nai
    FROM raw_ops
    WHERE op_name = 'hive::protocol::fill_order_operation'
      AND (body->'value'->>'current_orderid') IS NOT NULL
  ),
  ins AS (
    INSERT INTO btracker_app.open_orders_detail
      (account_name, order_id, nai, amount)
    SELECT acct, id, nai, amt
      FROM creates
    ON CONFLICT DO NOTHING
    RETURNING 1
  ),
  del_c AS (
    DELETE FROM btracker_app.open_orders_detail d
    USING cancels c
    WHERE d.account_name = c.acct
      AND d.order_id     = c.id
    RETURNING 1
  ),
  del_f AS (
    DELETE FROM btracker_app.open_orders_detail d
    USING fills f
    WHERE d.account_name = f.acct
      AND d.order_id     = f.id
      AND d.nai          = f.nai
    RETURNING 1
  ),
  snap AS (
    SELECT
      account_name,
      COUNT(*) FILTER (WHERE nai = '@@000000013')               AS open_orders_hbd_count,
      COALESCE(SUM(amount) FILTER (WHERE nai = '@@000000013'),0) AS open_orders_hbd_amount,
      COUNT(*) FILTER (WHERE nai = '@@000000021')               AS open_orders_hive_count,
      COALESCE(SUM(amount) FILTER (WHERE nai = '@@000000021'),0) AS open_orders_hive_amount
    FROM btracker_app.open_orders_detail
    GROUP BY account_name
  ),
  up_summary AS (
    INSERT INTO btracker_app.account_open_orders_summary (
      account_name,
      open_orders_hbd_count, open_orders_hbd_amount,
      open_orders_hive_count, open_orders_hive_amount
    )
    SELECT
      account_name,
      open_orders_hbd_count, open_orders_hbd_amount,
      open_orders_hive_count, open_orders_hive_amount
    FROM snap
    ON CONFLICT (account_name) DO UPDATE
      SET
        open_orders_hbd_count   = EXCLUDED.open_orders_hbd_count,
        open_orders_hbd_amount  = EXCLUDED.open_orders_hbd_amount,
        open_orders_hive_count  = EXCLUDED.open_orders_hive_count,
        open_orders_hive_amount = EXCLUDED.open_orders_hive_amount
    RETURNING 1
  ),
  purge AS (
    DELETE FROM btracker_app.account_open_orders_summary s
    WHERE s.open_orders_hbd_count  = 0
      AND s.open_orders_hive_count = 0
    RETURNING 1
  )
  SELECT
    (SELECT COUNT(*) FROM ins),
    (SELECT COUNT(*) FROM del_c),
    (SELECT COUNT(*) FROM del_f),
    (SELECT COUNT(*) FROM up_summary),
    (SELECT COUNT(*) FROM purge)
  INTO
    c_inserts,
    c_closed_cancels,
    c_closed_fills,
    c_summary,
    c_purged;

  RAISE NOTICE
    'Blocks %–% | new=% | cancels=% | fills=% | summary=% | purged=%',
    _from_block, _to_block,
    c_inserts,
    c_closed_cancels,
    c_closed_fills,
    c_summary,
    c_purged;
END;
$$;

RESET ROLE;

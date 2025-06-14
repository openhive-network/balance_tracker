SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_app.process_block_range_orders(
    IN _from_block INT,
    IN _to_block   INT
)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
  c_new_creates     INT;
  c_closed_rows     INT;
  c_summary_upserts INT;
  c_new_cancel_ids  INT;
  c_purged          INT;
BEGIN
  WITH
  /* ─────────────────────────────────────────────────────────────── */
  /* 1.  pull create / fill / cancel ops in this slice              */
  /* ─────────────────────────────────────────────────────────────── */
  raw AS (
    SELECT ov.body::jsonb AS body,
           ot.name        AS op
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot ON ot.id = ov.op_type_id
     WHERE ov.block_num BETWEEN _from_block AND _to_block
       AND ot.name IN (
         'hive::protocol::limit_order_create_operation',
         'hive::protocol::fill_order_operation',
         'hive::protocol::limit_order_cancel_operation',
         'hive::protocol::limit_order_cancelled_operation'
       )
  ),

  /* 2.  tidy creates                                               */
  creates AS (
    SELECT
      (body->'value'->>'owner')     ::TEXT    AS acct,
      (body->'value'->>'orderid')   ::BIGINT  AS id,
      (body->'value'->'amount_to_sell'->>'nai')       AS nai,
      (body->'value'->'amount_to_sell'->>'amount')::NUMERIC
        / POWER(10,(body->'value'->'amount_to_sell'->>'precision')::INT) AS amt
    FROM raw
    WHERE op = 'hive::protocol::limit_order_create_operation'
  ),

  /* 3.  fills – we need owner + id for BOTH sides                  */
  fills AS (
    SELECT (body->'value'->>'open_owner')   ::TEXT   AS acct,
           (body->'value'->>'open_orderid') ::BIGINT AS id
    FROM raw
    WHERE op = 'hive::protocol::fill_order_operation'
      AND body->'value'->>'open_orderid' IS NOT NULL
    UNION ALL
    SELECT (body->'value'->>'current_owner')   ::TEXT,
           (body->'value'->>'current_orderid') ::BIGINT
    FROM raw
    WHERE op = 'hive::protocol::fill_order_operation'
      AND body->'value'->>'current_orderid' IS NOT NULL
  ),

  /* 4.  cancels – include BOTH op-types                            */
  cancels AS (
    SELECT
      (body->'value'->>'orderid')::BIGINT                                 AS id,
      COALESCE(body->'value'->>'seller', body->'value'->>'owner')::TEXT   AS acct
    FROM raw
    WHERE op IN (
      'hive::protocol::limit_order_cancel_operation',
      'hive::protocol::limit_order_cancelled_operation'
    )
  ),

  /* 5.  insert new creates                                          */
  ins AS (
    INSERT INTO btracker_app.open_orders_detail (account_name, order_id, nai, amount)
    SELECT acct, id, nai, amt
    FROM   creates
    ON CONFLICT DO NOTHING
    RETURNING 1
  ),

  /* 6.  delete rows closed by cancel                                */
  del_cancel AS (
    DELETE FROM btracker_app.open_orders_detail d
    USING cancels c
    WHERE d.account_name = c.acct
      AND d.order_id     = c.id
    RETURNING 1
  ),

  /* 7.  delete rows closed by fill                                  */
  del_fill AS (
    DELETE FROM btracker_app.open_orders_detail d
    USING fills f
    WHERE d.account_name = f.acct
      AND d.order_id     = f.id
    RETURNING 1
  ),

  closed_rows AS (
    SELECT 1 FROM del_cancel
    UNION ALL
    SELECT 1 FROM del_fill
  ),

  /* 8. fresh per-account summary                                    */
  snap AS (
    SELECT
      account_name,
      COUNT(*) FILTER (WHERE nai='@@000000013')                 AS hbd_cnt,
      COALESCE(SUM(amount) FILTER (WHERE nai='@@000000013'),0)  AS hbd_amt,
      COUNT(*) FILTER (WHERE nai='@@000000021')                 AS hive_cnt,
      COALESCE(SUM(amount) FILTER (WHERE nai='@@000000021'),0)  AS hive_amt
    FROM btracker_app.open_orders_detail
    GROUP BY account_name
  ),

  /* 9. upsert summary                                               */
  up_summary AS (
    INSERT INTO btracker_app.account_open_orders_summary (
       account_name,
       open_orders_hbd_count,
       open_orders_hbd_amount,
       open_orders_hive_count,
       open_orders_hive_amount
    )
    SELECT
      account_name,
      hbd_cnt,  hbd_amt,
      hive_cnt, hive_amt
    FROM snap
    ON CONFLICT (account_name) DO UPDATE
      SET open_orders_hbd_count   = EXCLUDED.open_orders_hbd_count,
          open_orders_hbd_amount  = EXCLUDED.open_orders_hbd_amount,
          open_orders_hive_count  = EXCLUDED.open_orders_hive_count,
          open_orders_hive_amount = EXCLUDED.open_orders_hive_amount
    RETURNING 1
  ),

  /* 10. append new cancel IDs                                       */
  append_ids AS (
    UPDATE btracker_app.account_open_orders_summary dst
    SET cancelled_order_ids = dst.cancelled_order_ids || c.id
    FROM cancels c
    WHERE dst.account_name = c.acct
      AND NOT (c.id = ANY(dst.cancelled_order_ids))
    RETURNING 1
  ),

  /* 11. purge zero-count rows                                       */
  purge AS (
    DELETE FROM btracker_app.account_open_orders_summary dst
    WHERE dst.open_orders_hbd_count  = 0
      AND dst.open_orders_hive_count = 0
    RETURNING 1
  )

  /* 12. stats                                                       */
  SELECT
     (SELECT COUNT(*) FROM ins),
     (SELECT COUNT(*) FROM closed_rows),
     (SELECT COUNT(*) FROM up_summary),
     (SELECT COUNT(*) FROM append_ids),
     (SELECT COUNT(*) FROM purge)
  INTO
     c_new_creates,
     c_closed_rows,
     c_summary_upserts,
     c_new_cancel_ids,
     c_purged;

  RAISE NOTICE
    'Blocks %–% | new % | closed % | upserts % | cancel-ids % | purged %',
    _from_block, _to_block,
    c_new_creates, c_closed_rows, c_summary_upserts,
    c_new_cancel_ids, c_purged;
END;
$$;

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
  c_closed_cancels  INT;
  c_closed_fills    INT;
  c_closed_rows     INT;
  c_summary_upserts INT;
  c_new_cancel_ids  INT;
  c_purged          INT;
BEGIN
  /* 1) Insert new creates */
  WITH raw_ops AS (
    SELECT ov.body::jsonb AS body
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot ON ot.id = ov.op_type_id
     WHERE ov.block_num BETWEEN _from_block AND _to_block
       AND ot.name = 'hive::protocol::limit_order_create_operation'
  ),
  creates AS (
    SELECT
      (body->'value'->>'owner')::TEXT    AS acct,
      (body->'value'->>'orderid')::BIGINT AS id,
      (body->'value'->'amount_to_sell'->>'nai')    AS nai,
      ((body->'value'->'amount_to_sell'->>'amount')::NUMERIC
         / POWER(10,(body->'value'->'amount_to_sell'->>'precision')::INT)
      )                                            AS amt
    FROM raw_ops
    WHERE (body->'value'->>'orderid') IS NOT NULL
  ),
  ins AS (
    INSERT INTO btracker_app.open_orders_detail
      (account_name, order_id, nai, amount)
    SELECT acct, id, nai, amt
      FROM creates
    ON CONFLICT DO NOTHING
    RETURNING 1
  )
  SELECT COUNT(*) INTO c_new_creates FROM ins;

  /* 2) Delete cancelled orders */
  WITH raw_ops AS (
    SELECT ov.body::jsonb AS body
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot ON ot.id = ov.op_type_id
     WHERE ov.block_num BETWEEN _from_block AND _to_block
       AND ot.name IN (
         'hive::protocol::limit_order_cancel_operation',
         'hive::protocol::limit_order_cancelled_operation'
       )
  ),
  cancels AS (
    SELECT
      (body->'value'->>'orderid')::BIGINT                                    AS id,
      COALESCE(
        body->'value'->>'seller',
        body->'value'->>'owner'
      )::TEXT                                                                   AS acct
    FROM raw_ops
    WHERE (body->'value'->>'orderid') IS NOT NULL
  )
  DELETE FROM btracker_app.open_orders_detail d
  USING cancels c
  WHERE d.account_name = c.acct
    AND d.order_id     = c.id;
  GET DIAGNOSTICS c_closed_cancels = ROW_COUNT;

  /* 3) Delete filled orders */
  WITH raw_ops AS (
    SELECT ov.body::jsonb AS body
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot ON ot.id = ov.op_type_id
     WHERE ov.block_num BETWEEN _from_block AND _to_block
       AND ot.name = 'hive::protocol::fill_order_operation'
  ),
  fills AS (
    SELECT
      (body->'value'->>'open_owner')   ::TEXT   AS acct,
      (body->'value'->>'open_orderid') ::BIGINT AS id
    FROM raw_ops
    WHERE (body->'value'->>'open_orderid') IS NOT NULL

    UNION ALL

    SELECT
      (body->'value'->>'current_owner')   ::TEXT,
      (body->'value'->>'current_orderid') ::BIGINT
    FROM raw_ops
    WHERE (body->'value'->>'current_orderid') IS NOT NULL
  )
  DELETE FROM btracker_app.open_orders_detail d
  USING fills f
  WHERE d.account_name = f.acct
    AND d.order_id     = f.id;
  GET DIAGNOSTICS c_closed_fills = ROW_COUNT;

  c_closed_rows := c_closed_cancels + c_closed_fills;

  /* 4) Rebuild & upsert the summary */
  WITH snap AS (
    SELECT
      account_name,
      COUNT(*) FILTER (WHERE nai = '@@000000013')                    AS open_orders_hbd_count,
      COALESCE(SUM(amount) FILTER (WHERE nai = '@@000000013'), 0)     AS open_orders_hbd_amount,
      COUNT(*) FILTER (WHERE nai = '@@000000021')                    AS open_orders_hive_count,
      COALESCE(SUM(amount) FILTER (WHERE nai = '@@000000021'), 0)     AS open_orders_hive_amount
    FROM btracker_app.open_orders_detail
    GROUP BY account_name
  ),
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
      open_orders_hbd_count,
      open_orders_hbd_amount,
      open_orders_hive_count,
      open_orders_hive_amount
    FROM snap
    ON CONFLICT (account_name) DO UPDATE
      SET open_orders_hbd_count   = EXCLUDED.open_orders_hbd_count,
          open_orders_hbd_amount  = EXCLUDED.open_orders_hbd_amount,
          open_orders_hive_count  = EXCLUDED.open_orders_hive_count,
          open_orders_hive_amount = EXCLUDED.open_orders_hive_amount
    RETURNING 1
  )
  SELECT COUNT(*) INTO c_summary_upserts FROM up_summary;

  /* 5) Append new cancel IDs */
  WITH append_ids AS (
    UPDATE btracker_app.account_open_orders_summary dst
    SET cancelled_order_ids = dst.cancelled_order_ids || c.id
    FROM cancels c
    WHERE dst.account_name = c.acct
      AND NOT (c.id = ANY(dst.cancelled_order_ids))
    RETURNING 1
  )
  SELECT COUNT(*) INTO c_new_cancel_ids FROM append_ids;

  /* 6) Purge zero‐count summaries */
  WITH purge AS (
    DELETE FROM btracker_app.account_open_orders_summary dst
    WHERE dst.open_orders_hbd_count  = 0
      AND dst.open_orders_hive_count = 0
    RETURNING 1
  )
  SELECT COUNT(*) INTO c_purged FROM purge;

  /* 7) Final log */
  RAISE NOTICE
    'Blocks %–% | new_creates=% | cancelled_deleted=% | fills_deleted=% | total_closed=% | summary_upserts=% | appended_ids=% | purged=%',
    _from_block, _to_block,
    c_new_creates,
    c_closed_cancels,
    c_closed_fills,
    c_closed_rows,
    c_summary_upserts,
    c_new_cancel_ids,
    c_purged;
END;
$$;

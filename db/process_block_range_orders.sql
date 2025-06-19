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
  c_new_creates      INT;
  c_new_created_ids  INT;
  c_closed_cancels   INT;
  c_new_cancel_ids   INT;
  c_closed_fills     INT;
  c_new_fill_ids     INT;
  c_summary_upserts  INT;
  c_purged           INT;
BEGIN
  -- a) Load raw ops for this block range
  DROP TABLE IF EXISTS tmp_raw_ops;
  CREATE TEMP TABLE tmp_raw_ops ON COMMIT DROP AS
  SELECT
    ov.body   ::jsonb AS body,
    ot.name           AS op_name
  FROM hive.operations_view ov
  JOIN hafd.operation_types ot
    ON ot.id = ov.op_type_id
  WHERE ov.block_num BETWEEN _from_block AND _to_block
    AND ot.name IN (
      'hive::protocol::limit_order_create_operation',
      'hive::protocol::fill_order_operation',
      'hive::protocol::limit_order_cancel_operation',
      'hive::protocol::limit_order_cancelled_operation'
    );

  -- b) Insert new creates into detail; capture which actually inserted
  DROP TABLE IF EXISTS tmp_ins_created;
  CREATE TEMP TABLE tmp_ins_created ON COMMIT DROP AS
  INSERT INTO btracker_app.open_orders_detail
    (account_name, order_id, nai, amount)
  SELECT
    (body->'value'->>'owner')::TEXT,
    (body->'value'->>'orderid')::BIGINT,
    (body->'value'->'amount_to_sell'->>'nai'),
    ((body->'value'->'amount_to_sell'->>'amount')::NUMERIC
       / POWER(10,(body->'value'->'amount_to_sell'->>'precision')::INT)
    )
  FROM tmp_raw_ops
  WHERE op_name = 'hive::protocol::limit_order_create_operation'
    AND (body->'value'->>'orderid') IS NOT NULL
  ON CONFLICT DO NOTHING
  RETURNING account_name, order_id;
  SELECT COUNT(*) INTO c_new_creates FROM tmp_ins_created;

  -- Log each create event
  INSERT INTO btracker_app.order_event_log (acct, order_id, event_type, block_num)
  SELECT account_name, order_id, 'create', _to_block
  FROM tmp_ins_created;

  -- c) Delete cancelled orders; capture IDs
  DROP TABLE IF EXISTS tmp_cancels;
  CREATE TEMP TABLE tmp_cancels ON COMMIT DROP AS
  SELECT
    (body->'value'->>'orderid')::BIGINT AS id,
    COALESCE(
      body->'value'->>'seller',
      body->'value'->>'owner'
    )::TEXT                             AS acct
  FROM tmp_raw_ops
  WHERE op_name IN (
    'hive::protocol::limit_order_cancel_operation',
    'hive::protocol::limit_order_cancelled_operation'
  )
    AND (body->'value'->>'orderid') IS NOT NULL;
  DELETE FROM btracker_app.open_orders_detail d
  USING tmp_cancels c
  WHERE d.account_name = c.acct
    AND d.order_id     = c.id;
  GET DIAGNOSTICS c_closed_cancels = ROW_COUNT;

  -- Log each cancel event
  INSERT INTO btracker_app.order_event_log (acct, order_id, event_type, block_num)
  SELECT acct, id, 'cancel', _to_block
  FROM tmp_cancels;

  -- d) Delete filled orders; capture IDs
  DROP TABLE IF EXISTS tmp_fills;
  CREATE TEMP TABLE tmp_fills ON COMMIT DROP AS
    SELECT
      (body->'value'->>'open_owner')   ::TEXT   AS acct,
      (body->'value'->>'open_orderid') ::BIGINT AS id
    FROM tmp_raw_ops
    WHERE op_name = 'hive::protocol::fill_order_operation'
      AND (body->'value'->>'open_orderid') IS NOT NULL
  UNION ALL
    SELECT
      (body->'value'->>'current_owner')   ::TEXT   AS acct,
      (body->'value'->>'current_orderid') ::BIGINT AS id
    FROM tmp_raw_ops
    WHERE op_name = 'hive::protocol::fill_order_operation'
      AND (body->'value'->>'current_orderid') IS NOT NULL;
  DELETE FROM btracker_app.open_orders_detail d
  USING tmp_fills f
  WHERE d.account_name = f.acct
    AND d.order_id     = f.id;
  GET DIAGNOSTICS c_closed_fills = ROW_COUNT;

  -- Log each fill event
  INSERT INTO btracker_app.order_event_log (acct, order_id, event_type, block_num)
  SELECT acct, id, 'fill', _to_block
  FROM tmp_fills;

  -- e) Rebuild & upsert per-account summary counts
  WITH snap AS (
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
      open_orders_hbd_count,
      open_orders_hbd_amount,
      open_orders_hive_count,
      open_orders_hive_amount
    )
    SELECT * FROM snap
    ON CONFLICT (account_name) DO UPDATE
      SET open_orders_hbd_count   = EXCLUDED.open_orders_hbd_count,
          open_orders_hbd_amount  = EXCLUDED.open_orders_hbd_amount,
          open_orders_hive_count  = EXCLUDED.open_orders_hive_count,
          open_orders_hive_amount = EXCLUDED.open_orders_hive_amount
    RETURNING 1
  )
  SELECT COUNT(*) INTO c_summary_upserts FROM up_summary;

  -- f) Append to per-account arrays
  WITH a1 AS (
    UPDATE btracker_app.account_open_orders_summary dst
    SET created_order_ids   = dst.created_order_ids   || c.order_id
    FROM tmp_ins_created c
    WHERE dst.account_name = c.account_name
      AND NOT (c.order_id = ANY(dst.created_order_ids))
    RETURNING 1
  )
  SELECT COUNT(*) INTO c_new_created_ids FROM a1;

  WITH a2 AS (
    UPDATE btracker_app.account_open_orders_summary dst
    SET cancelled_order_ids = dst.cancelled_order_ids || c.id
    FROM tmp_cancels c
    WHERE dst.account_name = c.acct
      AND NOT (c.id = ANY(dst.cancelled_order_ids))
    RETURNING 1
  )
  SELECT COUNT(*) INTO c_new_cancel_ids FROM a2;

  WITH a3 AS (
    UPDATE btracker_app.account_open_orders_summary dst
    SET filled_order_ids    = dst.filled_order_ids    || f.id
    FROM tmp_fills f
    WHERE dst.account_name = f.acct
      AND NOT (f.id = ANY(dst.filled_order_ids))
    RETURNING 1
  )
  SELECT COUNT(*) INTO c_new_fill_ids FROM a3;

  -- g) Purge empty summaries
  WITH purge AS (
    DELETE FROM btracker_app.account_open_orders_summary dst
    WHERE dst.open_orders_hbd_count  = 0
      AND dst.open_orders_hive_count = 0
    RETURNING 1
  )
  SELECT COUNT(*) INTO c_purged FROM purge;

  -- h) Final console notice
  RAISE NOTICE
    'Blocks %–% | new_creates=% | created_ids_appended=% | cancelled_deleted=% | cancels_appended=% | fills_deleted=% | fills_appended=% | summary_upserts=% | purged=%',
    _from_block, _to_block,
    c_new_creates,
    c_new_created_ids,
    c_closed_cancels,
    c_new_cancel_ids,
    c_closed_fills,
    c_new_fill_ids,
    c_summary_upserts,
    c_purged;

  -- i) Persist run summary
  INSERT INTO btracker_app.block_range_run_log (
    from_block, to_block,
    new_creates, cancelled_deleted, fills_deleted,
    summary_upserts, purged,
    created_ids, cancelled_ids, filled_ids
  )
  VALUES (
    _from_block, _to_block,
    c_new_creates, c_closed_cancels, c_closed_fills,
    c_summary_upserts, c_purged,
    (SELECT array_agg(order_id) FROM tmp_ins_created),
    (SELECT array_agg(id)       FROM tmp_cancels),
    (SELECT array_agg(id)       FROM tmp_fills)
  );
END;
$$;
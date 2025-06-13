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
  _new_creates       INT;
  _closed_rows       INT;
  _summary_upserts   INT;
  _new_cancel_ids    INT;
  _summaries_purged  INT;
BEGIN
  /* ---------------------------------------------------------------------
     1) Load all create / fill / cancel operations in the block window
     --------------------------------------------------------------------*/
  WITH raw_ops AS (
    SELECT ov.body::jsonb AS body,
           ot.name        AS op_name
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

  /* ---------------------------------------------------------------------
     2) Normalise creates / fills / cancels into tidy CTEs
     --------------------------------------------------------------------*/
  creates AS (
    SELECT
      (body->'value'->>'owner')        ::TEXT    AS account_name,
      (body->'value'->>'orderid')      ::BIGINT  AS order_id,
      (body->'value'->'amount_to_sell'->>'nai') AS nai,
      (body->'value'->'amount_to_sell'->>'amount')::NUMERIC
        / POWER(10,(body->'value'->'amount_to_sell'->>'precision')::INT) AS amount
    FROM raw_ops
    WHERE op_name = 'hive::protocol::limit_order_create_operation'
  ),

  -- fills: we need BOTH sides (open_owner/open_orderid and current_owner/current_orderid)
  fills AS (
    SELECT
      (body->'value'->>'open_owner')   ::TEXT    AS account_name,
      (body->'value'->>'open_orderid') ::BIGINT  AS order_id
    FROM raw_ops
    WHERE op_name = 'hive::protocol::fill_order_operation'
      AND body->'value'->>'open_orderid' IS NOT NULL

    UNION ALL

    SELECT
      (body->'value'->>'current_owner')   ::TEXT,
      (body->'value'->>'current_orderid') ::BIGINT
    FROM raw_ops
    WHERE op_name = 'hive::protocol::fill_order_operation'
      AND body->'value'->>'current_orderid' IS NOT NULL
  ),

  cancels AS (
    SELECT
      (body->'value'->>'orderid')::BIGINT                                         AS order_id,
      COALESCE(body->'value'->>'seller', body->'value'->>'owner')::TEXT           AS account_name
    FROM raw_ops
    WHERE op_name IN (
      'hive::protocol::limit_order_cancel_operation',
      'hive::protocol::limit_order_cancelled_operation'
    )
  ),

  /* ---------------------------------------------------------------------
     3) Insert new creates into detail table
     --------------------------------------------------------------------*/
  insert_creates AS (
    INSERT INTO btracker_app.open_orders_detail (account_name, order_id, nai, amount)
    SELECT account_name, order_id, nai, amount
    FROM   creates
    ON CONFLICT DO NOTHING
    RETURNING 1
  ),

  /* ---------------------------------------------------------------------
     4) Delete rows closed by either fills or cancels
     --------------------------------------------------------------------*/
  del_by_cancel AS (
    DELETE FROM btracker_app.open_orders_detail d
    USING cancels c
    WHERE d.account_name = c.account_name
      AND d.order_id     = c.order_id
    RETURNING 1
  ),

  del_by_fill AS (
    DELETE FROM btracker_app.open_orders_detail d
    USING fills f
    WHERE d.account_name = f.account_name
      AND d.order_id     = f.order_id
    RETURNING 1
  ),

  closed_rows AS (
    SELECT 1 FROM del_by_cancel
    UNION ALL
    SELECT 1 FROM del_by_fill
  ),

  /* ---------------------------------------------------------------------
     5) Fresh per-account aggregation from the canonical detail table
     --------------------------------------------------------------------*/
  fresh_summary AS (
    SELECT
      account_name,
      COUNT(*) FILTER (WHERE nai='@@000000013')                 AS open_orders_hbd_count,
      COALESCE(SUM(amount) FILTER (WHERE nai='@@000000013'),0)  AS open_orders_hbd_amount,
      COUNT(*) FILTER (WHERE nai='@@000000021')                 AS open_orders_hive_count,
      COALESCE(SUM(amount) FILTER (WHERE nai='@@000000021'),0)  AS open_orders_hive_amount
    FROM btracker_app.open_orders_detail
    GROUP BY account_name
  ),

  /* ---------------------------------------------------------------------
     6) Upsert the summary table
     --------------------------------------------------------------------*/
  upsert_summary AS (
    INSERT INTO btracker_app.account_open_orders_summary (
      account_name,
      open_orders_hbd_count,
      open_orders_hive_count,
      open_orders_hive_amount,
      open_orders_hbd_amount
    )
    SELECT
      account_name,
      open_orders_hbd_count,
      open_orders_hive_count,
      open_orders_hive_amount,
      open_orders_hbd_amount
    FROM fresh_summary
    ON CONFLICT (account_name) DO UPDATE
      SET open_orders_hbd_count   = EXCLUDED.open_orders_hbd_count,
          open_orders_hive_count  = EXCLUDED.open_orders_hive_count,
          open_orders_hive_amount = EXCLUDED.open_orders_hive_amount,
          open_orders_hbd_amount  = EXCLUDED.open_orders_hbd_amount
    RETURNING 1
  ),

  /* ---------------------------------------------------------------------
     7) Append newly-seen cancel IDs into the array (optional bookkeeping)
     --------------------------------------------------------------------*/
  append_cancels AS (
    UPDATE btracker_app.account_open_orders_summary dst
    SET cancelled_order_ids = dst.cancelled_order_ids || c.order_id
    FROM cancels c
    WHERE dst.account_name = c.account_name
      AND NOT (c.order_id = ANY(dst.cancelled_order_ids))
    RETURNING 1
  ),

  /* ---------------------------------------------------------------------
     8) Purge summary rows whose counts are both zero
     --------------------------------------------------------------------*/
  purge_empty AS (
    DELETE FROM btracker_app.account_open_orders_summary dst
     WHERE dst.open_orders_hbd_count  = 0
       AND dst.open_orders_hive_count = 0
    RETURNING 1
  )

  /* ---------------------------------------------------------------------
     9) Gather stats and emit a notice
     --------------------------------------------------------------------*/
  SELECT
    (SELECT COUNT(*) FROM insert_creates),
    (SELECT COUNT(*) FROM closed_rows),
    (SELECT COUNT(*) FROM upsert_summary),
    (SELECT COUNT(*) FROM append_cancels),
    (SELECT COUNT(*) FROM purge_empty)
  INTO
    _new_creates,
    _closed_rows,
    _summary_upserts,
    _new_cancel_ids,
    _summaries_purged;

  RAISE NOTICE
   'Blocks %–% | creates % | closed % | upserts % | new cancels % | purged %',
   _from_block, _to_block,
   _new_creates,  _closed_rows,  _summary_upserts,
   _new_cancel_ids, _summaries_purged;

END;
$$;

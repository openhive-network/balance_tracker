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
  _closed_orders     INT;
  _summary_upserts   INT;
  _new_cancel_ids    INT;
  _summaries_deleted INT;
BEGIN
  ------------------------------------------------------------------
  -- 1) Pull slice-wide ops
  ------------------------------------------------------------------
  WITH raw_ops AS (
         SELECT ov.body::jsonb AS body,
                ot.name       AS op_name
           FROM hive.operations_view ov
           JOIN hafd.operation_types ot ON ot.id = ov.op_type_id
          WHERE ov.block_num BETWEEN _from_block AND _to_block
            AND ot.name IN (
                  'hive::protocol::limit_order_create_operation',
                  'hive::protocol::fill_order_operation',
                  'hive::protocol::limit_order_cancelled_operation'
                )
       ),

  ------------------------------------------------------------------
  -- 2) Bucket into creates / fills / cancels
  ------------------------------------------------------------------
       creates AS (
         SELECT
           (body->'value'->>'owner')     ::TEXT    AS account_name,
           (body->'value'->>'orderid')   ::BIGINT  AS order_id,
           (body->'value'->'amount_to_sell'->>'nai')        AS nai,
           ((body->'value'->'amount_to_sell'->>'amount')::NUMERIC
              / POWER(
                  10,
                  (body->'value'->'amount_to_sell'->>'precision')::INT
                )
           )                                             AS amount
         FROM raw_ops
         WHERE op_name = 'hive::protocol::limit_order_create_operation'
       ),

       fills AS (
         SELECT (body->'value'->>'open_orderid')::BIGINT    AS order_id
           FROM raw_ops
          WHERE op_name = 'hive::protocol::fill_order_operation'
            AND body->'value'->>'open_orderid' IS NOT NULL
         UNION
         SELECT (body->'value'->>'current_orderid')::BIGINT AS order_id
           FROM raw_ops
          WHERE op_name = 'hive::protocol::fill_order_operation'
            AND body->'value'->>'current_orderid' IS NOT NULL
       ),

       cancels AS (
         SELECT
           (body->'value'->>'orderid')::BIGINT AS order_id,
           (body->'value'->>'seller')      ::TEXT   AS account_name
         FROM raw_ops
         WHERE op_name = 'hive::protocol::limit_order_cancelled_operation'
       ),

  ------------------------------------------------------------------
  -- 3) Persist new creates
  ------------------------------------------------------------------
       create_rows AS (
         INSERT INTO btracker_app.open_orders_detail (order_id, account_name, nai, amount)
         SELECT order_id, account_name, nai, amount
           FROM creates
         ON CONFLICT DO NOTHING
         RETURNING 1
       ),

  ------------------------------------------------------------------
  -- 4) Delete any order_id seen in fills OR cancels
  ------------------------------------------------------------------
       close_rows AS (
         DELETE FROM btracker_app.open_orders_detail d
         USING (
           SELECT order_id FROM fills
           UNION
           SELECT order_id FROM cancels
         ) z
         WHERE d.order_id = z.order_id
         RETURNING 1
       ),

  ------------------------------------------------------------------
  -- 5) Re-aggregate every still-open order
  ------------------------------------------------------------------
       fresh_summary AS (
         SELECT
           account_name,
           COUNT(*) FILTER (WHERE nai = '@@000000013')                  AS open_orders_hbd_count,
           COALESCE(SUM(amount) FILTER (WHERE nai = '@@000000013'), 0) AS open_orders_hbd_amount,
           COUNT(*) FILTER (WHERE nai = '@@000000021')                  AS open_orders_hive_count,
           COALESCE(SUM(amount) FILTER (WHERE nai = '@@000000021'), 0) AS open_orders_hive_amount
         FROM btracker_app.open_orders_detail
         GROUP BY account_name
       ),

  ------------------------------------------------------------------
  -- 6) Upsert the summary
  ------------------------------------------------------------------
       upserts AS (
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

  ------------------------------------------------------------------
  -- 7) Append new cancels into the array
  ------------------------------------------------------------------
       append_cancels AS (
         UPDATE btracker_app.account_open_orders_summary dst
         SET cancelled_order_ids = dst.cancelled_order_ids || c.order_id
         FROM cancels c
         WHERE dst.account_name = c.account_name
           AND NOT (c.order_id = ANY(dst.cancelled_order_ids))
         RETURNING 1
       ),

  ------------------------------------------------------------------
  -- 8) Purge truly-empty accounts
  ------------------------------------------------------------------
       purge_empty AS (
         DELETE FROM btracker_app.account_open_orders_summary dst
          WHERE open_orders_hbd_count  = 0
            AND open_orders_hive_count = 0
         RETURNING 1
       )

  ------------------------------------------------------------------
  -- 9) Collect stats into locals
  ------------------------------------------------------------------
  SELECT
    (SELECT COUNT(*) FROM create_rows),
    (SELECT COUNT(*) FROM close_rows),
    (SELECT COUNT(*) FROM upserts),
    (SELECT COUNT(*) FROM append_cancels),
    (SELECT COUNT(*) FROM purge_empty)
  INTO
    _new_creates,
    _closed_orders,
    _summary_upserts,
    _new_cancel_ids,
    _summaries_deleted;

  RAISE NOTICE
    'Blocks %–% | creates %, closed %, upserts %, new-cancels %, rows-purged %',
    _from_block, _to_block,
    _new_creates, _closed_orders, _summary_upserts, _new_cancel_ids, _summaries_deleted;
END;
$$;

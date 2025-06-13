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
  __inserted_creates  INT;
  __closed_orders     INT;
  __upserted          INT;
  __cancelled_ids     INT;
  __deleted_summaries INT;
BEGIN
  ------------------------------------------------------------------
  -- 1) Slice-level raw operations
  ------------------------------------------------------------------
  WITH raw_ops AS (
         SELECT ov.body::jsonb AS body,
                ot.name        AS op_name
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
  -- 2) Break into creates / fills / cancels
  ------------------------------------------------------------------
       creates AS (
         SELECT
           (body->'value'->>'owner')::TEXT           AS account_name,
           (body->'value'->>'orderid')::BIGINT       AS order_id,
           (body->'value'->'amount_to_sell'->>'nai') AS nai,
           ((body->'value'->'amount_to_sell'->>'amount')::NUMERIC
             / POWER(
                 10,
                 (body->'value'->'amount_to_sell'->>'precision')::INT
               )
           )                                         AS amount
         FROM raw_ops
         WHERE op_name = 'hive::protocol::limit_order_create_operation'
           AND body->'value'->>'orderid' IS NOT NULL
       ),

       fills AS (
         SELECT (body->'value'->>'open_orderid')::BIGINT AS order_id
         FROM raw_ops
         WHERE op_name = 'hive::protocol::fill_order_operation'
           AND body->'value'->>'open_orderid' IS NOT NULL
         UNION
         SELECT (body->'value'->>'current_orderid')::BIGINT
         FROM raw_ops
         WHERE op_name = 'hive::protocol::fill_order_operation'
           AND body->'value'->>'current_orderid' IS NOT NULL
       ),

       cancels AS (
         SELECT
           (body->'value'->>'orderid')::BIGINT AS order_id,
           (body->'value'->>'owner')::TEXT     AS account_name
         FROM raw_ops
         WHERE op_name = 'hive::protocol::limit_order_cancelled_operation'
           AND body->'value'->>'orderid' IS NOT NULL
       ),

  ------------------------------------------------------------------
  -- 3) Persist creates into detail table
  ------------------------------------------------------------------
       insert_creates AS (
         INSERT INTO btracker_app.open_orders_detail (order_id, account_name, nai, amount)
         SELECT order_id, account_name, nai, amount
         FROM creates
         ON CONFLICT DO NOTHING
         RETURNING 1
       ),

  ------------------------------------------------------------------
  -- 4) Remove fills and cancels from detail table
  ------------------------------------------------------------------
       close_ops AS (
         DELETE FROM btracker_app.open_orders_detail d
         USING (
           SELECT order_id FROM fills
           UNION
           SELECT order_id FROM cancels
         ) x
         WHERE d.order_id = x.order_id
         RETURNING 1
       ),

  ------------------------------------------------------------------
  -- 5) Fresh full summary from detail table (all still-open orders)
  ------------------------------------------------------------------
       full_summary AS (
         SELECT
           account_name,
           COUNT(*) FILTER (WHERE nai = '@@000000013')                   AS open_orders_hbd_count,
           COALESCE(SUM(amount) FILTER (WHERE nai = '@@000000013'), 0)  AS open_orders_hbd_amount,
           COUNT(*) FILTER (WHERE nai = '@@000000021')                   AS open_orders_hive_count,
           COALESCE(SUM(amount) FILTER (WHERE nai = '@@000000021'), 0)  AS open_orders_hive_amount
         FROM btracker_app.open_orders_detail
         GROUP BY account_name
       ),

  ------------------------------------------------------------------
  -- 6) Upsert refreshed summary
  ------------------------------------------------------------------
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
         FROM full_summary
         ON CONFLICT (account_name) DO UPDATE
           SET open_orders_hbd_count   = EXCLUDED.open_orders_hbd_count,
               open_orders_hive_count  = EXCLUDED.open_orders_hive_count,
               open_orders_hive_amount = EXCLUDED.open_orders_hive_amount,
               open_orders_hbd_amount  = EXCLUDED.open_orders_hbd_amount
         RETURNING 1
       ),

  ------------------------------------------------------------------
  -- 7) Append newly seen cancellations into the array column
  ------------------------------------------------------------------
       record_cancels AS (
         UPDATE btracker_app.account_open_orders_summary dst
         SET cancelled_order_ids = dst.cancelled_order_ids || c.order_id
         FROM cancels c
         WHERE dst.account_name = c.account_name
           AND NOT (c.order_id = ANY(dst.cancelled_order_ids))
         RETURNING 1
       ),

  ------------------------------------------------------------------
  -- 8) Drop summary rows whose counts are now both zero
  ------------------------------------------------------------------
       clear_empty AS (
         DELETE FROM btracker_app.account_open_orders_summary dst
         WHERE open_orders_hbd_count  = 0
           AND open_orders_hive_count = 0
         RETURNING 1
       )

  ------------------------------------------------------------------
  -- 9) Collect stats
  ------------------------------------------------------------------
  SELECT
      (SELECT COUNT(*) FROM insert_creates),
      (SELECT COUNT(*) FROM close_ops),
      (SELECT COUNT(*) FROM upsert_summary),
      (SELECT COUNT(*) FROM record_cancels),
      (SELECT COUNT(*) FROM clear_empty)
    INTO
      __inserted_creates,
      __closed_orders,
      __upserted,
      __cancelled_ids,
      __deleted_summaries;

  RAISE NOTICE
    'Blocks %–% | creates %, closed %, summary-upserts %, new-cancels %, empty-accounts-deleted %',
    _from_block,
    _to_block,
    __inserted_creates,
    __closed_orders,
    __upserted,
    __cancelled_ids,
    __deleted_summaries;

END;
$$;

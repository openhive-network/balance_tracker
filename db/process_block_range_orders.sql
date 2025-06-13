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

  WITH
    ----------------------------------------------------------------
    -- 1) Pull all create/fill/cancel ops in this block range
    ----------------------------------------------------------------
    raw_ops AS (
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

    ----------------------------------------------------------------
    -- 2a) Creations: who, id, asset, amount
    ----------------------------------------------------------------
    creates AS (
      SELECT
        (body->'value'->>'owner')     ::TEXT   AS account_name,
        (body->'value'->>'orderid')   ::BIGINT AS order_id,
        (body->'value'->'amount_to_sell'->>'nai')       AS nai,
        ((body->'value'->'amount_to_sell'->>'amount')::NUMERIC
           / POWER(
               10,
               (body->'value'->'amount_to_sell'->>'precision')::INT
             )
        )                                             AS amount
      FROM raw_ops
      WHERE op_name = 'hive::protocol::limit_order_create_operation'
    ),

    ----------------------------------------------------------------
    -- 2b) Fills: any mention of the order closes it
    ----------------------------------------------------------------
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

    ----------------------------------------------------------------
    -- 2c) Cancels: who cancelled which id
    ----------------------------------------------------------------
    cancels AS (
      SELECT
        (body->'value'->>'orderid')::BIGINT AS order_id,
        (body->'value'->>'seller')      ::TEXT   AS account_name
      FROM raw_ops
      WHERE op_name = 'hive::protocol::limit_order_cancelled_operation'
    ),

    ----------------------------------------------------------------
    -- 3) Insert new creates into detail table
    ----------------------------------------------------------------
    create_rows AS (
      INSERT INTO btracker_app.open_orders_detail (
        account_name, order_id, nai, amount
      )
      SELECT account_name, order_id, nai, amount
        FROM creates
      ON CONFLICT DO NOTHING
      RETURNING 1
    ),

    ----------------------------------------------------------------
    -- 4a) Delete rows closed by cancel (match both account & id)
    ----------------------------------------------------------------
    del_by_cancel AS (
      DELETE FROM btracker_app.open_orders_detail d
      USING cancels c
      WHERE d.account_name = c.account_name
        AND d.order_id     = c.order_id
      RETURNING 1
    ),

    ----------------------------------------------------------------
    -- 4b) Delete rows closed by fill (lookup account via detail)
    ----------------------------------------------------------------
    del_by_fill AS (
      DELETE FROM btracker_app.open_orders_detail d
      USING fills f
      WHERE d.order_id = f.order_id
      RETURNING 1
    ),

    ----------------------------------------------------------------
    -- 4c) Union both delete counts
    ----------------------------------------------------------------
    close_rows AS (
      SELECT 1 FROM del_by_cancel
      UNION ALL
      SELECT 1 FROM del_by_fill
    ),

    ----------------------------------------------------------------
    -- 5) Re-aggregate every still-open order into a fresh summary
    ----------------------------------------------------------------
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

    ----------------------------------------------------------------
    -- 6) Upsert the summary table
    ----------------------------------------------------------------
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

    ----------------------------------------------------------------
    -- 7) Append any new cancelled IDs into the array
    ----------------------------------------------------------------
    append_cancels AS (
      UPDATE btracker_app.account_open_orders_summary dst
      SET cancelled_order_ids = dst.cancelled_order_ids || c.order_id
      FROM cancels c
      WHERE dst.account_name = c.account_name
        AND NOT (c.order_id = ANY(dst.cancelled_order_ids))
      RETURNING 1
    ),

    ----------------------------------------------------------------
    -- 8) Purge truly-empty accounts (both counts zero)
    ----------------------------------------------------------------
    purge_empty AS (
      DELETE FROM btracker_app.account_open_orders_summary dst
       WHERE dst.open_orders_hbd_count  = 0
         AND dst.open_orders_hive_count = 0
      RETURNING 1
    )

  ----------------------------------------------------------------
  -- 9) Collect slice stats into locals
  ----------------------------------------------------------------
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

  ----------------------------------------------------------------
  -- 10) Log the results
  ----------------------------------------------------------------
  RAISE NOTICE
    'Blocks %–% | new creates %, closed orders %, summary upserts %, new cancels %, rows purged %',
    _from_block, _to_block,
    _new_creates, _closed_orders, _summary_upserts, _new_cancel_ids, _summaries_deleted;
END;
$$;

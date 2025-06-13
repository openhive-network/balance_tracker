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
  __upserted     INT;
  __cancelled    INT;
  __deleted_rows INT;
BEGIN

  WITH
    -- 1) Grab creates, fills & cancels in one pass
    raw_ops AS (
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
          'hive::protocol::limit_order_cancelled_operation'
        )
    ),

    -- 2a) New creates
    creates AS (
      SELECT
        (body->'value'->>'owner')::TEXT           AS account_name,
        (body->'value'->>'orderid')::BIGINT       AS order_id,
        (body->'value'->'amount_to_sell'->>'nai') AS nai,
        (
          (body->'value'->'amount_to_sell'->>'amount')::NUMERIC
          / POWER(
              10::NUMERIC,
              (body->'value'->'amount_to_sell'->>'precision')::INT
            )
        )                                          AS amount
      FROM raw_ops
      WHERE op_name = 'hive::protocol::limit_order_create_operation'
        AND (body->'value'->>'orderid') IS NOT NULL
    ),

    -- 2b) Fills
    fills AS (
      SELECT (body->'value'->>'open_orderid')::BIGINT    AS order_id
      FROM raw_ops
      WHERE op_name = 'hive::protocol::fill_order_operation'
        AND (body->'value'->>'open_orderid') IS NOT NULL

      UNION

      SELECT (body->'value'->>'current_orderid')::BIGINT AS order_id
      FROM raw_ops
      WHERE op_name = 'hive::protocol::fill_order_operation'
        AND (body->'value'->>'current_orderid') IS NOT NULL
    ),

    -- 2c) Cancels (with account_name for array-append)
    cancels AS (
      SELECT
        (body->'value'->>'orderid')::BIGINT AS order_id,
        (body->'value'->>'owner')       ::TEXT   AS account_name
      FROM raw_ops
      WHERE op_name = 'hive::protocol::limit_order_cancelled_operation'
        AND (body->'value'->>'orderid') IS NOT NULL
    ),

    -- 3) This-slice open-orders summary
    open_summary AS (
      SELECT
        c.account_name,
        COUNT(*) FILTER (WHERE c.nai = '@@000000013')      AS open_orders_hbd_count,
        COALESCE(SUM(c.amount) FILTER (WHERE c.nai = '@@000000013'), 0) AS open_orders_hbd_amount,
        COUNT(*) FILTER (WHERE c.nai = '@@000000021')      AS open_orders_hive_count,
        COALESCE(SUM(c.amount) FILTER (WHERE c.nai = '@@000000021'), 0) AS open_orders_hive_amount
      FROM creates c
      LEFT JOIN fills   f ON f.order_id = c.order_id
      LEFT JOIN cancels x ON x.order_id = c.order_id
      WHERE f.order_id IS NULL
        AND x.order_id IS NULL
      GROUP BY c.account_name
    ),

    -- 4) Upsert into the summary table
    upsert AS (
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
      FROM open_summary
      ON CONFLICT (account_name) DO UPDATE
        SET
          open_orders_hbd_count   = EXCLUDED.open_orders_hbd_count,
          open_orders_hive_count  = EXCLUDED.open_orders_hive_count,
          open_orders_hive_amount = EXCLUDED.open_orders_hive_amount,
          open_orders_hbd_amount  = EXCLUDED.open_orders_hbd_amount
      RETURNING 1
    ),

    -- 5) Append newly-cancelled IDs into the array
    record_cancels AS (
      UPDATE btracker_app.account_open_orders_summary dst
      SET cancelled_order_ids = dst.cancelled_order_ids || c.order_id
      FROM cancels c
      WHERE dst.account_name = c.account_name
        AND NOT (c.order_id = ANY(dst.cancelled_order_ids))
      RETURNING 1
    ),

    -- 6) Only delete accounts with zero open-order counts
    cleanup AS (
      DELETE FROM btracker_app.account_open_orders_summary dst
       WHERE dst.open_orders_hbd_count  = 0
         AND dst.open_orders_hive_count = 0
      RETURNING 1
    )

  -- 7) Capture counts into vars
  SELECT
    (SELECT COUNT(*) FROM upsert),
    (SELECT COUNT(*) FROM record_cancels),
    (SELECT COUNT(*) FROM cleanup)
  INTO
    __upserted,
    __cancelled,
    __deleted_rows;

  RAISE NOTICE 'Blocks %–%: upserted %, cancelled %, deleted %',
    _from_block, _to_block, __upserted, __cancelled, __deleted_rows;

END;
$$;

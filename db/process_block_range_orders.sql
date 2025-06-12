SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION process_block_range_orders(
    IN _from_block INT,
    IN _to_block   INT
)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
  __upserted     INT;
  __deleted_rows INT;
BEGIN

  WITH
    -- 0) Find the current head of the chain
    head AS (
      SELECT MAX(block_num) AS blk
        FROM hive.operations_view
    ),

    -- 1) Pull only the creates in your slice
    creates AS (
      SELECT
        (ov.body::jsonb->'value'->>'owner')::TEXT     AS account_name,
        (ov.body::jsonb->'value'->>'orderid')::BIGINT AS order_id,
        (ov.body::jsonb->'value'->'amount_to_sell'->>'nai')    AS nai,
        (
          (ov.body::jsonb->'value'->'amount_to_sell'->>'amount')::NUMERIC
          / POWER(
              10,
              (ov.body::jsonb->'value'->'amount_to_sell'->>'precision')::INT
            )
        )                                                       AS amount
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot
        ON ot.id = ov.op_type_id
      WHERE ot.name = 'hive::protocol::limit_order_create_operation'
        AND ov.block_num BETWEEN _from_block AND _to_block
        AND (ov.body::jsonb->'value'->>'orderid') IS NOT NULL
    ),

    -- 2) Pull every fill of your orders, on *either* side, from slice start up to head
    fills AS (
      SELECT (ov.body::jsonb->'value'->>'open_orderid')::BIGINT  AS order_id
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot
        ON ot.id = ov.op_type_id
      JOIN head h ON ov.block_num <= h.blk
      WHERE ot.name = 'hive::protocol::fill_order_operation'
        AND (ov.body::jsonb->'value'->>'open_orderid') IS NOT NULL
        AND ov.block_num >= _from_block

      UNION

      SELECT (ov.body::jsonb->'value'->>'current_orderid')::BIGINT AS order_id
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot
        ON ot.id = ov.op_type_id
      JOIN head h ON ov.block_num <= h.blk
      WHERE ot.name = 'hive::protocol::fill_order_operation'
        AND (ov.body::jsonb->'value'->>'current_orderid') IS NOT NULL
        AND ov.block_num >= _from_block
    ),

    -- 3) Pull every cancel of your orders from slice start up to head
    cancels AS (
      SELECT (ov.body::jsonb->'value'->>'orderid')::BIGINT AS order_id
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot
        ON ot.id = ov.op_type_id
      JOIN head h ON ov.block_num <= h.blk
      WHERE ot.name = 'hive::protocol::limit_order_cancelled_operation'
        AND (ov.body::jsonb->'value'->>'orderid') IS NOT NULL
        AND ov.block_num >= _from_block
    ),

    -- 4) Now aggregate only those creates that were neither filled nor cancelled
    open_summary AS (
      SELECT
        c.account_name,
        COUNT(*) FILTER (WHERE c.nai = '@@000000013')            AS open_orders_hbd_count,
        COALESCE(SUM(c.amount) FILTER (WHERE c.nai = '@@000000013'), 0) AS open_orders_hbd_amount,
        COUNT(*) FILTER (WHERE c.nai = '@@000000021')            AS open_orders_hive_count,
        COALESCE(SUM(c.amount) FILTER (WHERE c.nai = '@@000000021'), 0) AS open_orders_hive_amount
      FROM creates c
      LEFT JOIN fills   f ON f.order_id   = c.order_id
      LEFT JOIN cancels x ON x.order_id   = c.order_id
      WHERE f.order_id IS NULL
        AND x.order_id IS NULL
      GROUP BY c.account_name
    ),

    -- 5) Upsert those per-account metrics
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

    -- 6) Remove any accounts that no longer have open orders
    cleanup AS (
      DELETE
        FROM btracker_app.account_open_orders_summary dst
       WHERE NOT EXISTS (
         SELECT 1
           FROM open_summary os
          WHERE os.account_name = dst.account_name
       )
      RETURNING 1
    )
  -- <<< no semicolon here >>>
  SELECT
    (SELECT COUNT(*) FROM upsert),
    (SELECT COUNT(*) FROM cleanup)
  INTO __upserted, __deleted_rows;

  RAISE NOTICE 'Blocks %–%: upserted %, deleted %',
    _from_block, _to_block, __upserted, __deleted_rows;

END;
$$;

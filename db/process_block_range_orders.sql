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
  -- 1) Pull just the three relevant op‐types in one go
  raw_ops AS (
    SELECT
      ov.body::jsonb      AS body,
      ot.name             AS op_name
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

  -- 2) Split out creates, fills, cancels
  creates AS (
    SELECT
      (body->'value'->>'owner')::TEXT            AS account_name,
      (body->'value'->>'orderid')::BIGINT        AS order_id,
      (body->'value'->'amount_to_sell'->>'nai')  AS nai,
      (
        (body->'value'->'amount_to_sell'->>'amount')::NUMERIC
        / POWER(
            10::NUMERIC,
            (body->'value'->'amount_to_sell'->>'precision')::INT
          )
      )                                           AS amount
    FROM raw_ops
    WHERE op_name = 'hive::protocol::limit_order_create_operation'
      AND (body->'value'->>'orderid') IS NOT NULL
  ),

  fills AS (
    SELECT
      (body->'value'->>'open_owner')::TEXT   AS account_name,
      (body->'value'->>'open_orderid')::BIGINT AS order_id
    FROM raw_ops
    WHERE op_name = 'hive::protocol::fill_order_operation'
      AND (body->'value'->>'open_orderid') IS NOT NULL
  ),

  cancels AS (
    SELECT
      (body->'value'->>'seller')::TEXT      AS account_name,
      (body->'value'->>'orderid')::BIGINT   AS order_id
    FROM raw_ops
    WHERE op_name = 'hive::protocol::limit_order_cancelled_operation'
      AND (body->'value'->>'orderid') IS NOT NULL
  ),

  -- 3) Compute per-account open orders summary
  open_summary AS (
    SELECT
      c.account_name,
      COUNT(*) FILTER (WHERE c.nai = '@@000000013') AS open_orders_hbd_count,
      COUNT(*) FILTER (WHERE c.nai = '@@000000021') AS open_orders_hive_count,
      COALESCE(SUM(c.amount) FILTER (WHERE c.nai = '@@000000021'), 0) AS open_orders_hive_amount,
      COALESCE(SUM(c.amount) FILTER (WHERE c.nai = '@@000000013'), 0) AS open_orders_hbd_amount
    FROM creates c
    LEFT JOIN fills   f ON f.order_id   = c.order_id
    LEFT JOIN cancels x ON x.order_id   = c.order_id
    WHERE f.order_id IS NULL
      AND x.order_id IS NULL
    GROUP BY c.account_name
  ),

  -- 4) Upsert into your summary table
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

  -- 5) Remove any account no longer in open_summary
  cleanup AS (
    DELETE FROM btracker_app.account_open_orders_summary dst
    WHERE NOT EXISTS (
      SELECT 1
        FROM open_summary os
       WHERE os.account_name = dst.account_name
    )
    RETURNING 1
  )

  -- 6) Capture row‐counts & log
  SELECT COUNT(*) INTO __upserted   FROM upsert;
  SELECT COUNT(*) INTO __deleted_rows FROM cleanup;

  RAISE NOTICE 'Blocks %–%: upserted %, deleted %',
    _from_block, _to_block, __upserted, __deleted_rows;

END;
$$;

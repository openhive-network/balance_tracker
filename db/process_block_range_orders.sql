SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION process_block_range_orders(
    IN _from INT,
    IN _to   INT
)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS $$
DECLARE
  __insert_orders INT;
BEGIN
  WITH
    -- 1) All fills in the DB
    filled_orders AS (
      SELECT DISTINCT
        (ov.body::jsonb->'value'->>'open_owner')::TEXT  AS account_name,
        (v.id_text)::BIGINT                              AS order_id
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot
        ON ot.id = ov.op_type_id
      CROSS JOIN LATERAL (
        VALUES
          (ov.body::jsonb->'value'->>'open_orderid'),
          (ov.body::jsonb->'value'->>'current_orderid')
      ) AS v(id_text)
      WHERE ot.name = 'hive::protocol::fill_order_operation'
        AND v.id_text IS NOT NULL
    ),

    -- 2) All cancels in the DB
    cancelled_orders AS (
      SELECT DISTINCT
        (ov.body::jsonb->'value'->>'owner')::TEXT     AS account_name,
        (ov.body::jsonb->'value'->>'orderid')::BIGINT AS order_id
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot
        ON ot.id = ov.op_type_id
      WHERE ot.name IN (
        'hive::protocol::limit_order_cancel_operation',
        'hive::protocol::limit_order_cancelled_operation'
      )
    ),

    -- 3) Creates that were neither filled nor cancelled
    open_creates AS (
      SELECT
        (ov.body::jsonb->'value'->>'owner')::TEXT           AS account_name,
        (ov.body::jsonb->'value'->>'orderid')::BIGINT       AS order_id,
        (ov.body::jsonb->'value'->'amount_to_sell'->>'nai') AS nai,
        (
          (ov.body::jsonb->'value'->'amount_to_sell'->>'amount')::NUMERIC
          / POWER(
              10::NUMERIC,
              (ov.body::jsonb->'value'->'amount_to_sell'->>'precision')::INT
            )
        ) AS amount
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot
        ON ot.id = ov.op_type_id
      WHERE ot.name = 'hive::protocol::limit_order_create_operation'
        AND NOT EXISTS (
          SELECT 1
          FROM filled_orders fo
          WHERE fo.order_id = (ov.body::jsonb->'value'->>'orderid')::BIGINT
        )
        AND NOT EXISTS (
          SELECT 1
          FROM cancelled_orders co
          WHERE co.order_id = (ov.body::jsonb->'value'->>'orderid')::BIGINT
        )
    ),

    -- 4) Summarize open orders by account
    open_summary AS (
      SELECT
        oc.account_name,
        COUNT(*) FILTER (WHERE am.asset = 'HBD')  AS open_orders_hbd_count,
        COUNT(*) FILTER (WHERE am.asset = 'HIVE') AS open_orders_hive_count,
        COALESCE(
          SUM(oc.amount) FILTER (WHERE am.asset = 'HIVE'),
          0
        ) AS open_orders_hive_amount,
        COALESCE(
          SUM(oc.amount) FILTER (WHERE am.asset = 'HBD'),
          0
        ) AS open_orders_hbd_amount
      FROM open_creates oc
      CROSS JOIN LATERAL (VALUES
        ('@@000000013','HBD'),
        ('@@000000021','HIVE')
      ) AS am(nai, asset)
      WHERE oc.nai = am.nai
      GROUP BY oc.account_name
    ),

    -- 5) Upsert into summary table
    insert_account_open_orders_summary AS (
      INSERT INTO btracker_app.account_open_orders_summary
        (account_name,
         open_orders_hbd_count,
         open_orders_hive_count,
         open_orders_hive_amount,
         open_orders_hbd_amount)
      SELECT
        account_name,
        open_orders_hbd_count,
        open_orders_hive_count,
        open_orders_hive_amount,
        open_orders_hbd_amount
      FROM open_summary
      ON CONFLICT (account_name) DO UPDATE
        SET open_orders_hbd_count   = EXCLUDED.open_orders_hbd_count,
            open_orders_hive_count  = EXCLUDED.open_orders_hive_count,
            open_orders_hive_amount = EXCLUDED.open_orders_hive_amount,
            open_orders_hbd_amount  = EXCLUDED.open_orders_hbd_amount
      RETURNING (xmax = 0) AS is_new_entry, account_name
    )

  -- 6) Count how many rows were upserted
  SELECT COUNT(*) INTO __insert_orders
    FROM insert_account_open_orders_summary;

  -- Optional: RAISE NOTICE 'Upserted % rows', __insert_orders;
END;
$$;

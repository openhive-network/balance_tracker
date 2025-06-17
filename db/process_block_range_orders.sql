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
  c_new_creates     INT := 0;
  c_closed_cancels  INT := 0;
  c_closed_fills    INT := 0;
  c_closed_rows     INT := 0;
  c_summary_upserts INT := 0;
  c_new_cancel_ids  INT := 0;
  c_purged          INT := 0;
  tmp_sample        TEXT[];
BEGIN
  -- Clean up any leftover temp table
  DROP TABLE IF EXISTS tmp_raw_ops;

  -- 1) Cache all relevant ops once
  CREATE TEMP TABLE tmp_raw_ops ON COMMIT DROP AS
  SELECT
    ov.body::jsonb AS body,
    ot.name        AS op_name
  FROM hive.operations_view ov
  JOIN hafd.operation_types ot ON ot.id = ov.op_type_id
  WHERE ov.block_num BETWEEN _from_block AND _to_block
    AND ot.name IN (
      'hive::protocol::limit_order_create_operation',
      'hive::protocol::fill_order_operation',
      'hive::protocol::limit_order_cancel_operation',
      'hive::protocol::limit_order_cancelled_operation'
    );

  -- 2) INSERT new creates --------------  (B)
  BEGIN
    WITH creates AS (
      SELECT
        (body->'value'->>'owner')::TEXT    AS acct,
        (body->'value'->>'orderid')::BIGINT AS id,
        (body->'value'->'amount_to_sell'->>'nai')    AS nai,
        ((body->'value'->'amount_to_sell'->>'amount')::NUMERIC
           / POWER(
               10,
               (body->'value'->'amount_to_sell'->>'precision')::INT
             )
        )                                            AS amt
      FROM tmp_raw_ops
      WHERE op_name = 'hive::protocol::limit_order_create_operation'
        AND (body->'value'->>'orderid') IS NOT NULL
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
    RAISE NOTICE '[B] creates inserted = %', c_new_creates;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '[B ERROR] in CREATE step: %', SQLERRM;
  END;

  -- 3) DELETE cancelled orders
  DELETE FROM btracker_app.open_orders_detail d
  USING (
    SELECT
      (body->'value'->>'orderid')::BIGINT AS id,
      COALESCE(
        body->'value'->>'seller',
        body->'value'->>'owner'
      )::TEXT                              AS acct
    FROM tmp_raw_ops
    WHERE op_name IN (
      'hive::protocol::limit_order_cancel_operation',
      'hive::protocol::limit_order_cancelled_operation'
    )
      AND (body->'value'->>'orderid') IS NOT NULL
  ) AS c
  WHERE d.account_name = c.acct
    AND d.order_id     = c.id;
  GET DIAGNOSTICS c_closed_cancels = ROW_COUNT;
  RAISE NOTICE '[Cancel-Delete] rows removed = %', c_closed_cancels;

  -- 4) DELETE filled orders  -----------  (C)
  BEGIN
    -- 4a) Sample the first 5 fill candidates
    WITH fills_info AS (
      SELECT
        (body->'value'->>'open_owner')   ::TEXT   AS acct,
        (body->'value'->>'open_orderid') ::BIGINT AS id,
        (body->'value'->'pays'->>'nai')           AS nai
      FROM tmp_raw_ops
      WHERE op_name = 'hive::protocol::fill_order_operation'
        AND (body->'value'->>'open_orderid') IS NOT NULL

      UNION ALL

      SELECT
        (body->'value'->>'current_owner')   ::TEXT   AS acct,
        (body->'value'->>'current_orderid') ::BIGINT AS id,
        (body->'value'->'receives'->>'nai')       AS nai
      FROM tmp_raw_ops
      WHERE op_name = 'hive::protocol::fill_order_operation'
        AND (body->'value'->>'current_orderid') IS NOT NULL
    )
    SELECT array_agg( (acct,id,nai)::TEXT ) INTO tmp_sample
    FROM (
      SELECT acct,id,nai FROM fills_info LIMIT 5
    ) sub;
    RAISE NOTICE '[C] sample fills-to-delete = %', tmp_sample;

    -- 4b) Now actually delete only the paid side
    DELETE FROM btracker_app.open_orders_detail d
    USING fills_info f
    WHERE d.account_name = f.acct
      AND d.order_id     = f.id
      AND d.nai          = f.nai;
    GET DIAGNOSTICS c_closed_fills = ROW_COUNT;
    RAISE NOTICE '[C] fills deleted = %', c_closed_fills;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '[C ERROR] in FILL-DELETE step: %', SQLERRM;
  END;

  c_closed_rows := c_closed_cancels + c_closed_fills;

  -- 5) Rebuild & upsert summary (no change)
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
    SELECT
      account_name,
      open_orders_hbd_count,
      open_orders_hbd_amount,
      open_orders_hive_count,
      open_orders_hive_amount
    FROM snap
    ON CONFLICT (account_name) DO UPDATE
      SET 
        open_orders_hbd_count   = EXCLUDED.open_orders_hbd_count,
        open_orders_hbd_amount  = EXCLUDED.open_orders_hbd_amount,
        open_orders_hive_count  = EXCLUDED.open_orders_hive_count,
        open_orders_hive_amount = EXCLUDED.open_orders_hive_amount
    RETURNING 1
  )
  SELECT COUNT(*) INTO c_summary_upserts FROM up_summary;

  -- 6) Append cancels & 7) Purge (unchanged)
  /* … your existing append_ids and purge logic here … */

  -- 8) Final log
  RAISE NOTICE
    'Blocks %–% | new_creates=% | cancels_deleted=% | fills_deleted=% | total_closed=% | summary_upserts=%',
    _from_block, _to_block,
    c_new_creates,
    c_closed_cancels,
    c_closed_fills,
    c_closed_rows,
    c_summary_upserts;
END;
$$;

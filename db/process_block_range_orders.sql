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
  c_new_creates     INT;
  c_closed_cancels  INT;
  c_closed_fills    INT;
  c_closed_rows     INT;
  c_summary_upserts INT;
  c_new_cancel_ids  INT;
  c_purged          INT;
BEGIN
  -- 1) Cache all relevant ops once
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
    );  -- ← no stray quote here

  -- 2) Insert new creates
  WITH creates AS (
    SELECT
      (body->'value'->>'owner')::TEXT    AS acct,
      (body->'value'->>'orderid')::BIGINT AS id,
      (body->'value'->'amount_to_sell'->>'nai')    AS nai,
      ((body->'value'->'amount_to_sell'->>'amount')::NUMERIC
         / POWER(10,(body->'value'->'amount_to_sell'->>'precision')::INT)
      )                                            AS amt
    FROM tmp_raw_ops
    WHERE op_name = 'hive::protocol::limit_order_create_operation'
      AND (body->'value'->>'orderid') IS NOT NULL
  ),
  ins AS (
    INSERT INTO btracker_app.open_orders_detail
      (account_name, order_id, nai, amount)
    SELECT acct, id, nai, amt FROM creates
    ON CONFLICT DO NOTHING
    RETURNING 1
  )
  SELECT COUNT(*) INTO c_new_creates FROM ins;

  -- 3) Delete cancels
  DELETE FROM btracker_app.open_orders_detail d
  USING (
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
      AND (body->'value'->>'orderid') IS NOT NULL
  ) AS c
  WHERE d.account_name = c.acct
    AND d.order_id     = c.id;
  GET DIAGNOSTICS c_closed_cancels = ROW_COUNT;

  -- 4) Delete fills
  DELETE FROM btracker_app.open_orders_detail d
  USING (
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
      AND (body->'value'->>'current_orderid') IS NOT NULL
  ) AS f
  WHERE d.account_name = f.acct
    AND d.order_id     = f.id;
  GET DIAGNOSTICS c_closed_fills = ROW_COUNT;

  c_closed_rows := c_closed_cancels + c_closed_fills;

  -- … rest of upsert / append / purge / final NOTICE …

END;
$$;

SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_app.process_block_range_orders(
  p_from_block INT,
  p_to_block   INT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  --------------------------------------------------------------------------------
  -- 1) Pull raw ops in this slice
  --------------------------------------------------------------------------------
  DROP TABLE IF EXISTS tmp_raw_ops;
  CREATE TEMP TABLE tmp_raw_ops AS
  SELECT
    ov.block_num,
    ot.name        AS op_name,
    ov.body::jsonb AS j
  FROM hive.operations_view ov
  JOIN hafd.operation_types ot
    ON ot.id = ov.op_type_id
  WHERE ot.name IN (
    'hive::protocol::limit_order_create_operation',
    'hive::protocol::limit_order_cancel_operation',
    'hive::protocol::limit_order_cancelled_operation',
    'hive::protocol::fill_order_operation'
  )
    AND ov.block_num BETWEEN p_from_block AND p_to_block;

  --------------------------------------------------------------------------------
  -- 2) Capture the latest CREATE per (acct,order,asset) in this slice
  --------------------------------------------------------------------------------
  DROP TABLE IF EXISTS tmp_creates;
  CREATE TEMP TABLE tmp_creates AS
  SELECT DISTINCT ON (acct, order_id, nai)
       acct,
       order_id,
       nai,
       amount,
       block_num
  FROM (
    SELECT
      (j->'value'->>'owner')           AS acct,
      (j->'value'->>'orderid')::BIGINT AS order_id,
      (j->'value'->'amount_to_sell'->>'nai')     AS nai,
      ((j->'value'->'amount_to_sell'->>'amount')::NUMERIC
         / POWER(
             10,
             (j->'value'->'amount_to_sell'->>'precision')::INT
           )
      )                                         AS amount,
      block_num
    FROM tmp_raw_ops
    WHERE op_name = 'hive::protocol::limit_order_create_operation'
  ) sub
  ORDER BY acct, order_id, nai, block_num DESC;

  --------------------------------------------------------------------------------
  -- 3) Log and upsert those CREATES
  --------------------------------------------------------------------------------
  INSERT INTO btracker_app.order_event_log (acct, order_id, event_type, block_num)
  SELECT acct, order_id, 'create', block_num
    FROM tmp_creates;

  INSERT INTO btracker_app.open_orders_detail (account_name, order_id, nai, amount)
  SELECT acct, order_id, nai, amount
    FROM tmp_creates
  ON CONFLICT (account_name, order_id, nai) DO UPDATE
    SET amount = EXCLUDED.amount;

  --------------------------------------------------------------------------------
  -- 4) Materialize FILLS with their deltas
  --------------------------------------------------------------------------------
  DROP TABLE IF EXISTS tmp_fills;
  CREATE TEMP TABLE tmp_fills AS
  SELECT
    (j->'value'->>'open_owner')           AS acct,
    (j->'value'->>'open_orderid')::BIGINT AS order_id,
    (j->'value'->'open_pays'->>'nai')     AS nai,
    ((j->'value'->'open_pays'->>'amount')::NUMERIC
       / POWER(
           10,
           (j->'value'->'open_pays'->>'precision')::INT
         )
    )                                     AS delta,
    block_num
  FROM tmp_raw_ops
  WHERE op_name = 'hive::protocol::fill_order_operation'
    AND j->'value'->>'open_owner' IS NOT NULL

  UNION ALL

  SELECT
    (j->'value'->>'current_owner')           AS acct,
    (j->'value'->>'current_orderid')::BIGINT AS order_id,
    (j->'value'->'current_pays'->>'nai')     AS nai,
    ((j->'value'->'current_pays'->>'amount')::NUMERIC
       / POWER(
           10,
           (j->'value'->'current_pays'->>'precision')::INT
         )
    )                                        AS delta,
    block_num
  FROM tmp_raw_ops
  WHERE op_name = 'hive::protocol::fill_order_operation'
    AND j->'value'->>'current_owner' IS NOT NULL;

  --------------------------------------------------------------------------------
  -- 5) Log, queue, and apply those FILLS
  --------------------------------------------------------------------------------
  -- 5a) log each fill event
  INSERT INTO btracker_app.order_event_log (acct, order_id, event_type, block_num)
  SELECT acct, order_id, 'fill', block_num
    FROM tmp_fills;

  -- 5b) any fills without an existing open → pending
  INSERT INTO btracker_app.pending_fills (account_name, order_id, nai, delta_amount)
  SELECT f.acct, f.order_id, f.nai, f.delta
    FROM tmp_fills f
    LEFT JOIN btracker_app.open_orders_detail d
      ON d.account_name = f.acct
     AND d.order_id     = f.order_id
     AND d.nai          = f.nai
   WHERE d.order_id IS NULL;

  -- 5c) subtract fills from open orders
  UPDATE btracker_app.open_orders_detail dst
     SET amount = dst.amount - f.delta
  FROM tmp_fills f
  WHERE dst.account_name = f.acct
    AND dst.order_id     = f.order_id
    AND dst.nai          = f.nai;

  --------------------------------------------------------------------------------
  -- 6) Log and DELETE CANCELs (drop the nai filter!)
  --------------------------------------------------------------------------------
  INSERT INTO btracker_app.order_event_log (acct, order_id, event_type, block_num)
  SELECT
    COALESCE(j->'value'->>'seller', j->'value'->>'owner') AS acct,
    (j->'value'->>'orderid')::BIGINT                     AS order_id,
    'cancel',
    block_num
  FROM tmp_raw_ops
  WHERE op_name IN (
    'hive::protocol::limit_order_cancel_operation',
    'hive::protocol::limit_order_cancelled_operation'
  );

  DELETE
    FROM btracker_app.open_orders_detail dst
  USING tmp_raw_ops t
  WHERE t.op_name IN (
    'hive::protocol::limit_order_cancel_operation',
    'hive::protocol::limit_order_cancelled_operation'
  )
    AND dst.account_name = COALESCE(t.j->'value'->>'seller', t.j->'value'->>'owner')
    AND dst.order_id     = (t.j->'value'->>'orderid')::BIGINT;

  --------------------------------------------------------------------------------
  -- 7) Done
  --------------------------------------------------------------------------------
  RAISE NOTICE 'Processed blocks % to %', p_from_block, p_to_block;
END;
$$;

RESET ROLE;

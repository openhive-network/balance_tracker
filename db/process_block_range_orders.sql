SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_app.process_block_range_orders(
  p_from_block INT,
  p_to_block   INT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  ----------------------------------------------------------------
  -- 1) Cache all relevant ops in this slice
  ----------------------------------------------------------------
  DROP TABLE IF EXISTS tmp_raw_ops;
  CREATE TEMP TABLE tmp_raw_ops AS
  SELECT
    ov.block_num,
    ot.name        AS op_name,
    ov.body::jsonb AS j
  FROM hive.operations_view ov
  JOIN hafd.operation_types ot ON ot.id = ov.op_type_id
  WHERE ot.name IN (
    'hive::protocol::limit_order_create_operation',
    'hive::protocol::limit_order_cancel_operation',
    'hive::protocol::limit_order_cancelled_operation',
    'hive::protocol::fill_order_operation'
  )
    AND ov.block_num BETWEEN p_from_block AND p_to_block;

  ----------------------------------------------------------------
  -- 2) Materialize fills for reuse
  ----------------------------------------------------------------
  DROP TABLE IF EXISTS tmp_fills;
  CREATE TEMP TABLE tmp_fills AS
  SELECT
    -- maker side
    (j->'value'->>'open_owner')        AS acct,
    (j->'value'->>'open_orderid')::BIGINT AS order_id,
    j->'value'->>'nai'                 AS nai,
    ((j->'value'->'open_pays'->>'amount')::NUMERIC
       / POWER(10,(j->'value'->'open_pays'->>'precision')::INT)
    )                                  AS delta,
    block_num
  FROM tmp_raw_ops
  WHERE op_name = 'hive::protocol::fill_order_operation'
    AND j->'value'->>'open_owner' IS NOT NULL

  UNION ALL

  SELECT
    -- taker side
    (j->'value'->>'current_owner'),
    (j->'value'->>'current_orderid')::BIGINT,
    j->'value'->>'nai',
    ((j->'value'->'current_pays'->>'amount')::NUMERIC
       / POWER(10,(j->'value'->'current_pays'->>'precision')::INT)
    ),
    block_num
  FROM tmp_raw_ops
  WHERE op_name = 'hive::protocol::fill_order_operation'
    AND j->'value'->>'current_owner' IS NOT NULL;

  ----------------------------------------------------------------
  -- 3a) Log creates + upsert
  ----------------------------------------------------------------
  INSERT INTO btracker_app.order_event_log(acct, order_id, event_type, block_num)
  SELECT
    (j->'value'->>'owner'),
    (j->'value'->>'orderid')::BIGINT,
    'create',
    block_num
  FROM tmp_raw_ops
  WHERE op_name = 'hive::protocol::limit_order_create_operation';

  INSERT INTO btracker_app.open_orders_detail(account_name, order_id, nai, amount)
  SELECT
    (j->'value'->>'owner'),
    (j->'value'->>'orderid')::BIGINT,
    (j->'value'->>'nai'),
    ((j->'value'->'amount_to_sell'->>'amount')::NUMERIC
       / POWER(10,(j->'value'->'amount_to_sell'->>'precision')::INT)
    )
  FROM tmp_raw_ops
  WHERE op_name = 'hive::protocol::limit_order_create_operation'
  ON CONFLICT (account_name, order_id, nai) DO UPDATE
    SET amount = EXCLUDED.amount;

  ----------------------------------------------------------------
  -- 3b) Log fills
  ----------------------------------------------------------------
  INSERT INTO btracker_app.order_event_log(acct, order_id, event_type, block_num)
  SELECT acct, order_id, 'fill', block_num
  FROM tmp_fills;

  ----------------------------------------------------------------
  -- 3c) Pending fills (before the create arrives)
  ----------------------------------------------------------------
  INSERT INTO btracker_app.pending_fills(account_name, order_id, nai, delta_amount)
  SELECT f.acct, f.order_id, f.nai, f.delta
  FROM tmp_fills f
  LEFT JOIN btracker_app.open_orders_detail d
    ON d.account_name = f.acct
   AND d.order_id     = f.order_id
   AND d.nai          = f.nai
  WHERE d.order_id IS NULL;

  ----------------------------------------------------------------
  -- 3d) Apply fills
  ----------------------------------------------------------------
  UPDATE btracker_app.open_orders_detail dst
  SET amount = dst.amount - f.delta
  FROM tmp_fills f
  WHERE dst.account_name = f.acct
    AND dst.order_id     = f.order_id
    AND dst.nai          = f.nai;

  ----------------------------------------------------------------
  -- 4) Log + delete cancels
  ----------------------------------------------------------------
  INSERT INTO btracker_app.order_event_log(acct, order_id, event_type, block_num)
  SELECT
    COALESCE(j->'value'->>'seller', j->'value'->>'owner'),
    (j->'value'->>'orderid')::BIGINT,
    'cancel',
    block_num
  FROM tmp_raw_ops
  WHERE op_name IN (
    'hive::protocol::limit_order_cancel_operation',
    'hive::protocol::limit_order_cancelled_operation'
  );

  DELETE FROM btracker_app.open_orders_detail dst
  USING tmp_raw_ops t
  WHERE t.op_name IN (
    'hive::protocol::limit_order_cancel_operation',
    'hive::protocol::limit_order_cancelled_operation'
  )
    AND dst.account_name = COALESCE(t.j->'value'->>'seller', t.j->'value'->>'owner')
    AND dst.order_id     = (t.j->'value'->>'orderid')::BIGINT
    AND dst.nai          = t.j->'value'->>'nai';

  ----------------------------------------------------------------
  -- 5) Done
  ----------------------------------------------------------------
  RAISE NOTICE 'Processed blocks % to %', p_from_block, p_to_block;
END;
$$;

RESET ROLE;

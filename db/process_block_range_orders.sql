SET ROLE btracker_owner;

CREATE OR REPLACE PROCEDURE btracker_app.process_block_range_orders(
  p_from_block INT,
  p_to_block   INT
)
LANGUAGE plpgsql
AS $$
BEGIN
  -- 1) Cache all relevant ops in this slice
  CREATE TEMP TABLE tmp_raw_ops ON COMMIT DROP AS
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
  -- 2a) CREATEs: log the event, then upsert the live‐orders table
  ----------------------------------------------------------------
  INSERT INTO btracker_app.order_event_log (acct, order_id, event_type, block_num)
  SELECT
    (j->'value'->>'owner')           AS acct,
    (j->'value'->>'orderid')::BIGINT AS order_id,
    'create'                         AS event_type,
    block_num
  FROM tmp_raw_ops
  WHERE op_name = 'hive::protocol::limit_order_create_operation';

  INSERT INTO btracker_app.open_orders_detail AS dst
    (account_name, order_id, nai, amount)
  SELECT
    (j->'value'->>'owner')           AS account_name,
    (j->'value'->>'orderid')::BIGINT AS order_id,
    (j->'value'->>'nai')             AS nai,
    ((j->'value'->'amount_to_sell'->>'amount')::NUMERIC
       / POWER(10,(j->'value'->'amount_to_sell'->>'precision')::INT)
    )                                 AS amount
  FROM tmp_raw_ops
  WHERE op_name = 'hive::protocol::limit_order_create_operation'
  ON CONFLICT (account_name, order_id, nai) DO UPDATE
    SET amount = EXCLUDED.amount;

  ----------------------------------------------------------------
  -- 2b) FILLS: decompose both sides, log events, capture pendings,
  --           then subtract from live‐orders
  ----------------------------------------------------------------
  WITH fills AS (
    SELECT
      (j->'value'->>'open_owner')       AS acct,
      (j->'value'->>'open_orderid')::BIGINT AS order_id,
      j->'value'->>'nai'                AS nai,
      ((j->'value'->'open_pays'->>'amount')::NUMERIC
         / POWER(10,(j->'value'->'open_pays'->>'precision')::INT)
      )                                 AS delta,
      block_num
    FROM tmp_raw_ops
    WHERE op_name = 'hive::protocol::fill_order_operation'
      AND j->'value'->>'open_owner' IS NOT NULL

    UNION ALL

    SELECT
      (j->'value'->>'current_owner'),
      (j->'value'->>'current_orderid')::BIGINT,
      j->'value'->>'nai',
      ((j->'value'->'current_pays'->>'amount')::NUMERIC
         / POWER(10,(j->'value'->'current_pays'->>'precision')::INT)
      ),
      block_num
    FROM tmp_raw_ops
    WHERE op_name = 'hive::protocol::fill_order_operation'
      AND j->'value'->>'current_owner' IS NOT NULL
  ),

  -- 2b-i) log every fill event
  _logged AS (
    INSERT INTO btracker_app.order_event_log (acct, order_id, event_type, block_num)
    SELECT acct, order_id, 'fill', block_num FROM fills
    RETURNING acct, order_id, nai, delta
  )

  -- 2b-ii) any fills whose live‐order row is not yet present become pendings
  INSERT INTO btracker_app.pending_fills (account_name, order_id, nai, delta_amount)
  SELECT l.acct, l.order_id, l.nai, l.delta
  FROM _logged l
  LEFT JOIN btracker_app.open_orders_detail d
    ON d.account_name = l.acct
   AND d.order_id     = l.order_id
   AND d.nai          = l.nai
  WHERE d.order_id IS NULL;

  -- 2b-iii) now apply all fills to the live‐orders table
  UPDATE btracker_app.open_orders_detail dst
  SET amount = dst.amount - f.delta
  FROM fills f
  WHERE dst.account_name = f.acct
    AND dst.order_id     = f.order_id
    AND dst.nai          = f.nai;

  ----------------------------------------------------------------
  -- 2c) CANCELs: log then delete
  ----------------------------------------------------------------
  WITH cancels AS (
    SELECT
      COALESCE(
        j->'value'->>'seller',
        j->'value'->>'owner'
      )                                  AS acct,
      (j->'value'->>'orderid')::BIGINT   AS order_id,
      j->'value'->>'nai'                 AS nai,
      block_num
    FROM tmp_raw_ops
    WHERE op_name IN (
      'hive::protocol::limit_order_cancel_operation',
      'hive::protocol::limit_order_cancelled_operation'
    )
  )

  -- 2c-i) log cancels
  INSERT INTO btracker_app.order_event_log (acct, order_id, event_type, block_num)
  SELECT acct, order_id, 'cancel', block_num
  FROM cancels;

  -- 2c-ii) delete from live‐orders
  DELETE FROM btracker_app.open_orders_detail dst
  USING cancels c
  WHERE dst.account_name = c.acct
    AND dst.order_id     = c.order_id
    AND dst.nai          = c.nai;

  ----------------------------------------------------------------
  -- 3) Done
  ----------------------------------------------------------------
  RAISE NOTICE 'Processed blocks % to %', p_from_block, p_to_block;
END;
$$;

RESET ROLE;

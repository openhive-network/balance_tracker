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
  -- 1) Pull raw ops for this slice
  --------------------------------------------------------------------------------
  DROP TABLE IF EXISTS tmp_raw_ops_flat;
  CREATE TEMP TABLE tmp_raw_ops_flat AS
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
  -- 2) Insert CREATEs
  --------------------------------------------------------------------------------
  INSERT INTO btracker_app.account_operations
    (account_name, order_id, nai, op_type, block_num, amount, raw)
  SELECT
    (j->'value'->>'owner')::TEXT           AS account_name,
    (j->'value'->>'orderid')::BIGINT       AS order_id,
    (j->'value'->'amount_to_sell'->>'nai') AS nai,
    'create'                               AS op_type,
    block_num,
    ((j->'value'->'amount_to_sell'->>'amount')::NUMERIC
       / POWER(
           10,
           (j->'value'->'amount_to_sell'->>'precision')::INT
         )
    )                                      AS amount,
    j                                      AS raw
  FROM tmp_raw_ops_flat
  WHERE op_name = 'hive::protocol::limit_order_create_operation';

  --------------------------------------------------------------------------------
  -- 3) Insert FILLS
  --------------------------------------------------------------------------------
  INSERT INTO btracker_app.account_operations
    (account_name, order_id, nai, op_type, block_num, amount, raw)
  SELECT
    (j->'value'->>'open_owner')::TEXT           AS account_name,
    (j->'value'->>'open_orderid')::BIGINT       AS order_id,
    (j->'value'->'open_pays'->>'nai')           AS nai,
    'fill'                                      AS op_type,
    block_num,
    ((j->'value'->'open_pays'->>'amount')::NUMERIC
       / POWER(
           10,
           (j->'value'->'open_pays'->>'precision')::INT
         )
    )                                            AS amount,
    j                                            AS raw
  FROM tmp_raw_ops_flat
  WHERE op_name = 'hive::protocol::fill_order_operation'
    AND j->'value'->>'open_owner' IS NOT NULL

  UNION ALL

  SELECT
    (j->'value'->>'current_owner')::TEXT        AS account_name,
    (j->'value'->>'current_orderid')::BIGINT    AS order_id,
    (j->'value'->'current_pays'->>'nai')        AS nai,
    'fill'                                     AS op_type,
    block_num,
    ((j->'value'->'current_pays'->>'amount')::NUMERIC
       / POWER(
           10,
           (j->'value'->'current_pays'->>'precision')::INT
         )
    )                                           AS amount,
    j                                           AS raw
  FROM tmp_raw_ops_flat
  WHERE op_name = 'hive::protocol::fill_order_operation'
    AND j->'value'->>'current_owner' IS NOT NULL;

  --------------------------------------------------------------------------------
  -- 4) Insert CANCELs
  --------------------------------------------------------------------------------
  INSERT INTO btracker_app.account_operations
    (account_name, order_id, nai, op_type, block_num, raw)
  SELECT
    COALESCE(
      j->'value'->>'seller',
      j->'value'->>'owner'
    )::TEXT                 AS account_name,
    (j->'value'->>'orderid')::BIGINT AS order_id,
    NULL                         AS nai,
    'cancel'                     AS op_type,
    block_num,
    j                            AS raw
  FROM tmp_raw_ops_flat
  WHERE op_name IN (
    'hive::protocol::limit_order_cancel_operation',
    'hive::protocol::limit_order_cancelled_operation'
  );

  RAISE NOTICE 'Flat ops inserted for blocks %–%', p_from_block, p_to_block;
END;
$$;

RESET ROLE;

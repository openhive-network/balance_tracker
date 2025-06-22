-- 1) Elevate privileges
SET ROLE btracker_owner;

-- 2) Create the converter slice processor
CREATE OR REPLACE FUNCTION btracker_app.process_block_range_converts(
  IN _from_block INT,
  IN _to_block   INT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  --------------------------------------------------------------------------------
  -- 1) grab all convert‐related ops in this slice
  --------------------------------------------------------------------------------
  DROP TABLE IF EXISTS tmp_convert_ops;
  CREATE TEMP TABLE tmp_convert_ops ON COMMIT DROP AS
  SELECT
    ov.block_num,
    ot.name         AS op_name,
    ov.body::jsonb  AS j
  FROM hive.operations_view ov
  JOIN hafd.operation_types ot
    ON ot.id = ov.op_type_id
  WHERE ot.name IN (
    'hive::protocol::convert_operation',
    'hive::protocol::fill_convert_request_operation'
  )
    AND ov.block_num BETWEEN _from_block AND _to_block;

  --------------------------------------------------------------------------------
  -- 2) append them to the flat table
  --------------------------------------------------------------------------------
  INSERT INTO btracker_app.account_convert_operations (
    account_name, request_id, nai, op_type, amount, block_num, raw
  )
  SELECT
    (j->'value'->>'owner')::TEXT                               AS account_name,
    (j->'value'->>'requestid')::BIGINT                         AS request_id,
    (j->'value'->'amount'->>'nai')::TEXT                       AS nai,
    CASE WHEN op_name = 'hive::protocol::convert_operation'
         THEN 'convert'
         ELSE 'fill'
    END                                                         AS op_type,
    CASE WHEN op_name = 'hive::protocol::convert_operation'
         THEN (
           (j->'value'->'amount'->>'amount')::NUMERIC
             / POWER(
                 10,
                 (j->'value'->'amount'->>'precision')::INT
               )
         )
         ELSE NULL
    END                                                         AS amount,
    block_num,
    j                                                           AS raw
  FROM tmp_convert_ops;

  --------------------------------------------------------------------------------
  -- 3) Done
  --------------------------------------------------------------------------------
  RAISE NOTICE 'Processed convert ops %–%', _from_block, _to_block;
END;
$$;

-- 3) Drop elevated role
RESET ROLE;

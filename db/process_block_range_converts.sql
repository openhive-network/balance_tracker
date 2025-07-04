SET ROLE btracker_owner;

DROP FUNCTION IF EXISTS btracker_app.process_block_range_converts(INT, INT);

CREATE OR REPLACE FUNCTION btracker_app.process_block_range_converts(
    _from_block INT,
    _to_block   INT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  --------------------------------------------------------------------------------
  -- 0) Silently drop any previous temp table
  --------------------------------------------------------------------------------
  BEGIN
    DROP TABLE tmp_convert_ops;
  EXCEPTION WHEN undefined_table THEN
    -- ignore if it wasn't there
    NULL;
  END;

  --------------------------------------------------------------------------------
  -- 1) Pull raw convert ops in this slice
  --------------------------------------------------------------------------------
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
  -- 2) Append them to the flat table, picking the correct NAI & amount fields
  --------------------------------------------------------------------------------
  INSERT INTO btracker_app.account_convert_operations (
    account_name,
    request_id,
    nai,
    op_type,
    amount,
    block_num,
    raw
  )
  SELECT
    (j->'value'->>'owner')::TEXT                                   AS account_name,
    (j->'value'->>'requestid')::BIGINT                             AS request_id,

    CASE
      WHEN op_name = 'hive::protocol::convert_operation'
        THEN (j->'value'->'amount'->>'nai')::TEXT
      ELSE (j->'value'->'amount_in'->>'nai')::TEXT
    END                                                             AS nai,

    CASE
      WHEN op_name = 'hive::protocol::convert_operation' THEN 'convert'
      ELSE 'fill'
    END                                                             AS op_type,

    CASE
      WHEN op_name = 'hive::protocol::convert_operation'
        THEN (
          (j->'value'->'amount'->>'amount')::NUMERIC
            / POWER(10, (j->'value'->'amount'->>'precision')::INT)
        )
      ELSE (
          (j->'value'->'amount_in'->>'amount')::NUMERIC
            / POWER(10, (j->'value'->'amount_in'->>'precision')::INT)
        )
    END                                                             AS amount,

    block_num,
    j                                                               AS raw
  FROM tmp_convert_ops;

  --------------------------------------------------------------------------------
  -- 3) Done
  --------------------------------------------------------------------------------
END;
$$;

RESET ROLE;

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
  -- 1) Pull raw convert ops in this slice
  --------------------------------------------------------------------------------
  DROP TABLE IF EXISTS tmp_convert_ops;
  CREATE TEMP TABLE tmp_convert_ops ON COMMIT DROP AS
  SELECT
    ov.block_num,
    ot.name        AS op_name,
    ov.body::jsonb AS j
  FROM hive.operations_view ov
  JOIN hafd.operation_types ot
    ON ot.id = ov.op_type_id
  WHERE ot.name IN (
    'hive::protocol::convert_operation',
    'hive::protocol::fill_convert_request_operation'
  )
    AND ov.block_num BETWEEN p_from_block AND p_to_block;

  --------------------------------------------------------------------------------
  -- 2) Append to flat table, extracting NAI and amount correctly
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
    -- owner is always under value.owner
    (j->'value'->>'owner')::TEXT AS account_name,

    -- requestid is always present
    (j->'value'->>'requestid')::BIGINT AS request_id,

    -- choose the right NAI
    CASE
      WHEN op_name = 'hive::protocol::convert_operation'
        THEN (j->'value'->'amount'->>'nai')::TEXT
      ELSE
        -- for fills, take the "in" asset (amount_in) or switch to amount_out if you prefer
        (j->'value'->'amount_in'->>'nai')::TEXT
    END                                                            AS nai,

    -- label row type
    CASE
      WHEN op_name = 'hive::protocol::convert_operation' THEN 'convert'
      ELSE 'fill'
    END                                                            AS op_type,

    -- choose the right numeric amount
    CASE
      WHEN op_name = 'hive::protocol::convert_operation'
        THEN (
          (j->'value'->'amount'->>'amount')::NUMERIC
            / POWER(
                10,
                (j->'value'->'amount'->>'precision')::INT
              )
        )
      ELSE (
          -- capture the HBD‐in on fill; if you want HIVE‐out, swap to amount_out
          (j->'value'->'amount_in'->>'amount')::NUMERIC
            / POWER(
                10,
                (j->'value'->'amount_in'->>'precision')::INT
              )
        )
    END                                                            AS amount,

    block_num,

    j                                                              AS raw
  FROM tmp_convert_ops;

  RAISE NOTICE 'Processed convert ops %–%', p_from_block, p_to_block;
END;
$$;

RESET ROLE;

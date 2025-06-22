CREATE OR REPLACE FUNCTION btracker_app.process_block_range_converts(
  p_from_block INT,
  p_to_block   INT
)
RETURNS VOID
LANGUAGE plpgsql
AS $BODY$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN
    SELECT
        ov.block_num,
        ot.name         AS op_name,
        ov.body::jsonb  AS j
    FROM hive.operations_view AS ov
    JOIN hafd.operation_types AS ot
      ON ot.id = ov.op_type_id
    WHERE ot.name IN (
        'hive::protocol::convert_operation',
        'hive::protocol::fill_convert_request_operation'
      )
      AND ov.block_num BETWEEN p_from_block AND p_to_block
  LOOP
    -- insert a new “convert” request
    IF rec.op_name = 'hive::protocol::convert_operation' THEN
      INSERT INTO btracker_app.account_convert_operations
        (block_num, op_type, request_id, nai, amount)
      VALUES
        (
          rec.block_num,
          'convert',
          (rec.j->'value'->>'requestid')::BIGINT,
          rec.j->'value'->'amount'->>'nai',
          (rec.j->'value'->'amount'->>'amount')::NUMERIC
        );
    -- insert a “fill” for an existing convert request
    ELSIF rec.op_name = 'hive::protocol::fill_convert_request_operation' THEN
      INSERT INTO btracker_app.account_convert_operations
        (block_num, op_type, request_id, nai, amount)
      VALUES
        (
          rec.block_num,
          'fill',
          (rec.j->'value'->>'requestid')::BIGINT,
          rec.j->'value'->'amount'->>'nai',
          (rec.j->'value'->'amount'->>'amount')::NUMERIC
        );
    END IF;
  END LOOP;
END;
$BODY$;

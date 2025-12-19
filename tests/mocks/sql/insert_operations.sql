-- =============================================================================
-- Insert Mock Operations Function
-- =============================================================================
--
-- PURPOSE:
--   Inserts mock blockchain operations into the HAF operations table from JSON.
--
-- PREREQUISITES:
--   - types.sql must be loaded first (defines btracker_backend.operation_type)
--   - HAF database with hafd schema
-- =============================================================================

SET ROLE btracker_owner;

-- -----------------------------------------------------------------------------
-- Function: insert_mock_operations
-- -----------------------------------------------------------------------------
-- Inserts mock operations into the HAF operations table.
--
-- Parameters:
--   _block_json - JSON object with "operations" array containing op data
--
-- JSON Format:
--   {
--     "operations": [
--       {
--         "block_num": 90000001,
--         "op_type_id": 32,
--         "op_pos": 0,
--         "trx_in_block": 0,
--         "body": {"type": "transfer_to_savings_operation", ...}
--       }
--     ]
--   }
--
-- Notes:
--   - op_type_id must match hive.operation_types
--   - Operation body is converted to binary using hafd.operation_from_jsontext
--   - Operation ID is generated from (block_num, op_type_id, op_pos)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION btracker_backend.insert_mock_operations(IN _block_json JSON)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
AS
$BODY$
BEGIN
  INSERT INTO hafd.operations(
    id, trx_in_block, op_pos, body_binary)
  SELECT
    -- Generate operation ID from block num, operation type ID and operation position
    hafd.operation_id(block_num, op_type_id, op_pos),
    trx_in_block,
    op_pos,
    -- Convert JSON text to binary representation of operation
    hafd.operation_from_jsontext(body::TEXT)
  FROM json_populate_recordset(
    NULL::btracker_backend.operation_type,
    _block_json->'operations'
  );

END;
$BODY$;

RESET ROLE;

-- =============================================================================
-- Update HAF State Functions
-- =============================================================================
--
-- PURPOSE:
--   Updates HAF internal state tables to recognize mock blocks as processable.
--   This enables Balance Tracker to sync and process the mock data.
--
-- FUNCTIONS:
--   get_mock_block_range()      - Query actual block range from inserted data
--   update_irreversible_block() - Update HAF state to process mock blocks
--
-- USAGE:
--   Call update_irreversible_block() AFTER inserting all mock blocks and
--   operations. It will dynamically detect the block range and configure HAF.
-- =============================================================================

SET ROLE btracker_owner;

-- -----------------------------------------------------------------------------
-- Function: get_mock_block_range
-- -----------------------------------------------------------------------------
-- Queries hafd.blocks to find the min/max block numbers of inserted mock data.
--
-- Returns:
--   min_block - First block number in the mock data
--   max_block - Last block number in the mock data
--
-- Notes:
--   - Mock blocks are identified by block numbers >= 90000000
--   - This function should be called AFTER inserting mock blocks
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION btracker_backend.get_mock_block_range()
RETURNS TABLE(min_block INT, max_block INT)
LANGUAGE plpgsql
STABLE
AS
$BODY$
BEGIN
  RETURN QUERY
  SELECT MIN(b.num)::INT, MAX(b.num)::INT
  FROM hafd.blocks b
  WHERE b.num >= 90000000;
END;
$BODY$;

-- -----------------------------------------------------------------------------
-- Function: update_irreversible_block
-- -----------------------------------------------------------------------------
-- Updates HAF state to recognize mock blocks as processable.
--
-- This function:
--   1. Queries hafd.blocks to find the actual mock block range
--   2. Sets the consistent block to the end of the mock range
--   3. Sets current/irreversible block to (start - 1) so first block gets processed
--   4. Inserts HF24 entry at first_block (required for delayed voting tests)
--
-- Prerequisites:
--   - Mock blocks must be inserted before calling this function
--   - Uses get_mock_block_range() to detect the block range dynamically
--
-- Returns:
--   start_block - First block in the detected range
--   end_block   - Last block in the detected range
--
-- After calling this function, Balance Tracker's process_blocks.sh will
-- process the mock operations when run.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION btracker_backend.update_irreversible_block()
RETURNS TABLE(start_block INT, end_block INT)
LANGUAGE plpgsql
VOLATILE
AS
$BODY$
DECLARE
  _mock_start_block_num INT;
  _mock_end_block_num INT;
BEGIN
  -- Dynamically determine block range from inserted mock data
  SELECT min_block, max_block
  INTO _mock_start_block_num, _mock_end_block_num
  FROM btracker_backend.get_mock_block_range();

  IF _mock_start_block_num IS NULL THEN
    RAISE EXCEPTION 'No mock blocks found in hafd.blocks (expected blocks >= 90000000)';
  END IF;

  -- Set consistent block to the end of mock data range
  UPDATE hafd.hive_state
  SET consistent_block = _mock_end_block_num;

  -- Set current and irreversible block to ONE BEFORE the start of mock data range
  -- HAF processes blocks AFTER current_block_num, so this ensures the first
  -- mock block (start_block) will be processed
  UPDATE hafd.contexts
  SET current_block_num = _mock_start_block_num - 1,
    irreversible_block = _mock_start_block_num - 1;

  -- Simulate 24th hardfork, required for delayed votes test
  -- HF24 introduced delayed voting (30-day delay before full voting power)
  -- Insert at the first mock block (matching original hardcoded behavior)
  INSERT INTO hafd.applied_hardforks
    (hardfork_num, block_num, hardfork_vop_id)
  SELECT 24, _mock_start_block_num, 386547060934967584;

  -- Return the detected block range for logging purposes
  RETURN QUERY SELECT _mock_start_block_num, _mock_end_block_num;
END;
$BODY$;

RESET ROLE;

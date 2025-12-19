-- =============================================================================
-- Insert Mock Blocks Function
-- =============================================================================
--
-- PURPOSE:
--   Inserts mock block headers into the HAF blocks table from JSON data.
--
-- PREREQUISITES:
--   - types.sql must be loaded first (defines btracker_backend.block_type)
--   - HAF database with hafd schema
-- =============================================================================

SET ROLE btracker_owner;

-- -----------------------------------------------------------------------------
-- Function: insert_mock_blocks
-- -----------------------------------------------------------------------------
-- Inserts mock block headers into the HAF blocks table.
--
-- Parameters:
--   _block_json - JSON object with "blocks" array containing block data
--
-- JSON Format:
--   {
--     "blocks": [
--       {
--         "block_num": 90000001,
--         "hash": "abc123...",
--         "prev": "def456...",
--         "producer_account": "witness-name",
--         ...
--       }
--     ]
--   }
--
-- Notes:
--   - Hash values should be hex-encoded strings (decoded to bytea)
--   - producer_account is looked up in hive.accounts_view
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION btracker_backend.insert_mock_blocks(IN _block_json JSON)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
AS
$BODY$
BEGIN
  INSERT INTO hafd.blocks(
    num,
    hash,
    prev,
    created_at,
    producer_account_id,
    transaction_merkle_root,
    extensions,
    witness_signature,
    signing_key,
    hbd_interest_rate,
    total_vesting_fund_hive,
    total_vesting_shares,
    total_reward_fund_hive,
    virtual_supply,
    current_supply,
    current_hbd_supply,
    dhf_interval_ledger
  )
  SELECT
    block_num,
    decode(hash, 'hex'),
    decode(prev, 'hex'),
    created_at,
    (SELECT av.id FROM hive.accounts_view av WHERE av.name = producer_account),
    decode(transaction_merkle_root, 'hex'),
    NULL,
    decode(witness_signature, 'hex'),
    signing_key,
    hbd_interest_rate::INT,
    total_vesting_fund_hive::numeric,
    total_vesting_shares::numeric,
    total_reward_fund_hive::numeric,
    virtual_supply::numeric,
    current_supply::numeric,
    current_hbd_supply::numeric,
    dhf_interval_ledger::numeric
  FROM json_populate_recordset(
    NULL::btracker_backend.block_type,
    _block_json->'blocks'
  );

END;
$BODY$;

RESET ROLE;

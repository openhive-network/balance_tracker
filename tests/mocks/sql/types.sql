-- =============================================================================
-- Mock Data Type Definitions
-- =============================================================================
--
-- PURPOSE:
--   Defines PostgreSQL composite types used for parsing JSON mock data.
--   These types map JSON structures to SQL records for bulk insertion.
--
-- TYPES:
--   operation_type - Represents a blockchain operation from JSON fixtures
--   block_type     - Represents a block header from JSON fixtures
--
-- USAGE:
--   Used by insert_blocks.sql and insert_operations.sql via json_populate_recordset()
-- =============================================================================

SET ROLE btracker_owner;

-- -----------------------------------------------------------------------------
-- Type: operation_type
-- -----------------------------------------------------------------------------
-- Represents a single blockchain operation for JSON parsing.
--
-- Fields:
--   block_num    - Block number containing the operation
--   op_type_id   - Operation type ID (maps to hive.operation_types)
--   op_pos       - Position within the block's operation list
--   trx_in_block - Transaction index within the block (-1 for virtual ops)
--   body         - JSON representation of the operation
-- -----------------------------------------------------------------------------
DROP TYPE IF EXISTS btracker_backend.operation_type CASCADE;
CREATE TYPE btracker_backend.operation_type AS (
    block_num INT,
    op_type_id INT,
    op_pos INT,
    trx_in_block INT,
    body JSON
);

-- -----------------------------------------------------------------------------
-- Type: block_type
-- -----------------------------------------------------------------------------
-- Represents a blockchain block header for JSON parsing.
--
-- Fields match hafd.blocks table structure. See HAF documentation for
-- detailed field descriptions.
-- -----------------------------------------------------------------------------
DROP TYPE IF EXISTS btracker_backend.block_type CASCADE;
CREATE TYPE btracker_backend.block_type AS (
    block_num INT,
    hash TEXT,
    prev TEXT,
    producer_account TEXT,
    transaction_merkle_root TEXT,
    extensions JSONB,
    witness_signature TEXT,
    signing_key TEXT,
    hbd_interest_rate BIGINT,
    total_vesting_fund_hive TEXT,
    total_vesting_shares TEXT,
    total_reward_fund_hive TEXT,
    virtual_supply TEXT,
    current_supply TEXT,
    current_hbd_supply TEXT,
    dhf_interval_ledger BIGINT,
    created_at TIMESTAMP
);

RESET ROLE;

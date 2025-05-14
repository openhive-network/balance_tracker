SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.operation_type CASCADE;
CREATE TYPE btracker_backend.operation_type AS (
    block_num INT,
    op_type_id INT,
    op_pos INT,
    trx_in_block INT,
    body_binary hafd.operation
);

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
    dhf_interval_ledger)
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
    hafd.operation_id(block_num, op_type_id, op_pos),
    trx_in_block,
    op_pos,
    body_binary
  FROM json_populate_recordset(
    NULL::btracker_backend.operation_type,
    _block_json->'operations'
  );

END;
$BODY$;

CREATE OR REPLACE FUNCTION btracker_backend.update_irreversible_block(IN _mock_start_block_num INT, IN _mock_end_block_num INT)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
AS
$BODY$
BEGIN
  UPDATE hafd.hive_state
  SET consistent_block = _mock_end_block_num;

  UPDATE hafd.contexts
  SET current_block_num = _mock_start_block_num,
    irreversible_block = _mock_start_block_num;

END;
$BODY$;


RESET ROLE;

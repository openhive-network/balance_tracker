\set ON_ERROR_STOP on
RESET ROLE;
-- Real prefix and mock header rows; there are no market operations in the gap.
INSERT INTO hafd.blocks
SELECT
    fixture.num,
    decode(lpad(to_hex(fixture.num), 8, '0'), 'hex') AS block_hash,
    b.prev,
    fixture.created_at,
    b.producer_account_id,
    b.transaction_merkle_root,
    b.extensions,
    b.witness_signature,
    b.signing_key,
    b.hbd_interest_rate,
    b.total_vesting_fund_hive,
    b.total_vesting_shares,
    b.total_reward_fund_hive,
    b.virtual_supply,
    b.current_supply,
    b.current_hbd_supply,
    b.dhf_interval_ledger
FROM (
    VALUES
    (5000000, TIMESTAMP '2020-05-01'),
    (90000001, TIMESTAMP '2021-01-01')
) AS fixture (num, created_at)
CROSS JOIN hafd.blocks AS b
WHERE b.num = 4;
-- The actual mock helper's HF24/26 entries reference the first operation ID.
-- A non-market operation satisfies that FK without creating lifecycle events.
INSERT INTO hafd.operations (id, trx_in_block, op_type_id, op_pos, body_value)
VALUES (hafd.operation_id(90000001, 1), 0, 7, 1, '{}');

SET ROLE btracker_owner;
CREATE SCHEMA btracker_app AUTHORIZATION btracker_owner;
SELECT hive.app_create_context('btracker_app', 'btracker_app', FALSE, FALSE);
SET search_path TO btracker_app;
\ir ../../db/order_lifecycle.sql
SET ROLE btracker_owner;
SET search_path TO btracker_app, order59_test;
SELECT install_order_lifecycle('btracker_app');
UPDATE hafd.contexts SET current_block_num = 5000000, irreversible_block = 5000000
WHERE name = 'btracker_app';
CREATE TEMP TABLE _btracker_ops_batch AS
SELECT
    id,
    block_num,
    op_type_id,
    body_value
FROM operations_view
WHERE block_num BETWEEN 1 AND 5000000;
SELECT process_order_lifecycle(1, 5000000);
SELECT order59_test.check_that((
    SELECT processed_through = 5000000 AND indexed_at = TIMESTAMP '2020-05-01'
    FROM order_lifecycle_status
), 'actual reducer establishes ready indexed prefix before mock cursor jump');
CREATE TEMP TABLE prefix_lifecycle_snapshot AS SELECT to_jsonb(l) AS row_data
FROM order_lifecycle AS l;

BEGIN;
UPDATE hafd.contexts SET current_block_num = 90000000, irreversible_block = 90000000
WHERE name = 'btracker_app';
TRUNCATE _btracker_ops_batch;
SELECT order59_test.expect_atomic_error(
    'SELECT process_order_lifecycle(90000001,90000001)',
    'must start at 5000001', 'unprepared mock cursor jump retains production contiguous-range guard'
);
ROLLBACK;

\ir ../mocks/sql/update_haf_state.sql
-- The actual mock installer connects as haf_admin to update HAF global state.
-- Reset the role so its global context update also covers earlier mixed-owner
-- fixtures; the real mock pipeline has only standalone btracker_app.
RESET ROLE;
SET search_path TO btracker_app, order59_test;
SELECT
    start_block,
    end_block
FROM btracker_backend.update_irreversible_block();
SET ROLE btracker_owner;
SELECT order59_test.check_that((
    SELECT
        processed_through = 90000000 AND indexed_at = TIMESTAMP '2020-05-01'
        AND NOT backfill_required AND backfill_target IS NULL
    FROM order_lifecycle_status
),
'actual mock helper aligns synthetic cursor while retaining indexed prefix timestamp');
SELECT order59_test.check_that(
    (SELECT jsonb_agg(row_data ORDER BY row_data ->> 'create_op_id') FROM prefix_lifecycle_snapshot)
    = (SELECT jsonb_agg(to_jsonb(l) ORDER BY to_jsonb(l) ->> 'create_op_id') FROM order_lifecycle AS l),
    'mock cursor remapping preserves all lifecycle incarnations and outcomes'
);

-- Deliver the next finalized synthetic header to the ordinary reducer.
SELECT hive.context_next_block('btracker_app');
UPDATE hafd.contexts SET irreversible_block = 90000001
WHERE name = 'btracker_app';
TRUNCATE _btracker_ops_batch;
SELECT process_order_lifecycle(90000001, 90000001);
SELECT order59_test.check_that((
    SELECT processed_through = 90000001 AND indexed_at = TIMESTAMP '2021-01-01'
    FROM order_lifecycle_status
), 'prepared empty mock range advances exact watermark and synthetic header timestamp');
SELECT order59_test.check_that(
    (SELECT jsonb_agg(row_data ORDER BY row_data ->> 'create_op_id') FROM prefix_lifecycle_snapshot)
    = (SELECT jsonb_agg(to_jsonb(l) ORDER BY to_jsonb(l) ->> 'create_op_id') FROM order_lifecycle AS l),
    'empty mock range leaves prefix creations and outcomes unchanged'
);

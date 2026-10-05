\set ON_ERROR_STOP on
RESET ROLE;

-- Switch the chain fixture to irreversible 1..4 plus the genuine reversible
-- fork-1 tail 5..6. HAF context views intentionally hide raw irreversible rows
-- above a context's own irreversible cursor.
UPDATE hafd.hive_state SET consistent_block = 4;
INSERT INTO hafd.blocks_reversible SELECT
    b.num,
    b.hash,
    b.prev,
    b.created_at,
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
    b.dhf_interval_ledger,
    1 AS fork_id
FROM hafd.blocks AS b
WHERE b.num IN (5, 6);
DELETE FROM hafd.blocks
WHERE num > 4;

-- Actual HAF operation storage/views, rather than a substituted operations_view.
-- The deliberately unknown cancellation fails in the second committed batch.
INSERT INTO hafd.operations (id, trx_in_block, op_type_id, op_pos, body_value)
SELECT
    hafd.operation_id(fixture.b, fixture.p) AS id,
    0 AS trx_in_block,
    fixture.t AS op_type_id,
    fixture.p AS op_pos,
    fixture.body AS body_value
FROM (
    VALUES
    (1, 1, 5, order59_test.creation('alice', 20, 10)),
    (2, 1, 5, order59_test.creation('bob', 20, 20, 13)),
    (3, 1, 85, order59_test.cancellation('alice', 999, 10))
) AS fixture (b, p, t, body)
INNER JOIN hafd.operation_types AS ot ON fixture.t = ot.id;
INSERT INTO hafd.operations_reversible (id, trx_in_block, op_type_id, op_pos, body_value, fork_id)
VALUES (hafd.operation_id(5, 1), 0, 5, 1, order59_test.creation('alice', 99, 5), 1);

SET ROLE btracker_owner;
CREATE SCHEMA order59_upgrade AUTHORIZATION btracker_owner;
SELECT hive.app_create_context('order59_upgrade', 'order59_upgrade');
SET search_path TO order59_upgrade;
-- A pre-existing projection has an irreversible baseline and reversible tail.
UPDATE hafd.contexts SET current_block_num = 4, irreversible_block = 4
WHERE name = 'order59_upgrade';
CREATE TABLE parent_projection (id INT PRIMARY KEY) INHERITS (order59_upgrade.order59_upgrade);
INSERT INTO parent_projection (id) VALUES (44);
SELECT hive.context_next_block('order59_upgrade');
INSERT INTO parent_projection (id) VALUES (55);
SELECT hive.context_next_block('order59_upgrade');
INSERT INTO parent_projection (id) VALUES (66);
\ir ../../db/order_lifecycle.sql
\ir ../../db/order_lifecycle_backfill.sql
SET ROLE btracker_owner;
SET search_path TO order59_upgrade;
SELECT install_order_lifecycle('order59_upgrade');
SELECT order59_test.check_that((
    SELECT processed_through = 0 AND backfill_required AND backfill_target IS NULL
    FROM order_lifecycle_status
), 'upgrade requires history instead of presenting empty counts');

-- Install the actual API views for the upgraded context too.
\ir ../../backend/endpoint_helpers/shared_functions/sync_status.sql
\ir ../../backend/endpoint_helpers/get_order_stats/views.sql
SET ROLE btracker_owner;
SET search_path TO order59_upgrade;
SELECT order59_test.expect_api_error('SELECT btracker_endpoints.get_order_stats()', 'PT503', 'upgrade API unavailable before backfill');

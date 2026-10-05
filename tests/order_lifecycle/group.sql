\set ON_ERROR_STOP on
SET ROLE btracker_owner;
CREATE SCHEMA order59_parent AUTHORIZATION btracker_owner;
CREATE SCHEMA hafbe_bal AUTHORIZATION btracker_owner;
SELECT hive.app_create_context('order59_parent', 'order59_parent');
SELECT hive.app_create_context('hafbe_bal', 'hafbe_bal');
SELECT hive.app_register('order59_embedded', ARRAY['order59_parent', 'hafbe_bal']::hive.contexts_group);
UPDATE hafd.contexts SET current_block_num = 4, irreversible_block = 4
WHERE name IN ('order59_parent', 'hafbe_bal');
SET search_path TO order59_parent;
CREATE TABLE parent_projection (id int PRIMARY KEY) INHERITS (order59_parent.order59_parent);
INSERT INTO parent_projection (id) VALUES (44);
SELECT
    hive.context_next_block('order59_parent') AS parent_block,
    hive.context_next_block('hafbe_bal') AS tracker_block;
INSERT INTO parent_projection (id) VALUES (55);
SELECT
    hive.context_next_block('order59_parent') AS parent_block,
    hive.context_next_block('hafbe_bal') AS tracker_block;
INSERT INTO parent_projection (id) VALUES (66);
SET search_path TO hafbe_bal;
\ir ../../db/order_lifecycle.sql
\ir ../../db/order_lifecycle_backfill.sql
SET ROLE btracker_owner;
SET search_path TO hafbe_bal;
SELECT install_order_lifecycle('hafbe_bal');
CALL backfill_order_lifecycle('hafbe_bal', 2, 'order59_embedded_lock');
SELECT order59_test.check_that(
    hive.app_get_current_block_num(ARRAY['order59_parent', 'hafbe_bal']::hive.contexts_group) = 4,
    'embedded maintenance rewinds and reattaches whole context group at baseline'
);
SELECT order59_test.check_that(
    hive.app_context_is_attached('order59_parent')
    AND hive.app_context_is_attached('hafbe_bal'), 'embedded parent and tracker are attached together'
);
SELECT order59_test.check_that(
    (SELECT array_agg(id ORDER BY id) = ARRAY[44] FROM order59_parent.parent_projection),
    'embedded parent projection keeps irreversible baseline through actual group HAF rewind'
);
SELECT order59_test.check_that(
    (SELECT count(*) = 2 FROM order_lifecycle)
    AND (SELECT processed_through = 4 AND NOT backfill_required FROM order_lifecycle_status),
    'embedded index covers target independently of parent projection'
);

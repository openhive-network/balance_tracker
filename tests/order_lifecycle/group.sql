\set ON_ERROR_STOP on
RESET ROLE;
CREATE ROLE hafbe_owner IN ROLE btracker_owner;
CREATE SCHEMA hafbe_app AUTHORIZATION hafbe_owner;
SET ROLE hafbe_owner;
SELECT hive.app_create_context('hafbe_app', 'hafbe_app');
SET ROLE btracker_owner;
CREATE SCHEMA hafbe_bal AUTHORIZATION btracker_owner;
SELECT hive.app_create_context('hafbe_bal', 'hafbe_bal');
SET ROLE hafbe_owner;
SELECT hive.app_register('order59_embedded', ARRAY['hafbe_app', 'hafbe_bal']::hive.contexts_group);
UPDATE hafd.contexts SET current_block_num = 4, irreversible_block = 4
WHERE name IN ('hafbe_app', 'hafbe_bal');
SELECT order59_test.check_that(
    (
        SELECT owner = 'hafbe_owner' FROM hafd.contexts
        WHERE name = 'hafbe_app'
    )
    AND (
        SELECT owner = 'btracker_owner' FROM hafd.contexts
        WHERE name = 'hafbe_bal'
    ),
    'embedded fixture uses actual distinct parent and child owners'
);
SELECT order59_test.check_that(
    pg_has_role('hafbe_owner', 'btracker_owner', 'MEMBER')
    AND NOT pg_has_role('btracker_owner', 'hafbe_owner', 'MEMBER'),
    'parent owner can impersonate tracker owner without reverse inheritance'
);
SET search_path TO hafbe_app;
CREATE TABLE parent_projection (id int PRIMARY KEY) INHERITS (hafbe_app.hafbe_app);
INSERT INTO parent_projection (id) VALUES (44);
SELECT
    hive.context_next_block('hafbe_app') AS parent_block,
    hive.context_next_block('hafbe_bal') AS tracker_block;
INSERT INTO parent_projection (id) VALUES (55);
SELECT
    hive.context_next_block('hafbe_app') AS parent_block,
    hive.context_next_block('hafbe_bal') AS tracker_block;
INSERT INTO parent_projection (id) VALUES (66);
SET search_path TO hafbe_bal;
\ir ../../db/order_lifecycle.sql
\ir ../../db/order_lifecycle_backfill.sql
SET ROLE btracker_owner;
SET search_path TO hafbe_bal;
SELECT install_order_lifecycle('hafbe_bal');
-- run.sh invokes the actual maintenance wrapper in a dedicated parent-role
-- session, then group_finished.sql validates the resulting registered tables.

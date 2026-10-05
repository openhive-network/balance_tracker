\set ON_ERROR_STOP on
SET ROLE hafbe_owner;
SET search_path TO hafbe_bal;
SELECT order59_test.check_that(
    hive.app_get_current_block_num(ARRAY['hafbe_app', 'hafbe_bal']::hive.contexts_group) = 4,
    'embedded maintenance rewinds and reattaches whole context group at baseline'
);
SELECT order59_test.check_that(
    hive.app_context_is_attached('hafbe_app')
    AND hive.app_context_is_attached('hafbe_bal'), 'embedded parent and tracker are attached together'
);
SELECT order59_test.check_that(
    (SELECT array_agg(id ORDER BY id) = ARRAY[44] FROM hafbe_app.parent_projection),
    'embedded parent projection keeps irreversible baseline through actual group HAF rewind'
);
SELECT order59_test.check_that(
    (SELECT count(*) = 2 FROM order_lifecycle)
    AND (SELECT processed_through = 4 AND NOT backfill_required FROM order_lifecycle_status),
    'embedded index covers target independently of parent projection'
);

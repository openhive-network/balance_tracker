\set ON_ERROR_STOP on
SET ROLE btracker_owner;
SET search_path TO order59_upgrade;
SELECT order59_test.check_that((
    SELECT processed_through = 0 AND backfill_required AND backfill_target IS NULL
    FROM order_lifecycle_status
), 'pruning and retention preflight failures leave checkpoint unmodified');
SELECT order59_test.check_that(
    hive.app_context_is_attached('order59_upgrade')
    AND hive.app_get_current_block_num('order59_upgrade') = 6, 'retention checked before destructive context detach'
);
SELECT order59_test.check_that(
    (SELECT array_agg(id ORDER BY id) = ARRAY[44, 55, 66] FROM parent_projection),
    'preflight rejection preserves all existing parent projection rows'
);

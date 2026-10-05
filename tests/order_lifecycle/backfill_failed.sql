\set ON_ERROR_STOP on
SET ROLE btracker_owner;
SET search_path TO order59_upgrade;
SELECT order59_test.check_that((
    SELECT
        processed_through = 2 AND backfill_required AND backfill_target = 4
        AND indexed_at = TIMESTAMP '2020-01-02'
    FROM order_lifecycle_status
),
'failed later batch retains atomic checkpoint and fixed irreversible target');
SELECT order59_test.check_that((SELECT count(*) = 2 FROM order_lifecycle), 'failed later batch preserves committed creation prefix');
SELECT order59_test.check_that(
    NOT hive.app_context_is_attached('order59_upgrade')
    AND hive.app_get_current_block_num('order59_upgrade') = 4, 'interrupted maintenance remains detached at recorded target'
);
SELECT order59_test.check_that(
    (SELECT array_agg(id ORDER BY id) = ARRAY[44] FROM parent_projection),
    'actual HAF detach preserves parent baseline and rewinds only reversible projection tail'
);
SELECT order59_test.expect_api_error(
    'SELECT btracker_endpoints.get_order_stats()', 'PT503',
    'partial checkpoint never becomes visible as complete API'
);
RESET ROLE;
UPDATE hafd.operations SET body_value = order59_test.cancellation('alice', 20, 10)
WHERE id = hafd.operation_id(3, 1);

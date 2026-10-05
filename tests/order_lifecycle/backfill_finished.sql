\set ON_ERROR_STOP on
SET ROLE btracker_owner;
SET search_path TO order59_upgrade;
SELECT order59_test.check_that((
    SELECT
        processed_through = 4 AND NOT backfill_required AND backfill_target IS NULL
        AND indexed_at = TIMESTAMP '2020-01-04'
    FROM order_lifecycle_status
),
'resume completes recorded target and readiness atomically');
SELECT order59_test.check_that(
    hive.app_context_is_attached('order59_upgrade')
    AND hive.app_get_current_block_num('order59_upgrade') = 4, 'actual HAF reattach preserves target cursor'
);
SELECT order59_test.check_that(
    (SELECT count(*) = 2 FROM order_lifecycle)
    AND (
        SELECT outcome = 'canceled' FROM order_lifecycle
        WHERE owner_id = 1
    )
    AND (
        SELECT outcome IS NULL FROM order_lifecycle
        WHERE owner_id = 2
    ), 'resume skips committed prefix and processes corrected terminal once'
);
SELECT order59_test.check_that(
    (SELECT array_agg(id ORDER BY id) = ARRAY[44] FROM parent_projection),
    'maintenance leaves existing irreversible projection unchanged'
);
SELECT order59_test.check_that(
    (SELECT (btracker_endpoints.get_order_stats()).created = 2)
    AND (SELECT (btracker_endpoints.get_order_stats()).canceled = 1), 'backfilled API reads actual HAF source-derived outcomes'
);
SELECT order59_test.check_that(
    NOT EXISTS (
        SELECT 1 FROM order_lifecycle
        WHERE order_id = 99
    ),
    'frozen target excludes source operations in later blocks'
);
CALL backfill_order_lifecycle('order59_upgrade', 2, 'order59_upgrade_lock');
SELECT order59_test.check_that((SELECT count(*) = 2 FROM order_lifecycle), 'completed maintenance is idempotent');

-- Prove the same application can then process the first block after B.
SELECT hive.context_next_block('order59_upgrade');
CREATE TEMP TABLE _btracker_ops_batch AS
SELECT
    id,
    block_num,
    op_type_id,
    body_value
FROM operations_view
WHERE block_num = 5;
SELECT process_order_lifecycle(5, 5);
SELECT order59_test.check_that(
    (SELECT count(*) = 3 FROM order_lifecycle)
    AND (SELECT processed_through = 5 FROM order_lifecycle_status), 'normal processing resumes immediately after fixed target'
);

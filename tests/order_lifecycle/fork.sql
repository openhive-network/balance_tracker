\set ON_ERROR_STOP on
SET ROLE btracker_owner;
SET search_path TO order59_test;
SELECT hive.context_back_from_fork('order59_test', 3);
SELECT check_that(
    (SELECT processed_through = 3 FROM order_lifecycle_status),
    'real HAF rewind restores empty-range watermark'
);
SELECT check_that((SELECT count(*) = 16 FROM order_lifecycle), 'real HAF rewind preserves earlier history');

TRUNCATE _btracker_ops_batch;
INSERT INTO _btracker_ops_batch VALUES
(410, 4, 5, creation('bob', 40, 100, 13)),
(420, 4, 57, fill('alice', 11, 100, 'bob', 40, 100)),
(430, 4, 5, creation('alice', 11, 7));
SELECT hive.context_next_block('order59_test');
SELECT process_order_lifecycle(4, 4);
TRUNCATE _btracker_ops_batch;
INSERT INTO _btracker_ops_batch VALUES
(510, 5, 85, cancellation('alice', 11, 7)),
(520, 5, 21, creation('alice', 11, 9));
SELECT hive.context_next_block('order59_test');
SELECT process_order_lifecycle(5, 5);
SELECT check_that(
    (
        SELECT count(*) = 3 FROM order_lifecycle
        WHERE owner_id = 1 AND order_id = 11
    ),
    'reversible branch retains repeated incarnations'
);
SELECT hive.context_back_from_fork('order59_test', 3);
SELECT check_that((SELECT count(*) = 16 FROM order_lifecycle), 'real HAF removes fork creations in reverse event order');
SELECT check_that((
    SELECT outcome IS NULL AND remaining = 100 AND terminal_op_id IS NULL
    FROM order_lifecycle
    WHERE create_op_id = 360
), 'real HAF reactivates original order after removing reused IDs');
SELECT check_that(
    (SELECT processed_through = 3 FROM order_lifecycle_status)
    AND (SELECT indexed_at = TIMESTAMP '2020-01-03' FROM order_lifecycle_status)
    AND hive.app_get_current_block_num('order59_test') = 3, 'real HAF restores data, timestamp and coverage together'
);
SELECT check_that(
    (SELECT (btracker_endpoints.get_order_stats()).created = 16),
    'API reads consistent branch after actual HAF rewind'
);

-- Alternate branch can reuse canonical IDs removed by rewind.
TRUNCATE _btracker_ops_batch;
INSERT INTO _btracker_ops_batch VALUES
(410, 4, 5, creation('bob', 40, 100, 13)),
(420, 4, 57, fill('alice', 11, 100, 'bob', 40, 100)),
(430, 4, 5, creation('alice', 11, 8));
SELECT hive.context_next_block('order59_test');
SELECT process_order_lifecycle(4, 4);
SELECT check_that(
    (SELECT count(*) = 18 FROM order_lifecycle)
    AND (
        SELECT remaining = 8 AND outcome IS NULL FROM order_lifecycle
        WHERE create_op_id = 430
    ),
    'alternate branch replay reconstructs new amounts with no stale fork incarnation'
);
SELECT check_that(
    (SELECT (btracker_endpoints.get_order_stats()).created = 18)
    AND (SELECT processed_through = 4 FROM order_lifecycle_status), 'API resumes exact coverage after alternate branch replay'
);

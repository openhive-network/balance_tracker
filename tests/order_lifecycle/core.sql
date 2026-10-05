\set ON_ERROR_STOP on
SET ROLE btracker_owner;
SET search_path TO order59_test, public;

-- Insert deliberately out of operation order: the reducer must use canonical ID.
INSERT INTO _btracker_ops_batch VALUES
(80, 1, 57, fill('alice', 3, 20, 'bob', 2, 40)),
(20, 1, 5, creation('bob', 1, 200, 13)),
(10, 1, 5, creation('alice', 1, 100)),
(30, 1, 57, fill('alice', 1, 40, 'bob', 1, 80)),
(40, 1, 5, creation('alice', 2, 50)),
(50, 1, 6, '{"owner":"alice","orderid":2}'),
(51, 1, 85, cancellation('alice', 2, 50)),
(60, 1, 21, creation('bob', 2, 40, 13, TRUE)),
(70, 1, 5, creation('alice', 3, 20)),
(90, 1, 5, creation('bob', 3, 30, 13)),
(91, 1, 85, cancellation('bob', 3, 30, 13)),
(100, 1, 21, creation('alice', 4, 12));
SELECT hive.context_next_block('order59_test');
SELECT process_order_lifecycle(1, 1);
SELECT check_that((SELECT count(*) = 7 FROM order_lifecycle), 'all creations survive ordered reduction');
SELECT check_that(
    (
        SELECT remaining = 60 AND outcome IS NULL FROM order_lifecycle
        WHERE create_op_id = 10
    ),
    'partial maker fill remains open'
);
SELECT check_that(
    (
        SELECT remaining = 120 AND outcome IS NULL FROM order_lifecycle
        WHERE create_op_id = 20
    ),
    'partial taker fill remains open'
);
SELECT check_that((
    SELECT outcome = 'canceled' AND terminal_op_id = 51 AND remaining = 50
    FROM order_lifecycle
    WHERE create_op_id = 40
), 'explicit cancel uses virtual terminal and keeps refund');
SELECT check_that((
    SELECT count(*) = 2
    FROM order_lifecycle
    WHERE
        create_op_id IN (60, 70)
        AND outcome = 'filled' AND remaining = 0 AND terminal_op_id = 80
), 'FOK/create2 fills both order sides immediately');
SELECT check_that(
    (
        SELECT outcome = 'canceled' AND terminal_op_id = 91 FROM order_lifecycle
        WHERE create_op_id = 90
    ),
    'expiry virtual cancellation needs no real cancellation'
);

TRUNCATE _btracker_ops_batch;
INSERT INTO _btracker_ops_batch VALUES
(210, 2, 5, creation('bob', 4, 120, 13)),
(220, 2, 57, fill('alice', 1, 60, 'bob', 4, 120)),
(230, 2, 5, creation('alice', 1, 10)),
(240, 2, 6, '{"owner":"alice","orderid":1}'),
(241, 2, 85, cancellation('alice', 1, 10)),
(250, 2, 21, creation('alice', 1, 7)),
(260, 2, 85, cancellation('bob', 1, 120, 13)),
(270, 2, 5, creation('alice', 5, 5)),
(280, 2, 5, creation('alice', 6, 6)),
(290, 2, 5, creation('bob', 5, 9, 13)),
(300, 2, 68, '{"account":"alice","other_affected_accounts":["bob"]}');
SELECT hive.context_next_block('order59_test');
SELECT process_order_lifecycle(2, 2);
SELECT check_that(
    (
        SELECT count(*) = 3 FROM order_lifecycle
        WHERE owner_id = 1 AND order_id = 1
    ),
    'reused IDs retain all incarnations within and across block ranges'
);
SELECT check_that(
    (
        SELECT outcome = 'filled' AND terminal_op_id = 220 FROM order_lifecycle
        WHERE create_op_id = 10
    ),
    'later fill closes original incarnation'
);
SELECT check_that(
    (
        SELECT outcome = 'canceled' AND remaining = 120 FROM order_lifecycle
        WHERE create_op_id = 20
    ),
    'partial fill followed by expiration counts only canceled'
);
SELECT check_that((
    SELECT count(*) = 4
    FROM order_lifecycle
    WHERE
        owner_id = 1 AND terminal_op_id = 300
        AND outcome = 'canceled'
), 'HF23 clears every active order of body.account');
SELECT check_that(
    (
        SELECT outcome IS NULL AND remaining = 9 FROM order_lifecycle
        WHERE create_op_id = 290
    ),
    'HF23 delegatees in other_affected_accounts keep their orders'
);

TRUNCATE _btracker_ops_batch;
INSERT INTO _btracker_ops_batch VALUES
(310, 3, 5, creation('alice', 10, 1)),
(320, 3, 5, creation('bob', 10, 1, 13)),
(330, 3, 57, fill('alice', 10, 0, 'bob', 10, 1)),
(340, 3, 85, cancellation('alice', 10, 1)),
(350, 3, 7, '{}'),
(360, 3, 5, creation('alice', 11, 100));
SELECT hive.context_next_block('order59_test');
SELECT process_order_lifecycle(3, 3);
SELECT check_that((
    SELECT outcome = 'canceled' AND remaining = 1 AND terminal_op_id = 340
    FROM order_lifecycle
    WHERE create_op_id = 310
), 'zero-paying rounded fill followed by authoritative dust refund');
SELECT check_that(
    (
        SELECT outcome = 'filled' AND remaining = 0 FROM order_lifecycle
        WHERE create_op_id = 320
    ),
    'opposite side of a zero-paying fill closes normally'
);
SELECT check_that((SELECT count(*) = 16 FROM order_lifecycle), 'unrelated operations do not create lifecycle rows');
SELECT check_that((SELECT processed_through = 3 FROM order_lifecycle_status), 'coverage advances with all rows');

-- Invalid inputs fail after a valid first creation to expose partial mutations.
TRUNCATE _btracker_ops_batch;
INSERT INTO _btracker_ops_batch VALUES (410, 4, 5, creation('alice', 20, 10)), (420, 4, 85, cancellation('alice', 999, 10));
SELECT expect_atomic_error('SELECT process_order_lifecycle(4,4)', 'No active order', 'unknown virtual cancel');
TRUNCATE _btracker_ops_batch;
INSERT INTO _btracker_ops_batch VALUES (410, 4, 5, creation('alice', 20, 10)), (420, 4, 5, creation('absent', 1, 10));
SELECT expect_atomic_error('SELECT process_order_lifecycle(4,4)', 'Unknown order creator', 'unknown creator');
TRUNCATE _btracker_ops_batch;
INSERT INTO _btracker_ops_batch VALUES (410, 4, 5, creation('alice', 20, 10)), (420, 4, 6, '{"owner":"alice","orderid":20}');
SELECT expect_atomic_error('SELECT process_order_lifecycle(4,4)', 'Missing virtual cancellation', 'real cancellation without virtual terminal');
TRUNCATE _btracker_ops_batch;
INSERT INTO _btracker_ops_batch VALUES (410, 4, 5, creation('alice', 20, 10)), (420, 4, 85, cancellation('alice', 20, 11));
SELECT expect_atomic_error('SELECT process_order_lifecycle(4,4)', 'refund does not match', 'incorrect virtual refund');
TRUNCATE _btracker_ops_batch;
INSERT INTO _btracker_ops_batch VALUES (410, 4, 5, creation('alice', 20, 10)), (420, 4, 57, '{}');
SELECT expect_atomic_error('SELECT process_order_lifecycle(4,4)', 'Malformed order fill', 'missing fill sides');
TRUNCATE _btracker_ops_batch;
INSERT INTO _btracker_ops_batch VALUES (410, 4, 5, creation('alice', 20, 10)), (420, 4, 57, fill('alice', 20, 11, 'bob', 5, 1));
SELECT expect_atomic_error('SELECT process_order_lifecycle(4,4)', 'Invalid fill asset or amount', 'fill exceeds remaining');
TRUNCATE _btracker_ops_batch;
INSERT INTO _btracker_ops_batch VALUES (410, 4, 5, creation('alice', 20, 10)),
(420, 4, 57, fill('alice', 20, 1, 'bob', 5, 1) || jsonb_build_object('open_pays', asset(1, 13)));
SELECT expect_atomic_error('SELECT process_order_lifecycle(4,4)', 'Invalid fill asset or amount', 'fill changes order asset');
TRUNCATE _btracker_ops_batch;
INSERT INTO _btracker_ops_batch VALUES (410, 4, 5, creation('alice', 20, 10)), (410, 4, 5, creation('bob', 20, 10, 13));
SELECT expect_atomic_error('SELECT process_order_lifecycle(4,4)', 'Duplicate or unordered', 'duplicate canonical operation ID');
TRUNCATE _btracker_ops_batch;
INSERT INTO _btracker_ops_batch VALUES (410, 4, 5, creation('alice', 11, 10));
SELECT expect_atomic_error('SELECT process_order_lifecycle(4,4)', 'order_lifecycle_active_key_idx', 'overlapping active incarnation');
TRUNCATE _btracker_ops_batch;
INSERT INTO _btracker_ops_batch VALUES (410, 4, 5, creation('alice', 20, 0));
SELECT expect_atomic_error('SELECT process_order_lifecycle(4,4)', 'Malformed order creation', 'zero initial sell amount');
SELECT expect_atomic_error('SELECT process_order_lifecycle(5,5)', 'must start at 4', 'coverage gap');
SELECT expect_atomic_error('SELECT process_order_lifecycle(3,3)', 'must start at 4', 'duplicate covered range');
SELECT expect_atomic_error('SELECT process_order_lifecycle(0,4)', 'Invalid order lifecycle block range', 'invalid block range');

BEGIN;
TRUNCATE _btracker_ops_batch;
RESET ROLE;
DELETE FROM hafd.blocks
WHERE num = 4;
SET ROLE btracker_owner;
SELECT expect_atomic_error('SELECT process_order_lifecycle(4,4)', 'Missing source timestamp', 'missing indexed block timestamp');
INSERT INTO _btracker_ops_batch VALUES (410, 4, 5, creation('alice', 20, 10));
SELECT expect_atomic_error('SELECT process_order_lifecycle(4,5)', 'Missing creation timestamp', 'missing creation timestamp');
ROLLBACK;

TRUNCATE _btracker_ops_batch;
SELECT hive.context_next_block('order59_test');
SELECT process_order_lifecycle(4, 4);
SELECT check_that((SELECT processed_through = 4 FROM order_lifecycle_status), 'empty block range advances coverage');

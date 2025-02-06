SET search_path TO btracker_app, public;
SET ROLE btracker_owner;

WITH operation_counts AS (
    SELECT 
        op.key as op_type,
        COUNT(*) as count
    FROM hive.operations_view operations
    CROSS JOIN LATERAL jsonb_each(operations.body) op
    WHERE operations.block_num <= 5000000
    AND op.key IN (
        'limit_order_create',
        'escrow_transfer',
        'convert'
    )
    GROUP BY op.key
)
SELECT * FROM operation_counts;

WITH sample_ops AS (
    SELECT 
        op.key as op_type,
        operations.block_num,
        op.value
    FROM hive.operations_view operations
    CROSS JOIN LATERAL jsonb_each(operations.body) op
    WHERE operations.block_num <= 5000000
    AND op.key IN (
        'limit_order_create',
        'escrow_transfer',
        'convert'
    )
    LIMIT 5 
)
SELECT * FROM sample_ops;

SELECT proname, proowner::regrole
FROM pg_proc 
WHERE pronamespace = 'btracker_app'::regnamespace
AND proname LIKE 'process_%';

RESET ROLE;
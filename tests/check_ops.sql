SELECT 
    op.key as operation_type,
    COUNT(*) as count
FROM hive.operations_view ops
CROSS JOIN LATERAL jsonb_each(ops.body) op
WHERE ops.block_num <= 5000000
AND op.key IN ('limit_order_create', 'escrow_transfer', 'convert')
GROUP BY op.key;
# Recurrent Transfer Processing

## Overview

Processes scheduled recurring transfer operations. Uses the full squashing pattern with recursive CTEs to handle complex lifecycle changes within a single block range.

**File**: `db/process_recurrent_transfers.sql`

## Operations Handled

| Operation | Effect |
|-----------|--------|
| `recurrent_transfer` | Create/modify/cancel scheduled transfer |
| `fill_recurrent_transfer` | System executed a scheduled transfer |
| `failed_recurrent_transfer` | System failed to execute (insufficient funds) |

## Tables Updated

| Table | Purpose |
|-------|---------|
| `recurrent_transfers` | Active scheduled transfers with execution state |

## Transfer Lifecycle

```
recurrent_transfer (amount > 0)
  │ Creates scheduled transfer
  ▼
fill_recurrent_transfer (success)
  │ Decrements remaining_executions
  │ Resets consecutive_failures to 0
  ▼
(repeat fills until remaining_executions = 0)
  │
  ▼ OR
failed_recurrent_transfer
  │ Increments consecutive_failures
  │ If consecutive_failures = 10: auto-cancel
  ▼
DELETED from recurrent_transfers
```

### Cancellation Triggers

A transfer is deleted when:
- `recurrent_transfer` with `amount = 0` (user cancel)
- `fill_recurrent_transfer` with `remaining_executions = 0` (completed)
- `failed_recurrent_transfer` with `consecutive_failures = 10` (auto-cancel)
- `failed_recurrent_transfer` with `deleted = true` (explicit system cancel)

## Key Fields

| Field | Description |
|-------|-------------|
| `from_account`, `to_account`, `transfer_id` | Primary key (HF28 introduced pair_id) |
| `amount`, `nai` | Transfer amount and asset type |
| `recurrence` | Hours between executions |
| `remaining_executions` | How many more times to execute |
| `consecutive_failures` | Failure count (resets on success) |
| `memo` | Transfer memo |
| `source_op` | Used to calculate next execution time |

## HF28 pair_id

Before HF28, only one recurrent transfer could exist between accounts. HF28 added `pair_id` (called `transfer_id` in our schema) allowing multiple:

```sql
-- Alice can have multiple recurring transfers to Bob
(alice, bob, transfer_id=0)  -- "rent"
(alice, bob, transfer_id=1)  -- "salary"
```

## Squashing Implementation

### The Challenge

In block range 1000-2000:
```
Block 1050: recurrent_transfer (setup: 10 HIVE, 5 executions)
Block 1200: fill_recurrent_transfer (remaining=4)
Block 1400: failed_recurrent_transfer (consecutive_failures=1)
Block 1600: fill_recurrent_transfer (remaining=3, consecutive_failures reset)
```

Naive: 4 database operations
Squashed: 1 database operation (final state only)

### Implementation Flow

1. **Fetch ops**: MATERIALIZED CTE gets all ops in range
2. **Parse**: CROSS JOIN LATERAL dispatches to appropriate parser
3. **Order**: ROW_NUMBER() assigns sequence per transfer
4. **Fetch existing**: LEFT JOIN to get current DB state
5. **Recursive merge**: Apply operations in order
6. **Extract final**: WHERE rn_desc = 1
7. **UPSERT/DELETE**: Write final state

### source_op Preservation Logic

When user modifies a transfer WITHOUT changing recurrence, preserve original `source_op`:

```sql
CASE
  WHEN next.op_type = recurrent_transfer
   AND next.recurrence = prev.recurrence
   AND NOT prev.delete_transfer THEN
    prev.source_op  -- Keep original (preserves schedule)
  ELSE
    next.source_op  -- Use new (resets schedule)
END
```

**Why?** `source_op` is used to calculate next execution time. If user just changes amount but keeps same schedule, don't reset the timer.

## Edge Cases

1. **Create-cancel-create**: Same transfer_id reused in same range
2. **Fill after cancel**: Fill operation for already-cancelled transfer
3. **Modify during execution**: User modifies while system is filling
4. **Zero remaining after fill**: Immediate deletion

## Debugging Tips

Check active transfers:
```sql
SELECT * FROM recurrent_transfers
WHERE from_account = (SELECT id FROM hive.accounts WHERE name = 'alice');
```

Check if transfer should have been deleted:
```sql
-- Should be empty for completed/cancelled transfers
SELECT * FROM recurrent_transfers
WHERE remaining_executions <= 0 OR consecutive_failures >= 10;
```

## Expansion Rules

When modifying recurrent transfer processing:
- Maintain source_op preservation logic for schedule continuity
- Handle all three operation types in the recursive CTE
- Update deletion logic if new cancellation triggers added

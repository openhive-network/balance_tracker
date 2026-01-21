# Savings Processing

Savings account balance tracking with 3-day withdrawal delay.

## Overview

`process_block_range_savings()` handles savings deposits, withdrawals, interest, and the 3-day withdrawal delay mechanism. It tracks both current balances and pending withdrawal requests.

**File**: [db/process_savings.sql](../../../db/process_savings.sql)

## Key Functions

| Function | Purpose | Location |
|----------|---------|----------|
| `process_block_range_savings(_from, _to, _report_step)` | Main savings processor | `db/process_savings.sql` |
| `process_transfer_to_savings_operation(body)` | Parse deposit operation | `backend/savings.sql` |
| `process_transfer_from_savings_operation(body)` | Parse withdrawal request | `backend/savings.sql` |
| `process_fill_transfer_from_savings_operation(body)` | Parse withdrawal completion | `backend/savings.sql` |
| `process_cancel_transfer_from_savings_operation(body)` | Parse withdrawal cancellation | `backend/savings.sql` |
| `process_interest_operation(body)` | Parse HBD interest payment | `backend/savings.sql` |

## Tables Updated

| Table | Read/Write | Purpose |
|-------|------------|---------|
| `account_savings` | RW | Current savings balance + pending withdrawal count |
| `account_savings_history` | W | Every savings balance change (append-only) |
| `transfer_saving_id` | RW | Tracks pending withdrawals for cancel/fill matching |
| `saving_history_by_day` | RW | Daily savings min/max/end-of-day |
| `saving_history_by_month` | RW | Monthly savings min/max/end-of-month |

## Operations Processed

| Operation | Effect on Savings |
|-----------|-------------------|
| `transfer_to_savings` | +balance (deposit) |
| `transfer_from_savings` | -balance, +pending_requests (start 3-day delay) |
| `fill_transfer_from_savings` | -pending_requests (delay completed) |
| `cancel_transfer_from_savings` | +balance, -pending_requests (cancel restores funds) |
| `interest` | +balance if `is_saved_into_hbd_balance=false` |

## Processing Pattern

The complexity comes from matching cancel/fill operations to their originating `transfer_from_savings`:

```
1. ops                        # Fetch all savings operations
2. filter_interest_ops        # Parse operations, filter interest by is_saved_into_hbd_balance
3. Split by operation type:
   - cancel_and_fill_transfers
   - transfers_from
   - income_transfers
4. Match cancel/fill to original transfer:
   a. join_canceled_transfers_in_query     # Try match within same batch
   b. canceled_transfers_already_inserted  # Look up from transfer_saving_id table
5. transfers_from_canceled_in_query  # Mark which transfers need persistence
6. union_operations                  # Combine all operations
7. Window functions for running balance
8. DML CTEs:
   - delete_all_canceled_or_filled_transfers  # Clean up transfer_saving_id
   - insert_all_new_registered_transfers_from_savings  # Persist pending withdrawals
   - insert_sum_of_transfers  # UPSERT current balance
   - insert_saving_balance_history  # Append to history
   - aggregated_savings_history + insert_by_day/month
```

## Key Patterns

### Matching Cancel/Fill to Original Transfer

When a user cancels a withdrawal, we need the original amount (not in the cancel operation). Two scenarios:

**Same Batch** (optimized path):
```sql
join_canceled_transfers_in_query AS (
  SELECT ...
    (SELECT tf.source_op FROM transfers_from tf
     WHERE tf.account_id = cpo.account_id
       AND tf.request_id = cpo.request_id
       AND tf.source_op < cpo.source_op
     ORDER BY tf.source_op DESC LIMIT 1) AS last_transfer_id
  FROM cancel_and_fill_transfers cpo
)
```

**Previous Batch** (requires table lookup):
```sql
canceled_transfers_already_inserted AS (
  SELECT ...
    (CASE WHEN pct.op_type_id = _op_cancel_transfer_from_savings
          THEN (- tsi.balance) ELSE pct.balance END) AS balance
  FROM prepare_canceled_transfers_already_inserted pct
  JOIN transfer_saving_id tsi ON tsi.request_id = pct.request_id AND tsi.account = pct.account_id
)
```

### Squashing Transfer Requests

If a withdrawal is created and cancelled in the same batch, skip persistence:
```sql
(NOT EXISTS (
  SELECT 1 FROM canceled_transfers_in_query cti
  WHERE cti.last_transfer_id = cp.source_op
)) AS insert_transfer_id_to_table
```

### Interest Operation Filtering

HBD interest can go to regular balance OR stay in savings:
```sql
WHERE
  ov.op_type_id IN (_op_transfer_to_savings, ...) OR
  (ov.op_type_id = _op_interest AND
   (ov.body->'value'->>'is_saved_into_hbd_balance')::BOOLEAN = false)
```

## How to Modify

### Adding Interest Tracking Statistics

1. Create new table for interest history
2. Add CTE to filter interest operations:
   ```sql
   interest_only AS (
     SELECT * FROM filter_interest_ops
     WHERE op_type_id = _op_interest
   )
   ```
3. Add insert CTE for interest tracking

### Changing Withdrawal Delay Period

The 3-day delay is enforced by the blockchain, not btracker. btracker only observes when operations occur.

## Edge Cases

### Race Condition: Same request_id Reuse

A user can reuse request_id after a previous withdrawal completes. The `source_op < cpo.source_op` filter ensures we match to the MOST RECENT transfer_from for that request_id.

### Interest to Savings vs Regular Balance

The `is_saved_into_hbd_balance` flag determines routing:
- `true`: Goes to regular HBD balance (handled by `process_balances.sql`)
- `false`: Stays in savings (handled here)

## Related Processing

- [balances.md](balances.md) - Regular (liquid) balance tracking
- `fill_transfer_from_savings` also affects liquid balance via `process_balances.sql`

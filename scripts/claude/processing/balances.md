# Balance Processing

Core balance tracking for HIVE, HBD, and VESTS tokens.

## Overview

`process_block_range_balances()` is the primary balance processor. It extracts balance-impacting operations from the blockchain, computes running balances using window functions, and maintains both current state and full history.

**File**: [db/process_balances.sql](../../../db/process_balances.sql)

## Key Functions

| Function | Purpose | Location |
|----------|---------|----------|
| `process_block_range_balances(_from, _to)` | Main processor - extracts and updates all balances | `db/process_balances.sql` |
| `hive.get_balance_impacting_operations()` | Returns list of operation types that affect balances | HAF core |
| `hive.get_impacted_balances(hafd._operation_from_jsonb(body), post_hf1)` | Parses operation and returns balance deltas | HAF core |

## Tables Updated

| Table | Read/Write | Purpose |
|-------|------------|---------|
| `current_account_balances` | RW | Current balance state (squashed) |
| `account_balance_history` | W | Every balance change (append-only) |
| `balance_history_by_day` | RW | Daily min/max/end-of-day balance |
| `balance_history_by_month` | RW | Monthly min/max/end-of-month balance |

## Processing Pattern

```
1. balance_impacting_ops      # Get operation type IDs from HAF
2. ops_in_range              # Fetch operations + parse balance deltas via LATERAL
3. group_by_account_nai      # Find unique (account, asset) pairs
4. get_latest_balance        # Fetch previous balance from current_account_balances
5. union_latest_balance_with_impacted_balances  # Combine prev + deltas
6. sum_balances              # Running balance via SUM() OVER window
7. prepare_balance_history   # Add ROW_NUMBER() DESC for "last operation wins"
8. insert_current_account_balances    # UPSERT final state (WHERE rn = 1)
9. insert_account_balance_history     # INSERT all operations to history
10. aggregated_balance_history        # Window functions for day/month aggregates
11. insert_*_by_day/month             # UPSERT with LEAST/GREATEST for min/max
```

## Operations Processed

This processor handles ALL balance-impacting operations via HAF's `get_balance_impacting_operations()`:

| Operation | Effect |
|-----------|--------|
| `transfer` | Move HIVE/HBD between accounts |
| `transfer_to_vesting` | Power up (HIVE → VESTS) |
| `fill_vesting_withdraw` | Weekly power-down payout |
| `claim_reward_balance` | Claim pending rewards |
| `fill_transfer_from_savings` | Complete savings withdrawal |
| `fill_order` | Market order matched |
| `fill_convert_request` | Conversion completed |
| `fill_recurrent_transfer` | Scheduled transfer executed |
| `escrow_release` | Release from escrow |
| Many others... | See HAF documentation |

## Key Patterns

### Running Balance Calculation

```sql
SUM(balance) OVER (
  PARTITION BY account_id, nai
  ORDER BY source_op, balance
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS balance
```

The `balance` column in `ops_in_range` is a **delta** (change), not absolute balance. The window function computes cumulative sum to get actual balance at each operation.

### "Last Operation Wins" Squashing

```sql
ROW_NUMBER() OVER (
  PARTITION BY account_id, nai
  ORDER BY source_op DESC, balance DESC
) AS rn

-- Then: WHERE rn = 1
```

Only the final state (most recent operation) is written to `current_account_balances`. This reduces writes from O(operations) to O(accounts).

### LEAST/GREATEST for Aggregated History

```sql
ON CONFLICT DO UPDATE SET
  min_balance = LEAST(EXCLUDED.min_balance, acc_history.min_balance),
  max_balance = GREATEST(EXCLUDED.max_balance, acc_history.max_balance)
```

When processing overlapping time periods, this ensures true min/max across all batches.

## Edge Cases

### Same source_op, Multiple Deltas

Operations like `escrow_rejected_operation` can produce multiple balance changes for the same asset. The secondary `ORDER BY balance` ensures deterministic ordering.

### Hardfork 1 Handling

```sql
JOIN hafd.applied_hardforks ah ON ah.hardfork_num = 1
CROSS JOIN hive.get_impacted_balances(
  hafd._operation_from_jsonb(ho.body),
  ho.block_num > ah.block_num  -- boolean: past HF1?
)
```

Pre-HF1 operations may need different interpretation.

## How to Modify

### Adding a New Operation Type

Balance-impacting operations are defined in HAF, not btracker. If HAF's `get_balance_impacting_operations()` is updated, btracker will automatically process the new operation type.

### Adding a New History Aggregation (e.g., by_week)

1. Create new table `balance_history_by_week`
2. Add window in `aggregated_balance_history`:
   ```sql
   ROW_NUMBER() OVER w_week_desc AS rn_by_week,
   MIN(balance) OVER w_week_all AS min_balance_week,
   ...
   WINDOW
     w_week_desc AS (PARTITION BY account_id, nai, date_trunc('week', ...) ORDER BY source_op DESC),
     w_week_all AS (PARTITION BY account_id, nai, date_trunc('week', ...))
   ```
3. Add insert CTE with `WHERE rn_by_week = 1`

## Related Processing

- [rewards.md](rewards.md) - Pending reward balance (separate from liquid balance)
- [savings.md](savings.md) - Savings account balance (separate from liquid)
- [delegations.md](delegations.md) - Delegated VESTS (affects available VESTS)

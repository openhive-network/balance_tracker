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
| `account_hbd_interest` | RW | Liquid HBD interest accumulator (`hbd_seconds`), frozen after HF25 |

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

## Liquid HBD Interest Accumulator (`account_hbd_interest`)

A trailing block in `process_balances` maintains the per-account liquid HBD interest
state, mirroring hived `database.cpp` `adjust_hbd_balance` → `evaluate_hbd_interest`.
It reuses the running HBD balance the main pipeline already computed (hung off
`join_created_at_to_balance_history`), so there is no separate processing function and
no extra operation scan.

Per liquid HBD balance change it tracks:
- `hbd_seconds` — Σ(balance × elapsed seconds) since the last interest payment
- `hbd_seconds_last_update` — effective_ts of the latest liquid HBD balance change
- `last_balance` — liquid HBD balance immediately after that change
- `hbd_last_interest_payment` — effective_ts of the latest interest payment

Key rules (all verified against hived):
- **30-day reset:** on a balance change, if `hbd_seconds > 0` AND
  `effective_ts - hbd_last_interest_payment > hbd_interest_compound_interval_sec()` (30 days),
  `hbd_seconds` is zeroed and the payment timestamp advances. This is the chain's rule and
  is **independent of any emitted `interest_operation`** — the chain only emits that vop when
  the rounded interest is non-zero, so dust balances reset silently. The reset is sequentially
  dependent (each reset moves the 30-day anchor), so it is computed in a `WITH RECURSIVE` CTE,
  not a window function.
- **New-account anchor = epoch:** a brand-new account's `hbd_last_interest_payment` (and
  `hbd_seconds_last_update`) seed to **epoch (1970)**, exactly like `account_object` — those
  fields have no initializer and are only written inside `adjust_hbd_balance`. Because the
  anchor is epoch, the first balance change with a positive pre-change balance already has
  `(effective_ts - anchor) > 30 days`, so the chain resets at that op no matter how soon it
  follows the first transfer. Seeding the anchor to the first op's timestamp instead would
  wrongly defer that first reset by 30 days. The first op itself never resets (pre-change
  balance is 0, so `hbd_seconds` stays 0).
- **effective_ts:** transaction ops (`trx_in_block >= 0`) use the previous block's time (head
  time has not advanced yet during tx application); standalone virtual ops (`trx_in_block = -1`)
  use the block's own time. Vops generated inside a transaction inherit its `trx_in_block`.
- **HF25 gate:** accrual is frozen from after HF25; the boundary is inclusive (`<= HF25 block`)
  because hived applies HF25 at the end of its activation block.

Consumers (HAFBE) read `btracker_backend.account_hbd_interest_view` and must add the idle-period
term before applying the witness rate:
`hbd_seconds + last_balance * (now - hbd_seconds_last_update)`.

Tests: the `regression-test` job (`tests/regression/`) compares `hbd_seconds`,
`hbd_seconds_last_update` and `hbd_last_interest_payment` field-by-field against a real
hived `database_api.list_accounts` snapshot (`accounts_dump.json.gz`, ~92k accounts), so
the accumulator is verified for actual parity with the chain (e.g. never-paid accounts
read back as `0 / 1970-01-01`, the epoch anchor). See `tests/regression/sql/02_compare_accounts.sql`.

## Related Processing

- [rewards.md](rewards.md) - Pending reward balance (separate from liquid balance)
- [savings.md](savings.md) - Savings account balance (separate from liquid)
- [delegations.md](delegations.md) - Delegated VESTS (affects available VESTS)

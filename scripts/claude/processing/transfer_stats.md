# Transfer Stats Processing

Transfer volume aggregation by hour, day, and month.

## Overview

`process_transfer_stats()` aggregates transfer operations into time-bucketed statistics. It computes sum, min, max, and count of transfers per asset type per time period.

**File**: [db/process_transfer_stats.sql](../../../db/process_transfer_stats.sql)

## Key Functions

| Function | Purpose | Location |
|----------|---------|----------|
| `process_transfer_stats(_from, _to)` | Main stats processor | `db/process_transfer_stats.sql` |
| `process_transfer(body)` | Parse transfer/recurrent transfer | `backend/transfers.sql` |
| `process_escrow_transfer(body)` | Parse escrow transfer | `backend/escrow.sql` |

## Tables Updated

| Table | Read/Write | Purpose |
|-------|------------|---------|
| `transfer_stats_by_hour` | RW | Hourly aggregates: sum, min, max, count |
| `transfer_stats_by_day` | RW | Daily aggregates |
| `transfer_stats_by_month` | RW | Monthly aggregates |

## Operations Processed

| Operation | Effect |
|-----------|--------|
| `transfer` | Standard HIVE/HBD transfer |
| `fill_recurrent_transfer` | Scheduled transfer executed |
| `escrow_transfer` | Multi-party escrow created |

## Processing Pattern

Uses GROUPING SETS for single-pass multi-level aggregation:

```
1. ops                    # Fetch transfer operations
2. transfer_results       # Parse standard/recurrent transfers
3. escrow_results         # Parse escrow transfers (different JSON)
4. gather_transfers       # UNION ALL both types
5. join_blocks_date       # Add hour/day/month timestamps via blocks_view
6. aggregated_stats       # GROUPING SETS for all three time levels
7. insert_trx_stats_by_*  # UPSERT into each stats table
```

## Key Patterns

### GROUPING SETS for Single-Pass Aggregation

Instead of three separate GROUP BY queries:
```sql
GROUP BY GROUPING SETS (
  (nai, by_hour),   -- grp_level = 3 (binary 011)
  (nai, by_day),    -- grp_level = 5 (binary 101)
  (nai, by_month)   -- grp_level = 6 (binary 110)
)
```

The `GROUPING()` function returns a bitmask:
- Bit 2 (value 4): 1 if by_hour is NULL (aggregated away)
- Bit 1 (value 2): 1 if by_day is NULL
- Bit 0 (value 1): 1 if by_month is NULL

| grp_level | Binary | Meaning |
|-----------|--------|---------|
| 3 | 011 | by_hour grouped, by_day/by_month NULL → hourly |
| 5 | 101 | by_day grouped, by_hour/by_month NULL → daily |
| 6 | 110 | by_month grouped, by_hour/by_day NULL → monthly |

### Incremental Merge with UPSERT

```sql
ON CONFLICT ... DO UPDATE SET
  sum_transfer_amount = trx_agg.sum_transfer_amount + EXCLUDED.sum_transfer_amount,
  max_transfer_amount = GREATEST(EXCLUDED.max_transfer_amount, trx_agg.max_transfer_amount),
  min_transfer_amount = LEAST(EXCLUDED.min_transfer_amount, trx_agg.min_transfer_amount),
  transfer_count = trx_agg.transfer_count + EXCLUDED.transfer_count
```

This enables:
1. Processing overlapping time periods correctly
2. Incremental updates as new blocks arrive
3. Idempotent re-processing

### Time Bucket Extraction

```sql
date_trunc('hour', bv.created_at) AS by_hour,
date_trunc('day', bv.created_at) AS by_day,
date_trunc('month', bv.created_at) AS by_month
```

`date_trunc` normalizes timestamps to bucket boundaries:
- `2024-01-15 14:32:45` → hour: `2024-01-15 14:00:00`
- Same timestamp → day: `2024-01-15 00:00:00`
- Same timestamp → month: `2024-01-01 00:00:00`

## Stats Columns

| Column | Meaning | Merge Strategy |
|--------|---------|----------------|
| `sum_transfer_amount` | Total value transferred | Add |
| `max_transfer_amount` | Largest single transfer | GREATEST |
| `min_transfer_amount` | Smallest single transfer | LEAST |
| `transfer_count` | Number of transfers | Add |
| `last_block_num` | Most recent block processed | Replace |
| `updated_at` | Time bucket (hour/day/month) | PK |
| `nai` | Asset type (HIVE=21, HBD=13) | PK |

## How to Modify

### Adding a New Time Bucket (e.g., by_week)

1. Create `transfer_stats_by_week` table
2. Add `date_trunc('week', bv.created_at) AS by_week` to `join_blocks_date`
3. Add `(nai, by_week)` to GROUPING SETS
4. Add `insert_trx_stats_by_week` CTE with appropriate grp_level filter

The grp_level for a new by_week would be calculated as:
- by_hour aggregated (bit 2 = 1)
- by_day aggregated (bit 1 = 1)
- by_week grouped (bit 0 = 0)
- by_month aggregated (new bit 3 = 1)

### Adding New Transfer Types

To track a new transfer-like operation:
1. Add operation ID lookup in DECLARE
2. Add to ops WHERE clause
3. Create parsing function in backend
4. Add results CTE + include in gather_transfers UNION ALL

### Adding Average Transfer Amount

```sql
aggregated_stats AS MATERIALIZED (
  SELECT
    ...
    SUM(transfer_amount)::NUMERIC / NULLIF(COUNT(*), 0) AS avg_transfer_amount,
    ...
)
```

## Edge Cases

### Empty Block Ranges

If no transfers in range, all counts will be 0 and no rows will be inserted/updated.

### xmax = 0 Detection

```sql
RETURNING (xmax = 0) as is_new_entry
```

This PostgreSQL trick detects INSERT vs UPDATE:
- xmax = 0: Fresh INSERT (no previous transaction touched this row)
- xmax > 0: UPDATE (row was modified)

Useful for monitoring insert vs update ratios.

## Related Processing

- [balances.md](balances.md) - Transfers also affect account balances
- [recurrent_transfers.md](recurrent_transfers.md) - fill_recurrent_transfer lifecycle
- [escrows.md](escrows.md) - escrow_transfer lifecycle

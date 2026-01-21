# Converts Processing

HBD→HIVE conversion request tracking with 3.5-day delay.

## Overview

`process_block_range_converts()` tracks pending HBD→HIVE conversions. When a user initiates a conversion, HBD is locked for 3.5 days before being converted to HIVE at the median price.

**File**: [db/process_block_range_converts.sql](../../../db/process_block_range_converts.sql)

## Key Functions

| Function | Purpose | Location |
|----------|---------|----------|
| `process_block_range_converts(_from, _to, _report_step)` | Main converts processor | `db/process_block_range_converts.sql` |
| `get_convert_request_event(body)` | Parse conversion initiation | `backend/convert_requests.sql` |
| `get_fill_convert_event(body)` | Parse conversion completion | `backend/convert_requests.sql` |

## Tables Updated

| Table | Read/Write | Purpose |
|-------|------------|---------|
| `convert_state` | RW | Pending conversions: owner_id, request_id, nai, remaining, request_block |

## Operations Processed

| Operation | Effect |
|-----------|--------|
| `convert` | Create pending conversion (HBD locked) |
| `fill_convert_request` | Reduce/complete conversion (HBD→HIVE executed) |

**Note**: `collateralized_convert_operation` (HIVE→HBD) is NOT handled here. It uses different mechanics where user receives HBD immediately while HIVE collateral is held.

## Processing Pattern

Follows the standard "squashing" pattern with pre-existing vs new entity handling:

```
1. ops_in_range        # Fetch convert + fill_convert operations
2. events_raw          # Parse via LATERAL + CASE
3. account_ids         # Batch lookup account names → IDs
4. events              # Normalized events with owner_id

5. creates / fills     # Split by operation type

6. PRE-EXISTING (created before this range):
   - create_keys       # Set of conversions created in this range
   - pre_fills         # Fills with no matching create (LEFT JOIN IS NULL)
   - pre_calc          # Calculate new remaining after fills
   - upd_pre           # UPDATE partially filled
   - pre_zero          # Identify fully filled for deletion

7. NEW (created in this range):
   - latest_creates         # Keep only most recent per key
   - fills_after_latest     # Sum fills AFTER the create
   - remaining_calc         # remaining = create_amount - sum_fill

8. survivors           # WHERE remaining > 0
9. ins_new             # UPSERT survivors
10. del_any            # DELETE fully filled
```

## Key Patterns

### Pre-existing vs New Conversions

**Pre-existing**: Created in a previous block range, stored in `convert_state`:
```sql
pre_fills AS (
  SELECT ...
  FROM fills f
  LEFT JOIN create_keys k USING (owner_id, request_id, nai)
  WHERE k.owner_id IS NULL  -- No create in this range = pre-existing
  GROUP BY f.owner_id, f.request_id, f.nai
)
```

**New**: Created in this range:
```sql
fills_after_latest AS (
  SELECT ...
  FROM latest_creates c
  LEFT JOIN fills f ON ... AND f.fill_op_id > c.create_op_id
  GROUP BY c.owner_id, c.request_id, c.nai
)
```

### request_id Reuse

A user can reuse the same request_id after a conversion completes:
```sql
latest_creates AS MATERIALIZED (
  SELECT DISTINCT ON (owner_id, request_id, nai)
    ...
  ORDER BY owner_id, request_id, nai, create_op_id DESC  -- Keep latest
)
```

### Squashing Out Completed Conversions

If a conversion is created AND fully filled in the same range, it's never stored:
```sql
survivors AS (
  SELECT ... FROM remaining_calc WHERE remaining > 0
)
```

## Data Flow Example

```
Block 1000: Alice converts 1000 HBD (request_id=5)
  -> convert_state: (alice, 5, NAI_HBD, remaining=1000, block=1000)

Block 1500 (3.5 days later): fill_convert_request for 1000 HBD
  -> convert_state: DELETE (alice, 5, NAI_HBD)
  -> process_balances handles the actual HBD→HIVE transfer
```

## How to Modify

### Adding Conversion Statistics

1. Create table for conversion metrics
2. Add aggregation CTE before final SELECT:
   ```sql
   conversion_stats AS (
     SELECT
       nai,
       SUM(create_amount) AS total_converted,
       COUNT(*) AS conversion_count
     FROM creates
     GROUP BY nai
   )
   ```
3. Add INSERT CTE for stats table

### Tracking Conversion History

Currently only pending conversions are stored. To add history:
1. Create `convert_history` table
2. Add INSERT CTE for completed conversions (from pre_zero)

## Edge Cases

### Partial Fills

In practice, conversions fill completely in a single operation. The code handles partial fills defensively by summing all fills.

### Same request_id Reuse

If a user completes conversion #5, then creates a new conversion #5, the `create_op_id DESC` ordering ensures only the NEW conversion is tracked.

## Related Processing

- [balances.md](balances.md) - `fill_convert_request` affects actual HBD/HIVE balances
- Collateralized converts (HIVE→HBD) are a separate mechanism not handled here

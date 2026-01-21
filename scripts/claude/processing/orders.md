# Market Order Processing

## Overview

Processes limit order operations for the internal HIVE/HBD market. Notable for handling `fill_order` which returns TWO events per operation (one for each side of the trade).

**File**: `db/process_block_range_orders.sql`

## Operations Handled

| Operation | Effect |
|-----------|--------|
| `limit_order_create` | Place new market order (original version) |
| `limit_order_create2` | Place new market order (with expiration) |
| `fill_order` | Market match - partially or fully fills order(s) |
| `limit_order_cancel` | User cancels their order |
| `limit_order_cancelled` | System auto-cancels (e.g., expired) |

## Tables Updated

| Table | Purpose |
|-------|---------|
| `order_state` | Open market orders with remaining amounts |

## Order Lifecycle

```
limit_order_create/create2
  │ Creates open order
  ▼
fill_order (partial)
  │ Reduces remaining amount
  ▼
fill_order (full) or limit_order_cancel/cancelled
  │
  ▼
DELETED from order_state
```

## fill_order Returns Two Events

A `fill_order` operation represents a trade between two parties. The parser function returns TWO rows:

```sql
-- get_limit_order_fill_events returns SETOF, not single row
SELECT * FROM btracker_backend.get_limit_order_fill_events(body)
-- Returns:
--   Row 1: (owner='alice', order_id=5, amount=100, nai=21)  -- Alice's order
--   Row 2: (owner='bob', order_id=3, amount=100, nai=13)    -- Bob's order
```

### Why UNION ALL in Parser

```sql
CROSS JOIN LATERAL (
  SELECT (...).*
  WHERE o.op_type_id IN (_op_create1, _op_create2, _op_cancel, _op_cancelled)
  UNION ALL
  SELECT * FROM get_limit_order_fill_events(o.body)
  WHERE o.op_type_id = _op_fill
) AS e
```

Create/cancel return exactly 1 row (use CASE....*).
Fill returns 0-2 rows (use SETOF function).

## Key Fields

| Field | Description |
|-------|-------------|
| `owner_id`, `order_id` | Primary key |
| `nai` | Asset being SOLD (21=HIVE, 13=HBD) |
| `remaining` | Amount still available for matching |
| `block_created` | When order was placed |

## Squashing Challenges

### Multiple Fills

Order can have many partial fills in one block range:

```
Block 1000: Alice creates order for 1000 HIVE
Block 1100: Fill 200 HIVE
Block 1200: Fill 300 HIVE
Block 1300: Fill 500 HIVE (order complete)
```

Squashed: Never insert order (created and completed in same range).

### Create-Cancel-Create

Same order_id reused after cancel:

```
Block 1000: Alice creates order #5 for 1000 HIVE
Block 1100: Fill 200 HIVE
Block 1200: Alice cancels order #5
Block 1300: Alice creates order #5 for 500 HIVE (reuse)
Block 1400: Fill 100 HIVE
```

Solution: Track latest create, only count fills AFTER it:

```sql
fills_after_latest AS (
  SELECT c.owner_id, c.order_id, c.nai,
         COALESCE(SUM(f.fill_amount), 0) AS sum_fill_after
  FROM latest_creates c
  LEFT JOIN fills f ON ...
   AND f.fill_op_id > c.create_op_id  -- Only fills AFTER this create
  GROUP BY ...
)
```

## Pre-existing Orders

Orders created BEFORE current block range that get fills/cancels:

```sql
-- Fills for orders NOT created in this range
pre_fills AS (
  SELECT f.*
  FROM fills f
  LEFT JOIN create_keys k USING (owner_id, order_id)
  WHERE k.owner_id IS NULL  -- No create = pre-existing
)
```

Update these in-place rather than squashing:

```sql
UPDATE order_state s
   SET remaining = pc.new_remaining
  FROM pre_calc pc
 WHERE (s.owner_id, s.order_id) = (pc.owner_id, pc.order_id)
   AND pc.new_remaining > 0  -- Still has balance
```

## Deletion Sources

Orders are deleted from `order_state` when:

```sql
del_keys AS (
  SELECT owner_id, order_id FROM del_pre           -- Pre-existing fully filled
  UNION ALL
  SELECT owner_id, order_id FROM pre_canceled      -- Pre-existing cancelled
  UNION ALL
  SELECT owner_id, order_id FROM canceled_after_latest  -- New then cancelled
  UNION ALL
  SELECT owner_id, order_id FROM remaining_calc WHERE remaining <= 0  -- New fully filled
)
```

## Debugging Tips

Check open orders:
```sql
SELECT o.*, av.name
FROM order_state o
JOIN hive.accounts_view av ON o.owner_id = av.id
WHERE av.name = 'alice';
```

Verify no orphaned orders:
```sql
-- Should be empty
SELECT * FROM order_state WHERE remaining <= 0;
```

## Expansion Rules

When modifying order processing:
- Remember fill_order returns TWO events (handle with SETOF function)
- Maintain fill_op_id > create_op_id ordering for reused order_ids
- Update deletion sources if new cancellation types added

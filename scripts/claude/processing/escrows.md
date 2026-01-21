# Escrow Processing

## Overview

Processes multi-party escrow agreements. Escrows involve three parties (sender, receiver, agent) and have complex state transitions including approval, dispute, and partial release.

**File**: `db/process_block_range_escrows.sql`

## Operations Handled

| Operation | Effect |
|-----------|--------|
| `escrow_transfer` | Create escrow with HIVE/HBD amounts and agent fee |
| `escrow_approved` | Agent or receiver approves the escrow |
| `escrow_rejected` | Agent or receiver rejects (before deadline) |
| `escrow_dispute` | Sender or receiver raises dispute (after approval) |
| `escrow_release` | Release funds (partial or full) |

## Tables Updated

| Table | Purpose |
|-------|---------|
| `escrow_state` | Active escrows with balances and flags |
| `escrow_fees` | Pending agent fees (deleted on approval) |

## Escrow Lifecycle

```
escrow_transfer
  │ Creates escrow + fee record
  │ Funds locked, awaiting approval
  ▼
escrow_approved (by agent AND receiver)
  │ Fee transferred to agent (deleted from escrow_fees)
  │ to_approved flag set TRUE
  │ Escrow now "active"
  ▼
(optional) escrow_dispute
  │ disputed flag set TRUE
  │ Only agent can now release funds
  ▼
escrow_release (one or more)
  │ Partial or full fund release
  │ Reduces hive_amount/hbd_amount
  ▼
(when both amounts = 0)
  │
  ▼
DELETED from escrow_state
```

### Alternative Path: Rejection

```
escrow_transfer
  ▼
escrow_rejected (before ratification deadline)
  │ All funds returned to sender
  │ Fee returned to sender
  ▼
DELETED from escrow_state + escrow_fees
```

## Key Fields

### escrow_state

| Field | Description |
|-------|-------------|
| `from_id`, `escrow_id` | Primary key (escrow_id is per-sender) |
| `hive_nai`, `hive_amount` | HIVE locked (21, amount in satoshis) |
| `hbd_nai`, `hbd_amount` | HBD locked (13, amount in satoshis) |
| `source_op` | Creation operation ID |
| `to_approved` | TRUE if receiver has approved |
| `disputed` | TRUE if dispute raised |

### escrow_fees

| Field | Description |
|-------|-------------|
| `from_id`, `escrow_id` | Foreign key to escrow_state |
| `nai`, `fee_amount` | Fee asset and amount for agent |

## State Flags

### to_approved

Set TRUE when `escrow_approved` fires. Once approved:
- Cannot be rejected
- Can be released or disputed
- Fee is paid to agent (deleted from escrow_fees)

### disputed

Set TRUE when `escrow_dispute` fires. Once disputed:
- Only AGENT can release funds (not sender/receiver)
- Agent decides fund split between parties

## Release Amounts

Releases use NEGATIVE amounts (subtracted from escrow):

```sql
-- In escrow_releases CTE
SELECT ...,
  e.hive_amount,  -- This is NEGATIVE
  e.hbd_amount    -- This is NEGATIVE
FROM escrow_releases

-- When aggregating
SUM(hive_amount)  -- Transfer (+100) + Release (-30) + Release (-20) = 50
```

## Squashing Pattern

### In-Range Transfers

Escrow created and fully released in same block range:

```
Block 1000: escrow_transfer (100 HIVE)
Block 1100: escrow_approved
Block 1200: escrow_release (-60 HIVE)
Block 1300: escrow_release (-40 HIVE)
```

Result: Never insert into escrow_state (squashed out).

### Pre-Range Handling

Escrows created before current range:

```sql
-- Releases to pre-existing escrows
releases_to_pre_range_transfers AS (
  SELECT er.*
  FROM escrow_releases er
  LEFT JOIN create_transfers_in_range frt ON ...
  WHERE frt.from_id IS NULL  -- No create = pre-existing
)

-- Join with existing state to compute new amounts
join_escrow_state_pre_range AS (
  SELECT
    ar.from_id, ar.escrow_id,
    es.hive_nai,
    (es.hive_amount + ar.hive_amount) AS hive_amount,  -- ar.hive_amount is negative
    ...
  FROM aggregate_releases ar
  JOIN escrow_state es ON ...
)
```

## Flag Preservation on UPSERT

Flags can only transition FALSE→TRUE, never back:

```sql
ON CONFLICT DO UPDATE SET
  to_approved = escrow_state.to_approved OR EXCLUDED.to_approved,
  disputed    = escrow_state.disputed OR EXCLUDED.disputed
```

## Fee Lifecycle

1. **Created** with `escrow_transfer`
2. **Deleted** on `escrow_approved` (fee paid to agent)
3. **Deleted** on `escrow_rejected` (fee returned to sender)
4. **Deleted** if escrow is deleted (cleanup)

```sql
-- Fees for approved pre-existing escrows
fee_approved_pre_range AS (
  SELECT ea.from_id, ea.escrow_id
  FROM unique_escrow_approves ea
  LEFT JOIN unique_escrow_fees ef ON ...
    AND ef.op_id > ea.op_id  -- Fee created AFTER approval (shouldn't happen)
  WHERE ef.from_id IS NULL
)

-- Delete fees when escrow approved or deleted
union_fee_deletions AS (
  SELECT from_id, escrow_id FROM fee_approved_pre_range
  UNION ALL
  SELECT from_id, escrow_id FROM union_deletions  -- Escrow rejections/completions
)
```

## Debugging Tips

Check active escrows:
```sql
SELECT es.*, av.name as sender
FROM escrow_state es
JOIN hive.accounts_view av ON es.from_id = av.id;
```

Check for orphaned fees:
```sql
-- Fees without matching escrow (should be empty)
SELECT ef.*
FROM escrow_fees ef
LEFT JOIN escrow_state es USING (from_id, escrow_id)
WHERE es.from_id IS NULL;
```

Check disputed escrows:
```sql
SELECT * FROM escrow_state WHERE disputed = TRUE;
```

## Edge Cases

1. **Approval order**: Both agent AND receiver must approve (separate operations)
2. **Partial release**: Can release some HIVE but keep HBD
3. **Zero amounts**: Escrow with only HIVE or only HBD is valid
4. **Re-use escrow_id**: After completion, sender can reuse same escrow_id

## Expansion Rules

When modifying escrow processing:
- Maintain flag OR logic (can only transition to TRUE)
- Handle both escrow_state AND escrow_fees tables
- Release amounts are NEGATIVE (subtracted)
- Consider three-party implications (sender, receiver, agent)

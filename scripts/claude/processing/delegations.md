# Delegation Processing

## Overview

Processes `delegate_vesting_shares` operations to track VESTS delegated between accounts. Handles the HF23 edge case where returned delegations create phantom self-delegations.

**File**: `db/process_delegations.sql`

## Operations Handled

| Operation | Effect |
|-----------|--------|
| `delegate_vesting_shares` | Create/modify/remove delegation from delegator to delegatee |

## Tables Updated

| Table | Purpose |
|-------|---------|
| `current_accounts_delegations` | Active delegation pairs (delegator, delegatee, amount) |

Note: Summary totals (total delegated out, total received) are calculated on-demand from `current_accounts_delegations` via helper functions.

## HF23 Edge Case

At Hard Fork 23, some accounts had their balances "returned" via delegation operations where `delegator == delegatee` (self-delegation). These are NOT real delegations and must be filtered out.

```sql
-- Filter out HF23 phantom self-delegations
WHERE d.delegator != d.delegatee
```

## Data Flow

```
delegate_vesting_shares operation
  │
  ├─> IF delegator == delegatee: SKIP (HF23 artifact)
  │
  ├─> IF amount > 0: CREATE/UPDATE delegation pair
  │   └─> Update current_accounts_delegations
  │
  └─> IF amount == 0: REMOVE delegation
      └─> Delete from current_accounts_delegations
```

## Key SQL Patterns

### Latest Delegation per Pair

Multiple delegation changes between same accounts in one block range:

```sql
SELECT DISTINCT ON (delegator, delegatee)
  delegator, delegatee, vesting_shares
FROM parsed_delegations
ORDER BY delegator, delegatee, source_op DESC
```

### Delegation Summary Aggregation

```sql
-- Delegator side: total given out
SELECT delegator AS account,
       SUM(vesting_shares) AS delegated_out
FROM current_accounts_delegations
GROUP BY delegator

-- Delegatee side: total received
SELECT delegatee AS account,
       SUM(vesting_shares) AS received
FROM current_accounts_delegations
GROUP BY delegatee
```

## Edge Cases

1. **Self-delegation (HF23)**: Filter where delegator == delegatee
2. **Zero amount**: Treat as delegation removal, not update
3. **Multiple changes**: Only keep final state per (delegator, delegatee) pair
4. **Account creation**: Delegatee account may be created by the delegation

## Expansion Rules

When modifying delegation processing:
- Maintain HF23 self-delegation filter
- Update `current_accounts_delegations` (summary totals calculated on-demand)
- Consider impact on effective vesting power calculations

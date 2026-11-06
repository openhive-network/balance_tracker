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
| `account_delegations` | Summary totals (total delegated out, total received) |
| `account_delegation_history` | `account_delegations` totals after every operation that changed them |

## Delegation History

`account_delegation_history` (account, delegated_vests, received_vests, source_op) gets
one row per account and operation that changed its totals, and one for the first operation
ever to touch an account (it creates the account's `account_delegations` row, also when it
changes nothing, e.g. a delegation of 0): the per-operation deltas
`union_delegations` already computes (delegator increases, delegatee changes, returns,
HF23 resets), summed per `(account, source_op)`, and added as a running sum to the
`account_delegations` totals before the batch (the statement still sees them). Its last
row per account equals `account_delegations`. The past-block balances read it
(`scripts/claude/endpoints/balances.md`). Only block processing writes it: an install
upgraded from a version synced before it existed must resync before at-block reads of
`delegated_vests`, `received_vests` and `post_voting_power_vests` are correct (no upgrade
step backfills it). `tests/maintenance/delegation_withdraw_history.sql` checks its rows.

## HF23 Edge Case

At Hard Fork 23, some accounts had their balances "returned" via delegation operations where `delegator == delegatee` (self-delegation). These are NOT real delegations and must be filtered out.

```sql
-- Filter out HF23 phantom self-delegations
WHERE d.delegator != d.delegatee
```

## HF23 Clear (`hardfork_hive_operation`)

hived's `clear_account` removes a cleared account's delegations and its pending
delegation returns (`vesting_delegation_expiration` objects), and emits no
`return_vesting_delegation` for them: the account's `delegated_vests` is 0 right after
its `hardfork_hive_operation`. `union_delegations` resets the account's delegation pairs
at that operation (negative deltas allowed there), and `hf23_dropped_returns` takes off
whatever delegated vests are left (the pending returns), only when nonzero.

Earlier versions kept the pending returns in `delegated_vests` forever (15 accounts on
mainnet, e.g. funtraveller 1,989,220,886,866), which lowered their effective vests in
the RC overflow checks (`rc_delegations.md`). Existing installs:
`repair_hf_23_pending_delegation_returns()` (`db/one_off_repairs.sql`) takes the value
kept at each cleared account's HF23 operation off `account_delegations` and the
account's later history rows, and adds the 0 history row at the operation.

## Data Flow

```
delegate_vesting_shares operation
  │
  ├─> IF delegator == delegatee: SKIP (HF23 artifact)
  │
  ├─> IF amount > 0: CREATE/UPDATE delegation pair
  │   └─> Update current_accounts_delegations
  │   └─> Update account_delegations (both parties)
  │
  └─> IF amount == 0: REMOVE delegation
      └─> Delete from current_accounts_delegations
      └─> Update account_delegations (reduce totals)
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
- Update both `current_accounts_delegations` AND `account_delegations`
- Consider impact on effective vesting power calculations

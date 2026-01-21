# Reward Processing

## Overview

Processes reward operations to track pending (unclaimed) rewards. Uses a recursive CTE to correctly handle claims that span multiple reward types.

**File**: `db/process_rewards.sql`

## Operations Handled

| Operation | Effect |
|-----------|--------|
| `author_reward` | Add to pending author rewards (posting rewards) |
| `curation_reward` | Add to pending curation rewards (voting rewards) |
| `comment_benefactor_reward` | Add to pending benefactor rewards |
| `claim_reward_balance` | Claim (clear) pending rewards, move to liquid balance |

## Tables Updated

| Table | Purpose |
|-------|---------|
| `account_rewards` | Pending unclaimed rewards per account |

Note: When rewards are claimed, `process_balances` also updates `current_account_balances` with the actual tokens received.

## Reward Types

| Field | Source Operation | Token Type |
|-------|-----------------|------------|
| `hbd_rewards` | author_reward | HBD |
| `hive_rewards` | author_reward | HIVE |
| `vesting_rewards` | author_reward, curation_reward, comment_benefactor_reward | VESTS |

## Claim Processing Challenge

The `claim_reward_balance` operation subtracts from pending rewards. But in a single block range, an account might:
1. Earn 100 HIVE reward
2. Earn 50 HIVE reward
3. Claim 120 HIVE

Naive approach fails: can't subtract 120 from 0 (pending balance from DB), then add 150.

### Solution: Recursive Processing

```sql
WITH RECURSIVE reward_calc AS (
  -- Base: Start with DB state (rn=0)
  SELECT account_id, hive_rewards, hbd_rewards, vesting_rewards,
         0 AS rn
  FROM account_rewards WHERE account_id IN (...)

  UNION ALL

  -- Recursive: Apply each operation in order
  SELECT
    prev.account_id,
    prev.hive_rewards + next.hive_delta,
    prev.hbd_rewards + next.hbd_delta,
    prev.vesting_rewards + next.vesting_delta,
    next.rn
  FROM reward_calc prev
  JOIN ordered_ops next ON next.account_id = prev.account_id
                       AND next.rn = prev.rn + 1
)
SELECT * FROM reward_calc WHERE rn = (SELECT MAX(rn) ...)
```

## Data Flow

```
Reward operation (author/curation/benefactor)
  │
  └─> Add to pending rewards for account
      └─> Update account_rewards

claim_reward_balance operation
  │
  ├─> Subtract from pending rewards
  │   └─> Update account_rewards (may go to 0)
  │
  └─> process_balances() handles actual token transfer
      └─> Update current_account_balances
```

## Key SQL Patterns

### Operation Ordering

```sql
SELECT *,
  ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY source_op) AS rn
FROM reward_operations
```

### Lifetime Tracking

Some implementations also track lifetime earned (not just pending):

```sql
-- If tracking lifetime rewards
UPDATE account_rewards SET
  lifetime_hive = lifetime_hive + CASE WHEN op_type = 'claim' THEN claimed_hive ELSE 0 END
```

## Edge Cases

1. **Claim before earn**: In same block range, claim comes first chronologically but references rewards earned later (impossible in reality, but handle gracefully)
2. **Partial claims**: claim_reward_balance can claim partial amounts
3. **Zero claims**: Claim with all zero amounts (no-op, skip)
4. **New accounts**: First reward creates the account_rewards row

## Expansion Rules

When modifying reward processing:
- Maintain operation ordering (source_op) for correct claim handling
- Coordinate with `process_balances` for claim token transfers
- Consider adding lifetime/historical reward tracking

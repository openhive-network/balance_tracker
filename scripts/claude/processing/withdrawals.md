# Withdrawals Processing

Power-down (vesting withdrawal) state tracking with routes and delayed voting.

## Overview

`process_block_range_withdrawals()` handles the power-down lifecycle: initiating withdrawals, routing rules, weekly payouts, and HF24+ delayed voting. It's split into four sections that run as separate queries within the same function.

**File**: [db/process_withdrawals.sql](../../../db/process_withdrawals.sql)

## Key Functions

| Function | Purpose | Location |
|----------|---------|----------|
| `process_block_range_withdrawals(_from, _to, _report_step)` | Main withdrawals processor | `db/process_withdrawals.sql` |
| `process_withdraw_vesting_operation(body, post_hf1, post_hf16, post_hf21)` | Parse withdrawal initiation; rate as hived computes it | `backend/operation_parsers/withdrawals.sql` |
| `process_set_withdraw_vesting_route_operation(body)` | Parse route setup | `backend/operation_parsers/withdrawals.sql` |
| `process_fill_vesting_withdraw_operation_for_withdrawals(body, post_hf1)` | Parse weekly payout | `backend/operation_parsers/withdrawals.sql` |
| `process_reset_withdraw_hf23(body)` | Handle HF23 account reset | `backend/operation_parsers/withdrawals.sql` |
| `process_delayed_voting_operation(body)` | Parse delayed voting event | `backend/operation_parsers/withdrawals.sql` |
| `process_transfer_to_vesting_completed_operation(body)` | Parse power-up completion | `backend/operation_parsers/withdrawals.sql` |

## Tables Updated

| Table | Read/Write | Purpose |
|-------|------------|---------|
| `account_withdraws` | RW | Withdrawal state: to_withdraw, withdrawn, rate, routes count, delayed_vests |
| `account_routes` | RW | Active routing rules (from_account, to_account, percent) |
| `account_withdraw_history` | W | Power-down state (rate, to_withdraw, withdrawn) after every withdraw_vesting, HF23 reset and fill |

## Operations Processed

| Operation | Effect |
|-----------|--------|
| `withdraw_vesting` | Set/update withdrawal: to_withdraw, vesting_withdraw_rate, withdrawn=0 |
| `set_withdraw_vesting_route` | Add/modify/delete routing rule |
| `fill_vesting_withdraw` | Increment withdrawn counter (weekly payout) |
| `hardfork_hive` | HF23: Reset withdrawal state to 0 for affected accounts |
| `transfer_to_vesting_completed` | HF24+: Add to delayed_vests |
| `delayed_voting` | HF24+: Remove from delayed_vests (delay period ended) |

## Processing Sections

The function runs four separate WITH queries:

### Section 1: Power-down state

Handles `withdraw_vesting`, `hardfork_hive` (HF23) and `fill_vesting_withdraw` in one
statement that replays each account's power-down operation by operation, writes the
state after every one to `account_withdraw_history` and the state after the last one to
`account_withdraws`.

- **Timeline**: the state before the batch (`account_withdraws`, as a setting at
  `source_op` 0; a row without `source_op`, created by a route change only, has never had
  a power-down) followed by the batch's operations in `source_op` order.
- **Segments**: `withdraw_vesting` and the HF23 reset set the whole state (rate,
  to_withdraw, withdrawn = 0) and start a segment; the fills up to the next setting pay
  that power-down. Fills before an account's first setting change nothing.
- **Payout**: `withdrawn` = the setting's withdrawn + the segment's running sum of fills.
  The first fill at which `withdrawn >= to_withdraw`, or the last fill of a payout that
  leaves 0 VESTS, ends the power-down: rate, to_withdraw and withdrawn are 0 from there to
  the next setting.
- **Final state**: the last timeline row; `account_withdraws.source_op` is the setting
  that started its segment (a fill keeps it).

A payout that leaves the account with 0 VESTS ends the power-down as hived's
`process_vesting_withdrawals` does (`libraries/chain/database_vesting.cpp`: rate,
`to_withdraw` and `withdrawn` to 0). A payout is all fills of an account in one block;
the VESTS balance after its last fill is `current_account_balances` minus the batch's
later VESTS changes in `_btracker_vests_batch`, so the step runs after
`process_block_range_balances`. Power-downs stored by earlier versions are ended once
per install by `repair_power_downs_without_vests()` (`db/one_off_repairs.sql`).

`account_withdraw_history` is what the past-block balances read
(`scripts/claude/endpoints/balances.md`). An install synced before it existed gets it
from the one-off repair `delegation_and_withdraw_history`, which replays the same
segments over the whole chain from `operations_view` and reads the VESTS after a payout
from `account_balance_history`; `tests/maintenance/delegation_withdraw_history.sql`
compares its rows with block processing's.

### Section 2: Routes

Handles `set_withdraw_vesting_route` operations.

**Complexity**: Routes are keyed by (from_account, to_account). Must track:
- New routes (+1 to withdraw_routes counter)
- Modified routes (0 change to counter)
- Deleted routes (percent=0, -1 to counter)

```sql
CASE
  WHEN prev_percent IS NULL AND percent != 0 THEN 1   -- NEW
  WHEN prev_percent IS NOT NULL AND percent != 0 THEN 0   -- UPDATE
  WHEN prev_percent IS NOT NULL AND percent = 0 THEN -1  -- DELETE
  ELSE 0  -- NOOP
END AS withdraw_routes
```

### Section 2b: HF23 Route Cleanup

Special handling for HF23 (Steem→Hive fork). Certain accounts had ALL routes deleted.

```sql
WHERE gi.source_op > ar.source_op  -- Only delete routes created BEFORE HF23
```

### Section 4: Delayed Voting (HF24+)

Handles three operations affecting `delayed_vests`:
- `fill_vesting_withdraw`: Reduces delayed_vests proportionally
- `transfer_to_vesting_completed`: Adds to delayed_vests
- `delayed_voting`: Removes from delayed_vests

**Recursive CTE for Clamping**: delayed_vests cannot go negative:
```sql
WITH RECURSIVE calculated_delays AS (
  -- Base case
  SELECT ... balance ... WHERE row_num = 1
  UNION ALL
  -- Recursive: clamp to 0
  SELECT ...
    GREATEST(next_cp.balance + prev.balance, 0) AS balance
  FROM calculated_delays prev
  JOIN add_row_number next_cp ON next_cp.row_num = prev.row_num + 1
)
```

## Hardfork Handling

| Hardfork | Block Lookup | Effect |
|----------|--------------|--------|
| HF1 | `_hf_vests_precision_block` | VESTS precision change (×1000000) |
| HF16 | `_hf_withdraw_rate_block` | 104→13 week withdrawal period |
| HF21 | `_hf_withdraw_rate_round_up_block` | Weekly rate rounded up when the amount does not divide evenly |
| HF23 | In operation body | Reset affected accounts |
| HF24 | `_hf_delayed_voting_block` | Delayed voting feature enabled |

## How to Modify

### Adding New Withdrawal Statistics

1. Add column to `account_withdraws` table
2. Update the appropriate section's INSERT/UPDATE

### Tracking Route History

Currently only current routes are stored. To add history:
1. Create `account_routes_history` table
2. Add INSERT CTE before DELETE in route processing

## Edge Cases

### Weekly Rate Rounding

The rate follows hived's `withdraw_vesting_evaluator`: `amount / intervals`, at least 1 for a
non-zero amount, plus 1 from HF21 on when `rate * intervals < amount`. Rates stored by versions
that always rounded down are corrected once per install by `repair_vesting_withdraw_rate()`
(`db/one_off_repairs.sql`, see `scripts/claude/main.md`).

### Same request_id After Completion

User completes power-down, then starts new one. The `source_op > aw.source_op` filter ensures fills only count toward the CURRENT withdrawal.

### HF23 Account Reset

Some accounts had all state zeroed. Routes created AFTER HF23 operation in the same block range are preserved.

### Negative Delayed Vests Prevention

Withdrawals reduce delayed_vests proportionally. Recursive CTE with GREATEST(..., 0) prevents underflow.

## Related Processing

- [balances.md](balances.md) - `fill_vesting_withdraw` affects VESTS balance
- [delegations.md](delegations.md) - Delegated VESTS cannot be withdrawn

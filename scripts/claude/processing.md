# Balance Processing Documentation

## Overview

Balance Tracker processes blockchain operations to maintain accurate account balances. All processing happens in SQL via PL/pgSQL stored procedures that extract data from HAF's `operations_view` and update state tables.

## Entry Point

```
HAFBE (hafbe_app.sql)
  └─> btracker_process_blocks(_context, _block_range, _is_massif_sync)
      ├─> process_balances()        # Core HIVE/HBD/VESTS
      ├─> process_delegations()     # Delegation pairs
      ├─> process_block_range_rc_delegations()  # RC delegation pairs
      ├─> process_rewards()         # Pending rewards
      ├─> process_savings()         # Savings accounts
      ├─> process_withdrawals()     # Power-down state
      ├─> process_block_range_recurrent_transfers()
      ├─> process_block_range_orders()
      ├─> process_block_range_converts()
      ├─> process_block_range_escrows()
      └─> process_transfer_stats()  # Volume aggregation
```

## Processing Functions Inventory

| Function | File | Operations Handled | Tables Updated |
|----------|------|-------------------|----------------|
| `process_balances` | `db/process_balances.sql` | All balance-affecting ops | `current_account_balances`, `account_balance_history` |
| `process_delegations` | `db/process_delegations.sql` | delegate_vesting_shares | `current_accounts_delegations`, `account_delegations` |
| `process_block_range_rc_delegations` | `db/process_rc_delegations.sql` | custom_json (id='rc', delegate_rc) | `current_rc_delegations`, `account_rc_delegations` |
| `process_rewards` | `db/process_rewards.sql` | author_reward, curation_reward, claim_reward_balance | `account_rewards` |
| `process_savings` | `db/process_savings.sql` | transfer_to_savings, transfer_from_savings, fill_transfer_from_savings | `account_savings`, `account_savings_history`, `savings_withdraws` |
| `process_withdrawals` | `db/process_withdrawals.sql` | withdraw_vesting, set_withdraw_vesting_route, fill_vesting_withdraw | `account_withdraws`, `withdraw_routes` |
| `process_block_range_recurrent_transfers` | `db/process_recurrent_transfers.sql` | recurrent_transfer, fill_recurrent_transfer, failed_recurrent_transfer | `recurrent_transfers` |
| `process_block_range_orders` | `db/process_block_range_orders.sql` | limit_order_create/create2, fill_order, limit_order_cancel/cancelled | `order_state` |
| `process_block_range_converts` | `db/process_block_range_converts.sql` | convert, fill_convert_request | `convert_state` |
| `process_block_range_escrows` | `db/process_block_range_escrows.sql` | escrow_transfer, escrow_release, escrow_approved, escrow_rejected, escrow_dispute | `escrow_state`, `escrow_fees` |
| `process_transfer_stats` | `db/process_transfer_stats.sql` | transfer, fill_recurrent_transfer, escrow_transfer | `transfer_stats_by_hour/day/month` |

## Operations Reference

### Operations That Affect Balances

| Operation | Effect | Processing Function |
|-----------|--------|---------------------|
| `transfer` | Move HIVE/HBD between accounts | `process_balances`, `process_transfer_stats` |
| `transfer_to_vesting` | Power up (HIVE → VESTS) | `process_balances` |
| `withdraw_vesting` | Start power-down | `process_withdrawals` |
| `fill_vesting_withdraw` | Weekly power-down payout | `process_balances`, `process_withdrawals` |
| `delegate_vesting_shares` | Delegate/undelegate VESTS | `process_delegations` |
| `claim_reward_balance` | Claim pending rewards | `process_rewards`, `process_balances` |
| `author_reward` | Post/comment reward credited | `process_rewards` |
| `curation_reward` | Voting reward credited | `process_rewards` |
| `comment_benefactor_reward` | Beneficiary reward credited | `process_rewards` |
| `transfer_to_savings` | Move to savings | `process_savings` |
| `transfer_from_savings` | Initiate 3-day withdrawal | `process_savings` |
| `fill_transfer_from_savings` | Complete savings withdrawal | `process_savings`, `process_balances` |
| `recurrent_transfer` | Schedule/modify recurring transfer | `process_block_range_recurrent_transfers` |
| `fill_recurrent_transfer` | Execute scheduled transfer | `process_balances`, `process_block_range_recurrent_transfers` |
| `limit_order_create/create2` | Place market order | `process_block_range_orders` |
| `fill_order` | Market order matched | `process_balances`, `process_block_range_orders` |
| `convert` | Initiate HBD→HIVE conversion | `process_block_range_converts` |
| `fill_convert_request` | Complete conversion (3.5 days) | `process_balances`, `process_block_range_converts` |
| `escrow_transfer` | Create escrow | `process_block_range_escrows` |
| `escrow_release` | Release from escrow | `process_block_range_escrows`, `process_balances` |

## The Squashing Pattern

All processing functions use the "squashing" pattern to efficiently process large block ranges during initial sync. This reduces database writes from O(operations) to O(unique entities).

### Pattern Overview

```
Block Range 1000-2000 contains:
  - Block 1050: alice transfers 10 HIVE to bob
  - Block 1200: alice transfers 5 HIVE to bob
  - Block 1400: bob transfers 3 HIVE to alice

Naive approach: 3 separate updates to account balances
Squashing approach: Compute net change in memory → 1 update each for alice/bob
```

### Implementation

1. **Fetch all operations** in block range using MATERIALIZED CTE
2. **Parse operation bodies** using CROSS JOIN LATERAL with parser functions
3. **Aggregate changes** using SUM/GROUP BY or recursive CTE
4. **Find final state** using ROW_NUMBER() DESC to get "last operation wins"
5. **UPSERT** final state using INSERT...ON CONFLICT DO UPDATE

### Example: Recurrent Transfers

```sql
-- Step 1: Fetch operations
WITH ops AS MATERIALIZED (
  SELECT body, op_type_id, id FROM operations_view
  WHERE block_num BETWEEN _from AND _to
    AND op_type_id IN (recurrent_transfer, fill_recurrent_transfer, failed_recurrent_transfer)
),
-- Step 2: Assign sequence numbers
latest_rec_transfers AS MATERIALIZED (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY from_account, to_account, transfer_id ORDER BY source_op DESC) AS rn_desc,
    ROW_NUMBER() OVER (PARTITION BY from_account, to_account, transfer_id ORDER BY source_op) AS rn_asc
  FROM parsed_ops
),
-- Step 3: Recursive CTE applies operations in order
recursive_transfers AS (
  WITH RECURSIVE calculated AS (
    -- Base: existing state or first operation
    SELECT * FROM ... WHERE rn_asc = 0
    UNION ALL
    -- Recursive: apply each subsequent operation
    SELECT ... FROM calculated prev JOIN ... next ON next.rn_asc = prev.rn_asc + 1
  )
  SELECT * FROM calculated WHERE rn_desc = 1  -- Final state only
)
-- Step 4: UPSERT
INSERT INTO recurrent_transfers SELECT ... FROM recursive_transfers
ON CONFLICT DO UPDATE SET ...
```

## State Management

### Current State vs History

Balance Tracker maintains two types of data:

| Type | Purpose | Examples |
|------|---------|----------|
| **Current State** | Latest values for fast queries | `current_account_balances`, `order_state`, `recurrent_transfers` |
| **History** | Every change for audit/analysis | `account_balance_history`, `transfer_stats_by_hour` |

### Lifecycle Tracking

Some entities have complex lifecycles tracked via state tables:

| Entity | Create Op | Modify Ops | Complete/Delete Ops |
|--------|-----------|------------|---------------------|
| Recurrent Transfer | recurrent_transfer (amount>0) | recurrent_transfer (modify) | fill (remaining=0), failed (consecutive=10), recurrent_transfer (amount=0) |
| Market Order | limit_order_create | fill_order (partial) | fill_order (full), limit_order_cancel |
| Conversion | convert | - | fill_convert_request |
| Escrow | escrow_transfer | escrow_dispute | escrow_release (full), escrow_rejected |

## Detailed Documentation

Each processing area has its own detailed documentation:

| Area | Documentation | Description |
|------|---------------|-------------|
| **Balances** | [balances.md](processing/balances.md) | Core HIVE/HBD/VESTS tracking with history |
| **Delegations** | [delegations.md](processing/delegations.md) | HF23 return-to-self handling |
| **RC Delegations** | [rc_delegations.md](processing/rc_delegations.md) | HF26+ Resource Credit delegations |
| **Rewards** | [rewards.md](processing/rewards.md) | Recursive claim processing for NAI 38 |
| **Savings** | [savings.md](processing/savings.md) | 3-day withdrawal delay matching |
| **Withdrawals** | [withdrawals.md](processing/withdrawals.md) | Power-down with routes and delayed voting |
| **Recurrent Transfers** | [recurrent_transfers.md](processing/recurrent_transfers.md) | Lifecycle squashing |
| **Orders** | [orders.md](processing/orders.md) | Fill event handling (2 rows per fill) |
| **Converts** | [converts.md](processing/converts.md) | HBD→HIVE 3.5-day conversion |
| **Escrows** | [escrows.md](processing/escrows.md) | Multi-party approval flow |
| **Transfer Stats** | [transfer_stats.md](processing/transfer_stats.md) | GROUPING SETS aggregation |

## Common Patterns

### Account Name → ID Lookup

All processing functions batch account lookups for efficiency:

```sql
all_account_names AS (
  SELECT DISTINCT from_account AS account_name FROM parsed_ops
  UNION
  SELECT DISTINCT to_account AS account_name FROM parsed_ops
),
account_ids AS MATERIALIZED (
  SELECT av.name AS account_name, av.id AS account_id
  FROM hive.accounts_view av
  WHERE av.name IN (SELECT account_name FROM all_account_names)
)
```

### Anti-Join Pattern

Used to find pre-existing entities (created before current block range):

```sql
-- Fills for orders created BEFORE this range
pre_fills AS (
  SELECT f.*
  FROM fills f
  LEFT JOIN create_keys k USING (owner_id, order_id)
  WHERE k.owner_id IS NULL  -- No create found = pre-existing
)
```

### GROUPING SETS for Multi-Level Aggregation

`process_transfer_stats` computes hour/day/month aggregates in a single pass:

```sql
GROUP BY GROUPING SETS (
  (nai, by_hour),   -- grp_level = 3
  (nai, by_day),    -- grp_level = 5
  (nai, by_month)   -- grp_level = 6
)
```

## Expansion Rules

When modifying processing:
- Update Operations Reference table if adding/modifying operation handling
- Update Processing Functions Inventory if adding new functions
- Create detailed doc in `processing/` subdirectory for complex areas
- Update main.md Key Files section if adding new processing files

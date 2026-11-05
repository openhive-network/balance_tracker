# Balance Processing Documentation

## Overview

Balance Tracker processes blockchain operations to maintain accurate account balances. All processing happens in SQL via PL/pgSQL stored procedures that extract data from HAF's `operations_view` and update state tables.

## Entry Point

```
HAFBE (hafbe_app.sql)
  └─> btracker_process_blocks(_context, _block_range, _is_massif_sync)
      ├─> process_balances()        # Core HIVE/HBD/VESTS
      ├─> process_block_range_rc_delegations()  # RC delegation pairs (between balances and withdrawals, see below)
      ├─> process_withdrawals()     # Power-down state
      ├─> process_savings()         # Savings accounts
      ├─> process_rewards()         # Pending rewards
      ├─> process_delegations()     # Delegation pairs
      ├─> process_block_range_recurrent_transfers()
      ├─> process_block_range_orders()
      ├─> process_block_range_converts()
      ├─> process_block_range_escrows()
      ├─> process_transfer_stats()  # Volume aggregation
      └─> process_block_range_proposal_payments()  # DHF payments per proposal
```

`btracker_process_blocks()` dispatches on `hive.get_current_stage_name()`:
MASSIVE_PROCESSING runs `btracker_massive_processing()` over the whole range,
LIVE runs `finalize_massive_sync()` (a no-op once done) and then
`btracker_single_processing()`. MASSIVE is not only the initial sync: HAF
re-enters it for any catch-up of more than 101 blocks. Work that is only meant
for the genesis replay, such as the throttled ANALYZE of the history tables, is
gated on `NOT isIndexesCreated()`, not on the stage name (see #64 and
`scripts/claude/main.md`, "Synchronization Stages").
LIVE does not finalize while HAF's own massive sync is still running and the
context is unfinalized and non-forking: it keeps processing massively instead
(`is_massive_sync_pending()`, see `scripts/claude/main.md`, "Processing while
hived replays").

The batch's session temp tables (`_btracker_ops_batch`, `_btracker_clears_batch`,
`_hbd_batch_rows`, `_btracker_vests_batch`, `_rc_delegation_ops`, `_rc_vest_checks`)
are reset through `btracker_reset_batch_table()` (`db/btracker_app.sql`, #118). A
multi-block batch (massive sync) drops and creates them, and `_btracker_ops_batch` is
analyzed. A single-block batch (live) keeps an existing table and empties it with
DELETE and does not analyze it: DROP/CREATE, TRUNCATE and ANALYZE all invalidate
the cached plans of every statement reading the table, and re-planning the process_*
statements cost about 22 ms of each live block. DELETE leaves dead rows that only
VACUUM reclaims (never run on temp tables), so a kept table over 64 pages is
dropped and created again (`_btracker_ops_batch` grows about a page per live block).
The plan cache's default `auto` mode then uses generic plans where they are not
costlier than custom ones. A new per-batch temp table follows the same pattern.

## Processing Functions Inventory

| Function | File | Operations Handled | Tables Updated |
|----------|------|-------------------|----------------|
| `process_balances` | `db/process_balances.sql` | All balance-affecting ops (deltas extracted by `btracker_backend.get_impacted_balances`, see `backend/operation_parsers/impacted_balances.sql`) | `current_account_balances`, `balance_totals`, `account_balance_history`, `balance_history_by_day/month`, `account_hbd_interest` |
| `process_delegations` | `db/process_delegations.sql` | delegate_vesting_shares, account_create_with_delegation, return_vesting_delegation, hardfork_hive (HF23) | `current_accounts_delegations`, `account_delegations`, `account_delegation_history` |
| `process_block_range_rc_delegations` | `db/process_rc_delegations.sql` | custom_json (id='rc', delegate_rc); withdraw_vesting, fill_vesting_withdraw, delegate_vesting_shares of RC delegators (hived's implicit reduction) | `current_rc_delegations`, `account_rc_delegations` |
| `process_rewards` | `db/process_rewards.sql` | author_reward, curation_reward, claim_reward_balance | `account_rewards` |
| `process_savings` | `db/process_savings.sql` | transfer_to_savings, transfer_from_savings, fill_transfer_from_savings | `account_savings`, `account_savings_history`, `savings_withdraws` |
| `process_withdrawals` | `db/process_withdrawals.sql` | withdraw_vesting, set_withdraw_vesting_route, fill_vesting_withdraw | `account_withdraws`, `withdraw_routes`, `account_withdraw_history` |
| `process_block_range_recurrent_transfers` | `db/process_recurrent_transfers.sql` | recurrent_transfer, fill_recurrent_transfer, failed_recurrent_transfer | `recurrent_transfers` |
| `process_block_range_orders` | `db/process_block_range_orders.sql` | limit_order_create/create2, fill_order, limit_order_cancel/cancelled | `order_state`, `order_lifecycle`, `order_stats_by_block`, `order_stats_by_block_group` |
| `process_block_range_converts` | `db/process_block_range_converts.sql` | convert, fill_convert_request | `convert_state` |
| `process_block_range_escrows` | `db/process_block_range_escrows.sql` | escrow_transfer, escrow_release, escrow_approved, escrow_rejected, escrow_dispute | `escrow_state`, `escrow_fees` |
| `process_transfer_stats` | `db/process_transfer_stats.sql` | transfer, fill_recurrent_transfer, escrow_transfer | `transfer_stats_by_hour/day/month` |
| `process_vesting_stats` | `db/process_vesting_stats.sql` | transfer_to_vesting, withdraw_vesting (excl. cancels), fill_vesting_withdraw | `vesting_stats_by_day/month` (global) |
| `process_account_vesting_stats` | `db/process_account_vesting_stats.sql` | same 3 vesting ops (excl. cancels), fanned out to impacted accounts | `account_vesting_history`, `account_vesting_by_day/month` |
| `process_block_range_proposal_payments` | `db/process_proposal_payments.sql` | proposal_pay | `proposal_payment_totals`, `proposal_payments` |

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
| `proposal_pay` | DHF pays a proposal (hourly) | `process_balances`, `process_block_range_proposal_payments` |
| `clear_null_account_balance` | hived empties `null` (burns everything) | `process_balances`, `process_savings`, `process_rewards`, `process_withdrawals` (see Balance-clearing operations) |
| `consolidate_treasury_balance` | hived empties `steem.dao` into `hive.fund` | same as `clear_null_account_balance` |

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
| **History** | Every change for audit/analysis | `account_balance_history`, `transfer_stats_by_hour`, `order_lifecycle` |

### Derived aggregates kept by processing

### Step order and batch staging

The RC delegation step runs right after `process_block_range_balances` and before
`process_block_range_withdrawals` / `process_block_range_delegations`, in
`btracker_massive_processing` (all three call sites) and `btracker_single_processing`.
It replays each RC delegator's vesting at every operation of the batch from
`account_withdraws`, `account_delegations` and `current_accounts_delegations` as they
were before the batch, and from the batch's VESTS changes, which
`process_block_range_balances` stages in the temp table `_btracker_vests_batch`
(like `_hbd_batch_rows`). Moving it after either step would make it count that
step's operations twice (see `scripts/claude/processing/rc_delegations.md`).

`balance_totals` (one row per NAI: `holders` = accounts with `balance > 0`, `total` =
`SUM(balance)`) is updated by `process_block_range_balances` in the same statement that
upserts `current_account_balances`, in massive sync and live alike. The `update_balance_totals`
CTE takes each touched (account, nai)'s balance before the range (the synthetic
`source_op = 0` row of `prepare_balance_history`, 0 for an account new to the asset) and
after it (`rn = 1`), and adds the per-NAI differences. Any new writer of
`current_account_balances` must keep `balance_totals` in step;
`tests/maintenance/balance_totals.sql` checks it after sync, a live block and a fork rollback.

History rows carry `op_type_id`: `get_impacted_balances_batch` returns each emission's
operation type, and `process_savings` already carries it per operation.

### Balance-clearing operations (#61)

hived empties two accounts in consensus, at every block in which they hold anything:
`clear_null_account_balance` burns everything on `null`, and
`consolidate_treasury_balance` moves everything on `steem.dao` (the pre-HF24 treasury)
to `hive.fund`. "Everything" is liquid HIVE/HBD/VESTS, savings, pending rewards and the
delayed votes. Neither op names the account, and HAF's balance collector emits no debit
for it, so `get_impacted_balances_batch` (a port of that collector, guarded by
`tests/parity`) has none either: the stages apply the clear after it.
`btracker_prefetch_operations` resolves the batch's clearing ops once, each with the
account it empties, into the temp table `_btracker_clears_batch`
(`btracker_backend.prefetch_account_clears`, `backend/operation_parsers/hardforks.sql`):
from `_btracker_ops_batch` for a batch of up to 5,000 operations (live), otherwise by
HAF's `(op_type_id, block_num)` index on `operations_view` (massive sync). The account
is a scalar lookup by name, never a join on the computed name (that sorts all of
`hafd.accounts`). The stages read their range of it with
`btracker_backend.account_clears_in_range(_from, _to)`. Code that builds
`_btracker_ops_batch` by hand (tests/maintenance) must call
`prefetch_account_clears` before running these stages.

| Stage | What a clear does |
|-------|-------------------|
| `process_block_range_balances` | A marker row per liquid asset at the op becomes a debit of exactly the running balance there (`apply_account_clears`): the history gets the debit row (op type 65 or 71), and later deltas in the batch start from 0. A clear of an asset held at 0 writes nothing. Only the cleared accounts take this window path; every other row passes straight through. The HBD-interest accumulator sees the debit like any HBD change. |
| `process_block_range_savings` | The same for savings (`apply_savings_clears`). Pending savings withdrawals are not cancelled. |
| `process_block_range_rewards` | `account_rewards` has no history, so the account's pending rewards are zeroed before the statement and only rewards after its last clear in the batch are applied. Lifetime totals (`account_info_rewards`) keep every reward. |
| `process_block_range_withdrawals` | `account_withdraws.delayed_vests` is reset to 0 at the account's last clear of the batch, inside the clamped fold; later delayed ops continue from 0. Only for an account with delayed vests stored or touched in the batch. |

Known limit, not reached on mainnet: hive.fund's side of a consolidate stays as the
collector emits it, liquid only (hived moves savings and pending rewards into hive.fund's
savings and rewards).

**A fresh sync is required.** There is no migration or repair for these clears: an
install synced by a version without them keeps its old `null` and `steem.dao` rows until
btracker is synced again from scratch. `tests/maintenance/cleared_accounts_batch.sql`
checks them (see `scripts/claude/tests.md`).

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
| **Vesting Stats** | [vesting_stats.md](processing/vesting_stats.md) | Power-up / power-down aggregation (day + month) |
| **Proposal Payments** | [proposal_payments.md](processing/proposal_payments.md) | DHF payments per proposal, numbered for paging |

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

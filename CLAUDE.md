# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Balance Tracker is a HAF (Hive Application Framework) application that tracks HBD and Hive balances for Hive blockchain accounts. It demonstrates how to build HAF apps and provides APIs for querying balance history by block or time.

## Architecture

The application uses a three-tier architecture:
- **Database Layer**: PostgreSQL with HAF extensions. All business logic is in SQL stored procedures.
- **API Layer**: PostgREST exposes SQL functions as REST endpoints. Alternatively, a Python backend can be used.
- **Frontend**: React-based web UI (in `gui/`)

Key database schemas:
- `btracker_app`: Core application tables and processing functions
- `btracker_endpoints`: PostgREST-exposed API functions
- `btracker_backend`: Backend helper functions

### Directory Structure

- `db/` - Core SQL: table definitions (`btracker_app.sql`), block processing functions (`process_*.sql`)
- `backend/` - SQL backend helpers: endpoint implementations, utilities, balance history queries
- `endpoints/` - PostgREST API definitions: endpoint SQL and type definitions
- `scripts/` - Shell scripts for installation and CI
- `tests/tavern/` - API integration tests using Tavern framework
- `tests/performance/` - JMeter performance tests

### Database Roles

- `btracker_owner`: Schema owner, runs migrations and block processing
- `btracker_user`: Read-only user for API queries (PostgREST connects as this role)

## Common Commands

All scripts are located in the `scripts/` directory.

### Installation and Setup
```bash
./scripts/install_app.sh    # Set up database schema
./scripts/uninstall_app.sh  # Remove database schema
```

### Block Processing
```bash
./scripts/process_blocks.sh  # Process blocks from HAF
```

### Testing
```bash
# Run Tavern API tests
cd tests/tavern
pytest
```

### Docker
```bash
cd docker
docker compose up -d                    # Start full stack
docker compose --profile swagger up -d  # Include Swagger UI
docker compose down -v                  # Stop and remove data
```

### Linting
SQL files are linted with sqlfluff (PostgreSQL dialect, max line length 150).

## Specialized Agents

This project uses specialized Claude agents via the Task tool. Agent names follow patterns based on their purpose:

- **Developer agent** (`{project-name}-dev`): For implementing features, fixing bugs, refactoring code, and performing coding tasks. Derive the project name from the application (e.g., Balance Tracker → `balance-tracker-dev`).
- **Reviewer agent** (`{project-name}-reviewer`): For verifying code quality, ensuring existing functionality remains intact, and checking for regressions. Use after code changes.
- **GitLab Pipeline Engineer** (`gitlab-pipeline-engineer`): For all git operations including committing, creating/updating MRs, and pipeline analysis. **Always use this agent instead of running glab/git commands directly.**

**Recommended workflow**:
1. Use the dev agent to write code
2. Use the reviewer agent to identify issues
3. Use the dev agent again to fix any issues found
4. Use `gitlab-pipeline-engineer` to commit and create/update MRs

## Development Notes

- HAF submodule is optional for running the app but required for CI tests
- Database connection defaults: localhost:5432, database `haf_block_log`
- PostgREST configuration is in `postgrest.conf`
- Block processing runs as `btracker_owner`, API queries as `btracker_user`

---

## How Balance Tracker Works

### Purpose and Goals

Balance Tracker extracts and indexes balance-related data from the Hive blockchain for fast querying. It tracks:

- **Account Balances**: HIVE, HBD, and VESTS holdings with full history
- **Savings**: Savings account balances with pending withdrawal tracking
- **Rewards**: Pending rewards (author, curation, benefactor) and total earned rewards
- **Delegations**: Vesting share delegations between accounts
- **Withdrawals**: Power-down (vesting withdrawal) state and routes
- **Recurrent Transfers**: Scheduled recurring transfers between accounts
- **Market Orders**: Open limit orders on the internal market
- **Conversions**: Pending HBD↔HIVE conversions
- **Escrows**: Escrow agreements with multi-party approval
- **Transfer Statistics**: Aggregated transfer volume by hour/day/month

### Processing Architecture

The system processes blockchain operations in **block ranges** rather than individual blocks. This "batch processing" approach enables:

1. **Fast sync**: Process thousands of blocks at once during initial sync
2. **Reduced database writes**: Compute final state, write once
3. **Efficient memory usage**: Process operations in-memory before persisting

The main entry point is `btracker_process_blocks()` in [btracker_app.sql](db/btracker_app.sql), which dispatches to specialized processors based on the current sync stage.

### The Squashing Pattern

The core optimization pattern used throughout the codebase is **"squashing"** - computing the final state from multiple operations before writing to the database.

**Example scenario**: An account has 10 balance changes in a block range:
- Without squashing: 10 UPDATE statements
- With squashing: Compute running balance in-memory, 1 UPDATE with final value

**Key techniques**:

1. **MATERIALIZED CTEs**: Force PostgreSQL to compute expensive subqueries once
2. **Window functions**: `SUM() OVER` for running totals, `ROW_NUMBER() OVER ... DESC` to identify the latest operation
3. **UPSERT pattern**: `INSERT ... ON CONFLICT DO UPDATE` for atomic insert-or-update
4. **Batch account lookups**: Collect account names first, lookup IDs in one query

---

## Process Functions Reference

Each `process_*.sql` file handles specific operation types. All follow the same pattern:

### [process_balances.sql](db/process_balances.sql)
**Operations**: All balance-impacting operations (transfers, rewards, vesting, etc.)

**Flow**:
1. `balance_impacting_ops`: Get operation type IDs from `hive.get_balance_impacting_operations()`
2. `ops_in_range`: Call `hive.get_impacted_balances()` for each operation to extract balance deltas
3. `get_latest_balance`: Fetch previous balance from `current_account_balances`
4. `prepare_balance_history`: Compute running balances using `SUM() OVER` window function
5. Insert to `current_account_balances` (latest only, rn=1) and `account_balance_history` (all changes)
6. Aggregate into `balance_history_by_day` and `balance_history_by_month`

**Key insight**: The `balance` column in ops is a **delta** (change), not an absolute value. Running balance is computed via cumulative sum.

### [process_delegations.sql](db/process_delegations.sql)
**Operations**: `delegate_vesting_shares`, `account_create_with_delegation`, `return_vesting_delegation`, `hardfork_hive` (HF23)

**Tables updated**:
- `current_accounts_delegations`: Active delegation pairs (delegator → delegatee)
- `account_delegations`: Summary per account (total received/delegated vests)

**Special handling**:
- HF23 reset all delegations for affected accounts (Steemit Inc)
- `return_vesting_delegation` is a virtual op when delegated vests return after 5-day delay

### [process_savings.sql](db/process_savings.sql)
**Operations**: `transfer_to_savings`, `transfer_from_savings`, `cancel_transfer_from_savings`, `fill_transfer_from_savings`, `interest`

**Tables updated**:
- `account_savings`: Current savings balance
- `account_savings_history`: All savings balance changes
- `transfer_saving_id`: Pending withdrawal requests (tracks request_id for matching cancel/fill)
- `saving_history_by_day` / `saving_history_by_month`: Aggregated history

**Key insight**: Savings withdrawals have a 3-day delay. `transfer_from_savings` creates a pending request; `fill_transfer_from_savings` completes it.

### [process_rewards.sql](db/process_rewards.sql)
**Operations**: `claim_reward_balance`, `author_reward`, `curation_reward`, `comment_reward`, `comment_benefactor_reward`

**Tables updated**:
- `account_rewards`: Pending reward balances (HIVE, HBD, VESTS, VESTS-as-HIVE)
- `account_info_rewards`: Lifetime posting/curation rewards earned

**Complex logic**: NAI 38 (VESTS-as-HIVE equivalent) requires a **recursive CTE** for claims because the claimed HIVE value depends on accumulated balance at claim time. Non-claim accounts use a fast path.

### [process_withdrawals.sql](db/process_withdrawals.sql)
**Operations**: `withdraw_vesting`, `set_withdraw_vesting_route`, `fill_vesting_withdraw`, `transfer_to_vesting_completed`, `delayed_voting`, `hardfork_hive`

**Tables updated**:
- `account_withdraws`: Current power-down state (rate, to_withdraw, withdrawn, delayed_vests)
- `account_routes`: Withdrawal routing rules (send vesting to other accounts)

**Special handling**:
- HF16 changed withdrawal period from 104 weeks to 13 weeks
- HF24 introduced delayed voting (30-day delay before full voting power)
- `delayed_vests` uses recursive CTE to clamp values to >= 0

### [process_recurrent_transfers.sql](db/process_recurrent_transfers.sql)
**Operations**: `recurrent_transfer`, `fill_recurrent_transfer`, `failed_recurrent_transfer`

**Table updated**: `recurrent_transfers` (active scheduled transfers)

**Lifecycle**:
1. User creates recurrent transfer (amount, recurrence, executions)
2. System fills it periodically (decrements remaining_executions)
3. If insufficient funds, consecutive_failures increments
4. After 10 consecutive failures OR all executions complete, transfer is deleted

### [process_transfer_stats.sql](db/process_transfer_stats.sql)
**Operations**: `transfer`, `fill_recurrent_transfer`, `escrow_transfer`

**Tables updated**: `transfer_stats_by_hour`, `transfer_stats_by_day`, `transfer_stats_by_month`

**Optimization**: Uses `GROUPING SETS` to compute hourly, daily, and monthly aggregations in a **single pass** over the data.

### [process_block_range_converts.sql](db/process_block_range_converts.sql)
**Operations**: `convert_operation`, `fill_convert_request_operation`

**Table updated**: `convert_state` (pending HBD→HIVE conversions)

**Note**: Conversions have a 3.5-day delay. Created amount minus fill amount = remaining.

### [process_block_range_orders.sql](db/process_block_range_orders.sql)
**Operations**: `limit_order_create`, `limit_order_create2`, `fill_order`, `limit_order_cancel`, `limit_order_cancelled`

**Table updated**: `order_state` (open market orders)

**Note**: `fill_order` affects TWO orders (both sides of the trade).

### [process_block_range_escrows.sql](db/process_block_range_escrows.sql)
**Operations**: `escrow_transfer`, `escrow_release`, `escrow_approved`, `escrow_rejected`, `escrow_dispute`

**Tables updated**:
- `escrow_state`: Active escrows with HIVE/HBD amounts and flags (approved, disputed)
- `escrow_fees`: Pending agent fees (deleted on approval or rejection)

---

## Key SQL Patterns

### 1. MATERIALIZED CTEs
```sql
WITH ops AS MATERIALIZED (
  SELECT ... FROM operations_view WHERE ...
)
```
Forces PostgreSQL to compute the CTE once and cache results. Critical when a CTE is referenced multiple times.

### 2. LATERAL + CASE for Operation Dispatch
```sql
FROM ops o
CROSS JOIN LATERAL (
  SELECT (
    CASE
      WHEN o.op_type_id = X THEN function_for_x(o.body)
      WHEN o.op_type_id = Y THEN function_for_y(o.body)
    END
  ).*
) AS result
```
Single pass through operations, dispatching to the appropriate parser.

### 3. Running Balance with Window Functions
```sql
SUM(balance_delta) OVER (
  PARTITION BY account_id, nai
  ORDER BY source_op
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS running_balance
```

### 4. "Last Operation Wins" with ROW_NUMBER
```sql
ROW_NUMBER() OVER (
  PARTITION BY account_id, nai
  ORDER BY source_op DESC
) AS rn
-- Then: WHERE rn = 1
```
Identifies the most recent operation per entity for final state extraction.

### 5. UPSERT Pattern
```sql
INSERT INTO table (columns...)
SELECT ...
ON CONFLICT ON CONSTRAINT pk_name DO UPDATE SET
  column1 = EXCLUDED.column1,
  column2 = EXCLUDED.column2
```

### 6. Aggregation Merge with LEAST/GREATEST
```sql
ON CONFLICT DO UPDATE SET
  min_balance = LEAST(EXCLUDED.min_balance, table.min_balance),
  max_balance = GREATEST(EXCLUDED.max_balance, table.max_balance)
```
Correctly merges min/max when processing overlapping time periods.

### 7. Recursive CTE for Dependent Calculations
```sql
WITH RECURSIVE calculated AS (
  SELECT * FROM base WHERE row_num = 1  -- Base case
  UNION ALL
  SELECT ... FROM calculated prev
  JOIN next_row ON next_row.row_num = prev.row_num + 1  -- Recursive case
)
```
Used when each row's value depends on the computed result of the previous row (not just input values).

---

## Data Model Summary

### Current State Tables (Final Balances)
- `current_account_balances` - Latest HIVE/HBD/VESTS balance per account
- `account_savings` - Latest savings balance per account
- `account_rewards` - Pending reward balances
- `account_delegations` - Total delegated/received vests summary
- `current_accounts_delegations` - Active delegation pairs
- `account_withdraws` - Power-down state
- `account_routes` - Withdrawal routing rules
- `recurrent_transfers` - Active scheduled transfers
- `order_state` - Open market orders
- `convert_state` - Pending conversions
- `escrow_state` - Active escrows
- `escrow_fees` - Pending escrow fees

### History Tables (All Changes)
- `account_balance_history` - Every balance change
- `account_savings_history` - Every savings balance change
- `balance_history_by_day` / `balance_history_by_month` - Aggregated balance snapshots
- `saving_history_by_day` / `saving_history_by_month` - Aggregated savings snapshots
- `transfer_stats_by_hour` / `_day` / `_month` - Transfer volume statistics

### Supporting Tables
- `asset_table` - Asset definitions (NAI 13=HBD, 21=HIVE, 37=VESTS)
- `btracker_app_status` - Processing control flag
- `version` - Schema version tracking
- `transfer_saving_id` - Pending savings withdrawal requests

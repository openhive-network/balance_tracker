# Balance Tracker Architecture

## Overview

Balance Tracker extracts and indexes balance-related data from the Hive blockchain for fast querying. It processes blockchain operations in batch ranges, computes running balances, and stores both current state and full history.

## What Balances Are Tracked

| Category | Data Tracked |
|----------|--------------|
| **Liquid Balances** | HIVE, HBD, VESTS with full history |
| **Savings** | Savings balances, pending withdrawal requests |
| **Rewards** | Pending rewards (author, curation, benefactor), lifetime earned |
| **Delegations** | VESTS delegated to/received from other accounts |
| **RC Delegations** | Resource Credits delegated to/received from other accounts |
| **Power-down** | Withdrawal rates, amounts, routing rules |
| **Recurrent Transfers** | Scheduled recurring transfers |
| **Conversions** | Pending HBD↔HIVE conversions (3.5-day delay) |
| **Market Orders** | Open limit orders on internal market |
| **Escrows** | Multi-party escrow agreements |
| **Transfer Stats** | Aggregated transfer volumes (hour/day/month) |
| **Vesting Stats** | Aggregated power-up / power-down volumes (day/month) |

## Directory Structure

```
btracker/
├── db/                           # Core SQL (tables, processing functions)
│   ├── btracker_app.sql          # Main schema, entry point
│   └── process_*.sql             # Balance processing (10 files)
├── backend/                      # SQL helpers
│   ├── aggregated/               # Balance aggregation functions
│   ├── balance_history/          # History query routers
│   ├── endpoint_helpers/         # API helper functions
│   ├── operation_parsers/        # JSONB operation extractors
│   └── utilities/                # Common utilities
├── endpoints/                    # PostgREST API
│   ├── account-balances/         # Balance query endpoints
│   ├── transfers/                # Transfer analytics
│   ├── other/                    # Utility endpoints
│   └── types/                    # OpenAPI type definitions
├── scripts/                      # Installation, CI
│   └── claude/                   # Claude documentation & tools
├── tests/                        # Test suites
│   ├── tavern/                   # API integration tests
│   ├── regression/               # Balance verification
│   └── performance/              # JMeter load tests
└── docker/                       # Docker Compose setup
```

## Database Schemas

| Schema | Role | Purpose |
|--------|------|---------|
| `btracker_app` | `btracker_owner` | Core tables and processing functions |
| `btracker_backend` | `btracker_owner` | Backend helper functions (not exposed) |
| `btracker_endpoints` | `btracker_user` | PostgREST-exposed API functions |

## Naming Conventions

- **Tables**: `current_*` for latest state, `*_history` for changes, `*_by_day/month` for aggregates
- **Functions**: `process_*` for block processing, `get_*` for queries
- **Types**: NAI identifiers (21=HIVE, 13=HBD, 37=VESTS)

## Key Files

| File | Purpose |
|------|---------|
| `db/btracker_app.sql` | Main schema, tables, `btracker_process_blocks()` entry point |
| `db/process_balances.sql` | Core balance tracking (HIVE/HBD/VESTS) |
| `db/process_delegations.sql` | Delegation tracking with HF23 handling |
| `db/process_rewards.sql` | Reward tracking with recursive CTE for claims |
| `endpoints/account-balances/get_account_balances.sql` | Primary balance query endpoint |
| `scripts/install_app.sh` | Schema installation |
| `scripts/process_blocks.sh` | Main processing loop |

## Processing Architecture

### Entry Point

HAFBE calls `btracker_process_blocks(_context, _block_range, false)` for each block range:

```
HAFBE (hafbe_app.sql)
  └─> btracker_process_blocks()
      ├─ MASSIVE_PROCESSING: 10,000-block batches during initial sync
      └─ LIVE: Single blocks during continuous sync
```

### The "Squashing" Pattern

All process functions use batch processing to minimize database writes:

1. **MATERIALIZED CTEs** fetch operations from HAF's `operations_view`
2. **Window functions** compute running balances: `SUM(delta) OVER (PARTITION BY account ORDER BY op)`
3. **ROW_NUMBER() DESC** identifies the "last operation wins" for final state
4. **UPSERT** (`INSERT ... ON CONFLICT DO UPDATE`) writes atomically

**Example**: 10 balance changes → compute final balance in memory → 1 UPDATE

### Synchronization Stages

```
MASSIVE_PROCESSING (blocks 1→5M, 10K batches, synchronous_commit=OFF)
        ↓ at head block
LIVE (new blocks 1 at a time, synchronous_commit=ON)
```

## HAF Integration

Balance Tracker uses HAF for:

- **Context**: `hive.app_create_context('btracker_app')`
- **Table registration**: `hive.app_register_table()` for reversible data
- **Block iteration**: `hive.app_next_iteration()` provides block ranges
- **Operation access**: `hive.operations_view` for blockchain operations
- **Balance extraction**: `btracker_backend.get_impacted_balances()` — a SQL/JSONB port of HAF's C function `hive.get_impacted_balances()`, kept in sync via `tests/parity/impacted_balances_parity.sql` (see `backend/operation_parsers/impacted_balances.sql`)

## HAFBE Integration

### How HAFBE Uses btracker

HAFBE dispatches block ranges to btracker alongside other submodules:

```sql
-- In hafbe_app.sql
PERFORM btracker_process_blocks(_context_btracker, _block_range, false);
```

### What HAFBE Can Query

Any HAFBE endpoint can query btracker data directly:

```sql
-- Current balance
SELECT balance FROM btracker_app.current_account_balances
WHERE account = 123 AND nai = 21;

-- Or use endpoint functions
SELECT * FROM btracker_endpoints.get_account_balances('gtg');
```

### Key Functions for HAFBE

| Function | Returns |
|----------|---------|
| `btracker_endpoints.get_account_balances(_account)` | Full balance snapshot |
| `btracker_endpoints.get_balance_history(_account, _type, ...)` | Balance changes |
| `btracker_endpoints.get_account_delegations(_account)` | Delegation pairs |
| `btracker_endpoints.get_top_holders(_nai, _limit)` | Ranked holders |

For HAFBE documentation, see parent repo's `scripts/claude/`.

## Data Model Summary

### Current State Tables
- `current_account_balances` - Latest HIVE/HBD/VESTS
- `account_savings` - Latest savings balances
- `account_rewards` - Pending reward balances
- `account_delegations` - Total delegated/received VESTS summary
- `current_accounts_delegations` - Active VESTS delegation pairs
- `account_rc_delegations` - Total delegated/received RC summary
- `current_rc_delegations` - Active RC delegation pairs
- `account_withdraws` - Power-down state
- `recurrent_transfers` - Active scheduled transfers
- `order_state` - Open market orders
- `convert_state` - Pending conversions
- `escrow_state` - Active escrows

### History Tables
- `account_balance_history` - Every balance change
- `account_savings_history` - Every savings change
- `balance_history_by_day/month` - Aggregated snapshots
- `transfer_stats_by_hour/day/month` - Volume statistics
- `vesting_stats_by_day/month` - Power-up / power-down statistics (global)
- `account_vesting_history` - Per-account vesting event history (per-impacted-account, with seq_no)
- `account_vesting_by_day/month` - Per-account vesting aggregates

## Common SQL Patterns

### Running Balance with Window Function
```sql
SUM(balance_delta) OVER (
  PARTITION BY account_id, nai ORDER BY source_op
) AS running_balance
```

### Last Operation Wins
```sql
ROW_NUMBER() OVER (
  PARTITION BY account_id, nai ORDER BY source_op DESC
) AS rn
-- Then: WHERE rn = 1
```

### UPSERT Pattern
```sql
INSERT INTO table (cols...) SELECT ...
ON CONFLICT ON CONSTRAINT pk DO UPDATE SET col = EXCLUDED.col
```

## Expansion Rules

When modifying this file:
- Add new tracked data types to "What Balances Are Tracked"
- Update directory structure if adding new folders
- Document new key files and their purposes
- Update HAF/HAFBE integration sections if interfaces change

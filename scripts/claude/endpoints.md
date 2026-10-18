# Balance Tracker API Endpoints

## Overview

Balance Tracker exposes its data via PostgREST, which automatically converts SQL functions in the `btracker_endpoints` schema into REST endpoints. The API provides comprehensive balance information for Hive blockchain accounts.

### API Architecture

```
REST Client → PostgREST → btracker_endpoints.* functions → btracker_backend.* helpers → Core tables
```

- **Endpoint Layer** (`btracker_endpoints`): Thin wrappers that validate input, set cache headers, and delegate to backend
- **Backend Layer** (`btracker_backend`): Complex query logic with MATERIALIZED CTEs and window functions
- **Data Layer** (`btracker_app`): Core tables with current state and history

### Base URL Patterns

| Context | Base URL |
|---------|----------|
| Standalone | `http://localhost:3000/rpc/{function_name}` |
| HAFBE Integrated | `http://localhost:3000/balance-api/{path}` |

## Endpoint Categories

| Category | Documentation | Endpoints |
|----------|---------------|-----------|
| **Balances** | [endpoints/balances.md](endpoints/balances.md) | Account balances, history, aggregation, top holders |
| **Delegations** | [endpoints/delegations.md](endpoints/delegations.md) | Delegation pairs (incoming/outgoing) |
| **Transfers** | [endpoints/transfers.md](endpoints/transfers.md) | Transfer statistics, recurrent transfers |
| **Vesting** | [endpoints/vesting.md](endpoints/vesting.md) | Power-up / power-down history & statistics |
| **Utility** | [endpoints/utility.md](endpoints/utility.md) | Version, sync status, TVL |

## Endpoint Summary

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/accounts/{name}/balances` | GET | Full balance snapshot |
| `/accounts/{name}/balance-history` | GET | Paginated balance changes |
| `/accounts/{name}/aggregated-history` | GET | Daily/monthly/yearly summaries |
| `/accounts/{name}/delegations` | GET | Incoming/outgoing delegations |
| `/accounts/{name}/recurrent-transfers` | GET | Scheduled transfers |
| `/top-holders` | GET | Leaderboard by asset |
| `/transfer-statistics` | GET | Network transfer volume |
| `/vesting-stats` | GET | Network power-up / power-down volume |
| `/accounts/{name}/vesting-history` | GET | Per-account power-up / power-down events |
| `/accounts/{name}/vesting-stats` | GET | Per-account power-up / power-down aggregates |
| `/version` | GET | Git commit hash |
| `/last-synced-block` | GET | Sync status |
| `/total-value-locked` | GET | TVL metrics |

## Common Patterns

### Caching Strategy

| Data Type | Cache Duration | Reason |
|-----------|----------------|--------|
| Irreversible historical | 1 year | Immutable data |
| Live/recent data | 2 seconds | Changes with blocks |
| Sync status | 0 seconds | Real-time accuracy needed |
| Version | 27 hours | Only changes on deploy |

### Block Range Filters

Many endpoints accept `from-block` and `to-block` parameters:

```bash
# By block number
?from-block=5000000&to-block=6000000

# By timestamp
?from-block=2016-09-15%2019:47:21&to-block=2016-10-01%2000:00:00
```

### Pagination

History endpoints use page-based pagination:

```bash
?page=1&page-size=100&direction=desc
```

- `page`: 1-based page number (oldest=1, newest=N)
- `page-size`: Results per page (max 1000)
- `direction`: `asc` (oldest first) or `desc` (newest first)

### Per-row and plan-cache costs

- Build amount objects with `btracker_backend.create_amount_object()`: it is an inlinable `LANGUAGE sql IMMUTABLE` CASE on the NAI, guarded against `asset_table` at install. Do not turn it back into a plpgsql table lookup; the statistics endpoints call it four times per output row.
- Resolve per-row block lookups (e.g. the block at or before a period end) with a `LEFT JOIN LATERAL` in the main statement, not a plpgsql function per row.
- Choose `plan_cache_mode` per function, never globally. Static plpgsql statements whose best plan depends on direction or bounds (`get_account_vesting_history`, `get_balance_history`, `get_top_holders`) set `force_custom_plan`. `get_account_balances` is a series of small account-equality lookups and sets `force_generic_plan` so it plans once per session.
- Do not aggregate `current_account_balances` per request: `/total-value-locked`'s VESTS sum and `/top-holders`' unfiltered liquid count are point reads of `balance_totals`, which block processing maintains. A balance-range filter still counts by scan.
- Read a history row's operation type from its stored `op_type_id` (also on `account_balance_history_view` / `account_savings_history_view`), never by joining `hafd.operations`.
- A new session's first call pays catalog loading, plpgsql compilation and one plan per statement for every relation it reads (about 4 ms against 0.5 ms steady state). Do not add statements or relations that a statement already reading the same source could answer: `get_account_balances` reads the head block's vesting fund and shares in its one `hive.blocks_view` statement instead of calling `hive.get_vesting_balance()`, which would plan the view again.

## HAFBE Integration

HAFBE endpoints can query btracker data directly:

```sql
-- Direct table access
SELECT balance FROM btracker_app.current_account_balances
WHERE account = 123 AND nai = 21;

-- Via endpoint functions
SELECT * FROM btracker_endpoints.get_account_balances('gtg');
```

For HAFBE REST API details, see parent repo's `scripts/claude/endpoints.md`.

## Expansion Rules

When adding new endpoints:
1. Add SQL function to appropriate `endpoints/` subdirectory
2. Create OpenAPI spec comment in the SQL file
3. Add entry to relevant category documentation in `scripts/claude/endpoints/`
4. Update this index if adding a new category

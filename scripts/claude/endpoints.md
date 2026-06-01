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
| **Analytics** | (DAU only for now — see `endpoints/analytics/`) | Network-level engagement metrics |
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
| `/daily-active-users` | GET | Unique submitters and op volume per day/week/month |
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

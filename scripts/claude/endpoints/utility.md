# Utility Endpoints

## Overview

Utility endpoints provide operational information about the Balance Tracker instance: version, sync status, and aggregate metrics. These endpoints are essential for monitoring, health checks, and deployment verification.

## Endpoints

### get_btracker_version

**REST Path**: `GET /version`
**SQL Function**: `btracker_endpoints.get_btracker_version()`
**Source**: `endpoints/other/get_btracker_version.sql`

Returns the Git commit hash of the deployed Balance Tracker version.

#### Parameters

None

#### Returns: TEXT

Git commit SHA-1 hash (40 characters)

#### Example

```sql
-- SQL
SELECT * FROM btracker_endpoints.get_btracker_version();
-- Returns: "c2fed8958584511ef1a66dab3dbac8c40f3518f0"
```

```bash
# REST
curl 'http://localhost:3000/balance-api/version'
```

#### Caching

Long cache (27 hours) - version only changes on deployment.

#### Use Cases

- Deployment verification
- API client compatibility checks
- Debug logging
- Monitoring dashboards

---

### get_btracker_last_synced_block

**REST Path**: `GET /last-synced-block`
**SQL Function**: `btracker_endpoints.get_btracker_last_synced_block()`
**Source**: `endpoints/other/get_btracker_last_synced_block.sql`

Returns the highest block number that Balance Tracker has processed.

#### Parameters

None

#### Returns: INT

Block number of last processed block

#### Example

```sql
-- SQL
SELECT * FROM btracker_endpoints.get_btracker_last_synced_block();
-- Returns: 85000000
```

```bash
# REST
curl 'http://localhost:3000/balance-api/last-synced-block'
```

#### Sync Status Interpretation

Compare with HAF's irreversible block to check sync progress:

```sql
SELECT
  btracker_endpoints.get_btracker_last_synced_block() AS synced,
  hive.app_get_irreversible_block() AS irreversible,
  hive.app_get_irreversible_block() - btracker_endpoints.get_btracker_last_synced_block() AS lag;
```

| Lag | Interpretation |
|-----|----------------|
| 0 | Fully synced |
| 1-100 | Nearly synced (normal lag) |
| 100+ | App is catching up |

#### Caching

No cache (max-age=0) - sync status needs real-time accuracy.

#### Use Cases

- Monitoring dashboards (is the app synced?)
- API client freshness checks
- Load balancer health endpoints
- Data consistency verification

---

### get_total_value_locked

**REST Path**: `GET /total-value-locked`
**SQL Function**: `btracker_endpoints.get_total_value_locked()`
**Source**: `endpoints/other/get_total_value_locked.sql`

Returns blockchain-wide totals for "locked" value (staked assets and savings).

#### Parameters

None

#### Returns: `btracker_backend.total_value_locked`

| Field | Type | Description |
|-------|------|-------------|
| block_num | INT | Block height of the snapshot |
| total_vests | TEXT | Sum of all VESTS (staked HIVE) |
| savings_hive | TEXT | Sum of all HIVE in savings |
| savings_hbd | TEXT | Sum of all HBD in savings |

#### What Is "Locked" Value

| Category | Description | Lock Duration |
|----------|-------------|---------------|
| **VESTS** | Staked HIVE (Hive Power) | 13-week power-down |
| **HIVE Savings** | HIVE in savings accounts | 3-day withdrawal |
| **HBD Savings** | HBD in savings accounts (earns ~20% APR) | 3-day withdrawal |

#### Example

```sql
-- SQL
SELECT * FROM btracker_endpoints.get_total_value_locked();
```

```bash
# REST
curl 'http://localhost:3000/balance-api/total-value-locked'
```

#### Example Response

```json
{
  "block_num": 85000000,
  "total_vests": "448144916705468383",
  "savings_hive": "12345678900000",
  "savings_hbd": "9876543210000"
}
```

#### Caching

2-second cache - totals change with every block as users stake/unstake.

#### Use Cases

- DeFi dashboards showing TVL metrics
- Network health monitoring
- Comparison with other blockchain TVL metrics
- Staking ratio analysis (VESTS vs liquid HIVE)

---

## Data Sources

| Endpoint | Data Source |
|----------|-------------|
| get_btracker_version | `version` table |
| get_btracker_last_synced_block | `hafd.contexts` table (HAF framework) |
| get_total_value_locked | `current_account_balances`, `account_savings` |

## Health Check Pattern

Combine utility endpoints for comprehensive health monitoring:

```sql
SELECT jsonb_build_object(
  'version', btracker_endpoints.get_btracker_version(),
  'synced_block', btracker_endpoints.get_btracker_last_synced_block(),
  'irreversible_block', hive.app_get_irreversible_block(),
  'sync_lag', hive.app_get_irreversible_block() - btracker_endpoints.get_btracker_last_synced_block(),
  'is_synced', btracker_endpoints.get_btracker_last_synced_block() >= hive.app_get_irreversible_block() - 10
) AS health;
```

## Related Documentation

- Type definitions: `endpoints/types/total_value_locked.sql`
- Installation: `scripts/install_app.sh` (sets version)

## How to Add New Utility Endpoint

1. Create SQL file in `endpoints/other/`
2. Add OpenAPI spec comment for PostgREST
3. Create endpoint function in `btracker_endpoints` schema
4. Add entry to this documentation
5. Add Tavern test in `tests/tavern/patterns-mainnet/`

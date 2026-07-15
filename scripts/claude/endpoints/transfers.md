# Transfer Endpoints

## Overview

Transfer endpoints provide network-wide transfer statistics and per-account recurrent (scheduled) transfer information. These endpoints support volume analysis, network activity monitoring, and scheduled payment tracking.

## Endpoints

### get_transfer_statistics

**REST Path**: `GET /transfer-statistics`
**SQL Function**: `btracker_endpoints.get_transfer_statistics(...)`
**Source**: `endpoints/transfers/get_transfer_statistics.sql`

Returns aggregated transfer volume statistics for HIVE or HBD at configurable time granularity.

#### Parameters

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| coin-type | liquid_nai_type | Yes | - | 'HBD' or 'HIVE' (VESTS not transferable) |
| granularity | granularity_hourly | No | 'yearly' | 'hourly', 'daily', 'monthly', or 'yearly' |
| direction | sort_direction | No | 'desc' | 'asc' or 'desc' |
| from-block | TEXT | No | NULL | Start block (number or timestamp) |
| to-block | TEXT | No | NULL | End block (number or timestamp) |

#### Returns: SETOF `btracker_backend.transfer_stats`

| Field | Type | Description |
|-------|------|-------------|
| date | TIMESTAMP | Period start timestamp (start of the bucket; the hour/day/month/year the transfers occurred) |
| total_transfer_amount | TEXT | Sum of all transfer amounts |
| average_transfer_amount | TEXT | Mean transfer size |
| maximum_transfer_amount | TEXT | Largest single transfer |
| minimum_transfer_amount | TEXT | Smallest single transfer |
| transfer_count | INT | Number of transfers |
| last_block_num | INT | Reference block for period |

#### What Transfers Are Tracked

| Operation | Included | Description |
|-----------|----------|-------------|
| `transfer` | Yes | Direct account-to-account transfers |
| `fill_recurrent_transfer` | Yes | Executed scheduled transfers |
| `escrow_transfer` | Yes | Funds locked in escrow |
| `fill_order` | No | Market trades (not transfers) |
| `transfer_to_vesting` | No | Power-ups (tracked in balance history) |

#### Examples

```sql
-- SQL: Monthly HBD transfer volume
SELECT * FROM btracker_endpoints.get_transfer_statistics(
  'HBD', 'monthly', 'desc', NULL, NULL
);
```

```bash
# REST: Daily HIVE transfers
curl 'http://localhost:3000/balance-api/transfer-statistics?coin-type=HIVE&granularity=daily'
```

#### Example Response

```json
[
  {
    "date": "2024-01-01T00:00:00",
    "total_transfer_amount": "69611921266",
    "average_transfer_amount": "1302405",
    "maximum_transfer_amount": "18000000",
    "minimum_transfer_amount": "1",
    "transfer_count": 54665,
    "last_block_num": 80000000
  }
]
```

#### Use Cases

- Network activity dashboards
- Volume analysis for trading decisions
- Exchange integration (deposit/withdrawal monitoring)
- Economic analysis and reporting

---

### get_recurrent_transfers

**REST Path**: `GET /accounts/{account-name}/recurrent-transfers`
**SQL Function**: `btracker_endpoints.get_recurrent_transfers(account-name TEXT)`
**Source**: `endpoints/account-balances/get_recurrent_transfers.sql`

Returns all active recurrent (scheduled) transfers for an account.

#### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| account-name | TEXT | Yes | Hive account name |

#### Returns: `btracker_backend.recurrent_transfers`

| Field | Type | Description |
|-------|------|-------------|
| outgoing_recurrent_transfers | outgoing_recurrent_transfers[] | Transfers FROM this account |
| incoming_recurrent_transfers | incoming_recurrent_transfers[] | Transfers TO this account |

Each recurrent transfer record:

| Field | Type | Description |
|-------|------|-------------|
| from/to | TEXT | Counterparty account name |
| pair_id | INT | Unique identifier for sender-receiver pair |
| amount | JSONB | {nai, amount, precision} object |
| consecutive_failures | INT | Recent failed execution count |
| remaining_executions | INT | Transfers left to execute |
| recurrence | INT | Hours between executions |
| memo | TEXT | Optional transfer memo |
| trigger_date | TIMESTAMP | When next execution occurs |
| operation_id | TEXT | Creation transaction reference |
| block_num | INT | Block of last modification |

#### Examples

```sql
-- SQL: Get scheduled transfers for blocktrades
SELECT * FROM btracker_endpoints.get_recurrent_transfers('blocktrades');
```

```bash
# REST
curl 'http://localhost:3000/balance-api/accounts/blocktrades/recurrent-transfers'
```

#### Recurrent Transfer Mechanics (HF25+)

1. User creates recurrent transfer specifying:
   - Amount and asset (HBD or HIVE)
   - Recurrence interval (in hours)
   - Number of executions
2. System automatically executes at specified interval
3. If sender has insufficient funds, `consecutive_failures` increments
4. Transfer is deleted after:
   - All executions complete (`remaining_executions` = 0)
   - 10 consecutive failures
   - User cancels (amount = 0)

#### Sorting

Results sorted by `trigger_date` ascending (soonest-to-execute first).

---

## Data Sources

| Endpoint | Primary Tables |
|----------|---------------|
| get_transfer_statistics | transfer_stats_by_hour, transfer_stats_by_day, transfer_stats_by_month |
| get_recurrent_transfers | recurrent_transfers |

## Related Documentation

- Processing (statistics): [../processing/transfer_stats.md](../processing/transfer_stats.md)
- Processing (recurrent): [../processing/recurrent_transfers.md](../processing/recurrent_transfers.md)
- Type definitions: `endpoints/types/transfer_stats.sql`, `endpoints/types/recurrent_transfers.sql`

## How to Add New Transfer Endpoint

1. Create SQL file in `endpoints/transfers/`
2. Add OpenAPI spec comment for PostgREST
3. Create endpoint function in `btracker_endpoints` schema
4. Create backend helper in `btracker_backend` schema if complex
5. Add entry to this documentation
6. Add Tavern test in `tests/tavern/patterns-mainnet/`

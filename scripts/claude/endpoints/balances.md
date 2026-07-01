# Balance Endpoints

## Overview

Balance endpoints provide comprehensive account balance information including liquid balances, vesting shares, rewards, savings, and more. These are the primary data access points for Balance Tracker.

## Endpoints

### get_account_balances

**REST Path**: `GET /accounts/{account-name}/balances`
**SQL Function**: `btracker_endpoints.get_account_balances(account-name TEXT)`
**Source**: `endpoints/account-balances/get_account_balances.sql`

Returns comprehensive balance information for a single Hive account.

#### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| account-name | TEXT | Yes | Hive account name |

#### Returns: `btracker_backend.balance`

| Field | Type | Description |
|-------|------|-------------|
| hbd_balance | BIGINT | HBD liquid balance |
| hive_balance | BIGINT | HIVE liquid balance |
| vesting_shares | TEXT | VESTS balance (as string for precision) |
| vesting_balance_hive | BIGINT | VESTS converted to HIVE equivalent |
| post_voting_power_vests | TEXT | Effective voting power (own VESTS - delegated + received) |
| delegated_vests | TEXT | VESTS delegated to others |
| received_vests | TEXT | VESTS received from others |
| curation_rewards | TEXT | Lifetime curation rewards in VESTS |
| posting_rewards | TEXT | Lifetime posting rewards in VESTS |
| hbd_rewards | BIGINT | Pending unclaimed HBD rewards |
| hive_rewards | BIGINT | Pending unclaimed HIVE rewards |
| vests_rewards | TEXT | Pending unclaimed VESTS rewards |
| hive_vesting_rewards | BIGINT | Pending VESTS rewards in HIVE equivalent |
| hbd_savings | BIGINT | HBD savings balance |
| hive_savings | BIGINT | HIVE savings balance |
| savings_withdraw_requests | INT | Count of pending savings withdrawals |
| vesting_withdraw_rate | TEXT | Weekly power-down payout amount |
| to_withdraw | TEXT | Total VESTS remaining in power-down |
| withdrawn | TEXT | Total VESTS already withdrawn |
| withdraw_routes | INT | Number of power-down routing rules |
| delayed_vests | TEXT | VESTS blocked by power-down |
| conversion_pending_amount_hbd | BIGINT | HBD in pending conversions |
| conversion_pending_count_hbd | INT | Count of HBD conversion requests |
| conversion_pending_amount_hive | BIGINT | HIVE in pending conversions |
| conversion_pending_count_hive | INT | Count of HIVE conversion requests |
| open_orders_hbd_count | INT | Count of open HBD market orders |
| open_orders_hive_count | INT | Count of open HIVE market orders |
| open_orders_hive_amount | BIGINT | Total HIVE in open orders |
| open_orders_hbd_amount | BIGINT | Total HBD in open orders |
| savings_pending_amount_hbd | BIGINT | HBD in pending savings withdrawals |
| savings_pending_amount_hive | BIGINT | HIVE in pending savings withdrawals |
| escrow_pending_amount_hbd | BIGINT | HBD locked in escrows |
| escrow_pending_amount_hive | BIGINT | HIVE locked in escrows |
| escrow_pending_count | INT | Count of active escrows |

#### Examples

```sql
-- SQL
SELECT * FROM btracker_endpoints.get_account_balances('blocktrades');
```

```bash
# REST
curl -X GET 'http://localhost:3000/balance-api/accounts/blocktrades/balances'
```

#### Architecture Notes

- Thin wrapper that resolves account name to ID, then delegates to backend helper
- 2-second cache (balances change frequently)
- Returns 404 if account doesn't exist

---

### get_balance_history

**REST Path**: `GET /accounts/{account-name}/balance-history`
**SQL Function**: `btracker_endpoints.get_balance_history(...)`
**Source**: `endpoints/account-balances/get_balance_history.sql`

Returns paginated history of balance changes for a specific asset type.

#### Parameters

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| account-name | TEXT | Yes | - | Hive account name |
| coin-type | nai_type | Yes | - | Asset type: HBD, HIVE, or VESTS |
| balance-type | balance_type | No | 'balance' | 'balance' (liquid) or 'savings_balance' |
| page | INT | No | NULL (newest) | Page number (1 = oldest) |
| page-size | INT | No | 100 | Results per page (max 1000) |
| direction | sort_direction | No | 'desc' | 'asc' or 'desc' |
| from-block | TEXT | No | NULL | Start block (number or timestamp) |
| to-block | TEXT | No | NULL | End block (number or timestamp) |

#### Returns: `btracker_backend.operation_history`

| Field | Type | Description |
|-------|------|-------------|
| total_operations | INT | Total matching records |
| total_pages | INT | Total pages available |
| operations_result | balance_history[] | Array of balance change records |

Each `balance_history` record:

| Field | Type | Description |
|-------|------|-------------|
| block_num | INT | Block number of the change |
| operation_id | TEXT | Unique operation identifier |
| op_type_id | INT | Operation type ID |
| balance | TEXT | Balance after this operation |
| prev_balance | TEXT | Balance before this operation |
| balance_change | TEXT | Delta (balance - prev_balance) |
| timestamp | TIMESTAMP | Block timestamp |

#### Examples

```sql
-- SQL: Get VESTS history for blocktrades
SELECT * FROM btracker_endpoints.get_balance_history(
  'blocktrades', 'VESTS', 'balance', 1, 100, 'desc', NULL, NULL
);
```

```bash
# REST: Get recent HBD changes
curl 'http://localhost:3000/balance-api/accounts/gtg/balance-history?coin-type=HBD&page-size=50'
```

#### Validation Rules

- page-size: 1-1000
- VESTS + savings_balance is invalid (savings only holds HBD/HIVE)
- Block numbers must be positive

---

### get_balance_aggregation

**REST Path**: `GET /accounts/{account-name}/aggregated-history`
**SQL Function**: `btracker_endpoints.get_balance_aggregation(...)`
**Source**: `endpoints/account-balances/get_history_aggregation.sql`

Returns aggregated balance history at day/month/year granularity for charts.

#### Parameters

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| account-name | TEXT | Yes | - | Hive account name |
| coin-type | nai_type | Yes | - | Asset type: HBD, HIVE, or VESTS |
| granularity | granularity | No | 'yearly' | 'daily', 'monthly', or 'yearly' |
| direction | sort_direction | No | 'desc' | 'asc' or 'desc' |
| from-block | TEXT | No | NULL | Start block (number or timestamp) |
| to-block | TEXT | No | NULL | End block (number or timestamp) |

#### Returns: SETOF `btracker_backend.aggregated_history`

| Field | Type | Description |
|-------|------|-------------|
| date | TIMESTAMP | Period end timestamp |
| balance | balances | {balance, savings_balance} at period end |
| prev_balance | balances | {balance, savings_balance} at period start |
| min_balance | balances | Minimum during period |
| max_balance | balances | Maximum during period |

#### Examples

```sql
-- SQL: Monthly HIVE balance for blocktrades
SELECT * FROM btracker_endpoints.get_balance_aggregation(
  'blocktrades', 'HIVE', 'monthly', 'desc', NULL, NULL
);
```

```bash
# REST: Yearly VESTS summary
curl 'http://localhost:3000/balance-api/accounts/gtg/aggregated-history?coin-type=VESTS&granularity=yearly'
```

#### Gap Filling

Uses RECURSIVE CTE to carry forward balances for periods with no activity.

---

### get_top_holders

**REST Path**: `GET /top-holders`
**SQL Function**: `btracker_endpoints.get_top_holders(...)`
**Source**: `endpoints/account-balances/get_top_holders.sql`

Returns paginated leaderboard of top asset holders.

#### Parameters

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| coin-type | nai_type | Yes | - | Asset type: HBD, HIVE, or VESTS |
| balance-type | balance_type | No | 'balance' | 'balance' or 'savings_balance' |
| page | INT | No | 1 | 1-based page number |
| page-size | INT | No | 100 | Results per page (max 1000) |
| min-balance | BIGINT | No | - | Inclusive lower balance bound (any coin-type), in the asset's smallest unit |
| max-balance | BIGINT | No | - | Exclusive upper balance bound (any coin-type), in the asset's smallest unit |

The range is a generic half-open `[min-balance, max-balance)` bound applied to
whichever `coin-type`/`balance-type` is requested. It is applied before
pagination, so `total_accounts` and `total_pages` describe the filtered subset.
Rank stays **global**: a filtered holder keeps its position in the full
leaderboard (the top of a mid bracket is not rank 1), so a range with an upper
bound offsets ranks by the number of holders above it. Bounds must be
non-negative and `min-balance <= max-balance`. Omitting both preserves the
unfiltered behavior; passing only `min-balance` gives an open-ended top bracket.
For an HP distribution bracket, pass converted VESTS values with
`coin-type=VESTS` (HP-to-VESTS conversion remains the client's responsibility).

#### Returns: `btracker_backend.top_holders`

| Field | Type | Description |
|-------|------|-------------|
| total_accounts | INT | Count of accounts with positive balance |
| total_pages | INT | Total pages available |
| holders_result | ranked_holder[] | Array of ranked accounts |

Each `ranked_holder`:

| Field | Type | Description |
|-------|------|-------------|
| rank | INT | Global rank (1 = highest balance) |
| account | TEXT | Account name |
| value | TEXT | Balance amount |

#### Examples

```sql
-- SQL: Top 100 HIVE holders
SELECT * FROM btracker_endpoints.get_top_holders('HIVE', 'balance', 1, 100);
```

```bash
# REST: Top HBD savings holders
curl 'http://localhost:3000/balance-api/top-holders?coin-type=HBD&balance-type=savings_balance'

# REST: VESTS holders in [20,000,000, 200,000,000)
curl 'http://localhost:3000/balance-api/top-holders?coin-type=VESTS&min-balance=20000000&max-balance=200000000'

# REST: Open-ended range (top bracket), works for any coin-type
curl 'http://localhost:3000/balance-api/top-holders?coin-type=HIVE&min-balance=1000000'
```

#### Use Cases

- Whale watching and monitoring
- Wealth distribution analysis
- Governance participation metrics (VESTS = voting power)
- Exchange cold wallet identification

---

## Data Sources

| Endpoint | Primary Tables |
|----------|---------------|
| get_account_balances | current_account_balances, account_delegations, account_rewards, account_savings, account_withdraws, convert_state, order_state, escrow_state |
| get_balance_history | account_balance_history, account_savings_history |
| get_balance_aggregation | balance_history_by_day, balance_history_by_month |
| get_top_holders | current_account_balances, account_savings |

## Related Documentation

- Processing: [../processing/balances.md](../processing/balances.md)
- Delegations: [delegations.md](delegations.md)
- Type definitions: `endpoints/types/balances.sql`

## How to Add New Balance Endpoint

1. Create SQL file in `endpoints/account-balances/`
2. Add OpenAPI spec comment for PostgREST
3. Create endpoint function in `btracker_endpoints` schema
4. Create backend helper in `btracker_backend` schema if complex
5. Add entry to this documentation
6. Add Tavern test in `tests/tavern/patterns-mainnet/`

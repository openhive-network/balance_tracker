# Delegation Endpoints

## Overview

Delegation endpoints provide information about VESTS (Hive Power) delegations between accounts. Delegations transfer voting power without transferring ownership of the underlying VESTS.

## How Delegations Work

- **Delegator**: Account that delegates VESTS to another account
- **Delegatee**: Account that receives delegated VESTS
- Delegator retains ownership but loses voting influence on delegated amount
- Delegatee gains voting power but cannot sell/transfer the VESTS
- Undelegating has a 5-day return delay (delayed_vests)

## Endpoints

### get_balance_delegations

**REST Path**: `GET /accounts/{account-name}/delegations`
**SQL Function**: `btracker_endpoints.get_balance_delegations(account-name TEXT)`
**Source**: `endpoints/account-balances/get_account_delegations.sql`

Returns all active VESTS delegations for an account, split into incoming and outgoing.

#### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| account-name | TEXT | Yes | Hive account name |

#### Returns: `btracker_backend.delegations`

| Field | Type | Description |
|-------|------|-------------|
| outgoing_delegations | outgoing_delegations[] | VESTS delegated TO other accounts |
| incoming_delegations | incoming_delegations[] | VESTS received FROM other accounts |

Each `outgoing_delegations` record:

| Field | Type | Description |
|-------|------|-------------|
| delegatee | TEXT | Account receiving the delegation |
| amount | TEXT | VESTS amount delegated |
| operation_id | TEXT | Unique operation identifier |
| block_num | INT | Block where delegation was created/modified |

Each `incoming_delegations` record:

| Field | Type | Description |
|-------|------|-------------|
| delegator | TEXT | Account that delegated |
| amount | TEXT | VESTS amount received |
| operation_id | TEXT | Unique operation identifier |
| block_num | INT | Block where delegation was created/modified |

#### Examples

```sql
-- SQL: Get all delegations for blocktrades
SELECT * FROM btracker_endpoints.get_balance_delegations('blocktrades');
```

```bash
# REST
curl -X GET 'http://localhost:3000/balance-api/accounts/blocktrades/delegations'
```

#### Example Response

```json
{
  "outgoing_delegations": [
    {
      "delegatee": "hive.fund",
      "amount": "1000000000000",
      "operation_id": "21474802120262208",
      "block_num": 4999992
    }
  ],
  "incoming_delegations": [
    {
      "delegator": "steemit",
      "amount": "500000000000",
      "operation_id": "21474660386343488",
      "block_num": 4999959
    }
  ]
}
```

#### Architecture Notes

- Results sorted by amount descending (largest delegations first)
- Empty arrays returned as `[]` (not NULL) for consistent JSON
- 2-second cache (delegations can change frequently)

---

## Data Sources

| Table | Purpose |
|-------|---------|
| `current_accounts_delegations` | Active delegation pairs (delegator → delegatee → amount) |
| `account_delegations` | Summary totals (total delegated, total received per account) |

## Delegation Processing

Delegations are tracked via the `delegate_vesting_shares` operation:

```
delegate_vesting_shares {
  delegator: "alice",
  delegatee: "bob",
  vesting_shares: "1000.000000 VESTS"
}
```

Special cases handled:
- **HF23 Return-to-self**: When delegation amount = 0, it's an undelegation
- **Delayed return**: Undelegated VESTS are locked for 5 days before returning

See [../processing/delegations.md](../processing/delegations.md) for detailed processing logic.

## Related Documentation

- Processing: [../processing/delegations.md](../processing/delegations.md)
- Balance summary: [balances.md](balances.md) (includes delegated_vests, received_vests fields)
- Type definitions: `endpoints/types/delegations.sql`

## How to Add New Delegation Endpoint

1. Create SQL file in `endpoints/account-balances/`
2. Add OpenAPI spec comment for PostgREST
3. Create endpoint function in `btracker_endpoints` schema
4. Create backend helper in `btracker_backend` schema
5. Add entry to this documentation
6. Add Tavern test in `tests/tavern/patterns-mainnet/`

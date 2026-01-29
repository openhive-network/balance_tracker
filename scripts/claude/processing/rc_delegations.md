# RC Delegation Processing

## Overview

Processes RC (Resource Credit) delegations transmitted via `custom_json_operation` with `id="rc"`. RC delegations allow accounts to share their Resource Credits with other accounts without transferring ownership.

**File**: `db/process_rc_delegations.sql`

## Hardfork Requirement

RC delegations were formalized in **HF26** (block ~68,676,505). The processing function automatically skips blocks before HF26.

```sql
SELECT block_num INTO __hardfork_26_block
FROM hafd.applied_hardforks
WHERE hardfork_num = btracker_backend.hf_rc_delegations();

IF __hardfork_26_block IS NULL OR _to < __hardfork_26_block THEN
  RETURN;
END IF;
```

## Operations Handled

| Operation | Effect |
|-----------|--------|
| `custom_json_operation` with `id='rc'` containing `delegate_rc_operation` | Create/modify/remove RC delegation |

## Operation Format

```json
{
  "type": "custom_json_operation",
  "value": {
    "required_auths": [],
    "required_posting_auths": ["delegator"],
    "id": "rc",
    "json": "{\"type\":\"delegate_rc_operation\",\"value\":{\"from\":\"alice\",\"delegatees\":[\"bob\",\"carol\"],\"max_rc\":1000000000}}"
  }
}
```

The inner JSON contains:
- `from`: delegator account name
- `delegatees`: array of 1-100 recipient account names
- `max_rc`: amount of RC to delegate to EACH delegatee

## Tables Updated

| Table | Purpose |
|-------|---------|
| `current_rc_delegations` | Active RC delegation pairs (delegator, delegatee, max_rc) |
| `account_rc_delegations` | Summary totals (total RC delegated out, total RC received) |

## C++ Parser Function

The function uses HAF's C++ parser for validation and extraction:

```sql
hive.parse_rc_delegation((body->'value'->>'json')::text)
```

Returns one row per delegatee with:
- `from_account TEXT`: delegator name
- `to_account TEXT`: delegatee name
- `max_rc BIGINT`: RC amount delegated

Invalid operations (malformed JSON, invalid accounts, etc.) return an empty set.

## Data Flow

```
custom_json_operation (id='rc')
  │
  ├─> Parse with hive.parse_rc_delegation()
  │   └─> Returns empty set if invalid: SKIP
  │
  ├─> Resolve account names to IDs via hive.accounts_view
  │
  ├─> IF max_rc > 0: CREATE/UPDATE RC delegation pair
  │   └─> Upsert to current_rc_delegations
  │   └─> Update account_rc_delegations (both parties)
  │
  └─> IF max_rc == 0: REMOVE RC delegation
      └─> Delete from current_rc_delegations
      └─> Update account_rc_delegations (reduce totals)
```

## Key Differences from VESTS Delegations

| Aspect | VESTS Delegations | RC Delegations |
|--------|-------------------|----------------|
| Operation type | `delegate_vesting_shares` | `custom_json_operation` with `id='rc'` |
| Parser | JSONB extraction | C++ `hive.parse_rc_delegation()` |
| Return delay | 5-day delay when undelegating | No delay |
| Hardfork | Always available | HF26+ only |
| Multi-delegatee | One operation = one delegatee | One operation can have up to 100 delegatees |

## Key SQL Patterns

### Parsing RC Operations

```sql
SELECT
  parsed.from_account,
  parsed.to_account,
  parsed.max_rc,
  o.id AS source_op
FROM ops o
CROSS JOIN LATERAL hive.parse_rc_delegation((o.body->'value'->>'json')::text) AS parsed
WHERE parsed.from_account IS NOT NULL
```

### Account Name Resolution

```sql
-- Batch lookup for efficiency
WITH all_account_names AS (
  SELECT DISTINCT from_account AS account_name FROM parsed_rc_delegations
  UNION
  SELECT DISTINCT to_account AS account_name FROM parsed_rc_delegations
),
account_ids AS (
  SELECT av.name AS account_name, av.id AS account_id
  FROM hive.accounts_view av
  WHERE av.name IN (SELECT account_name FROM all_account_names)
)
```

### Delta Calculation (Squashing Pattern)

```sql
SELECT
  delegator, delegatee, max_rc,
  max_rc - LAG(max_rc, 1, 0) OVER w_asc AS rc_delta,
  ROW_NUMBER() OVER w_desc AS rn  -- rn=1 is latest
FROM add_prev_rc_delegation
WINDOW
  w_asc AS (PARTITION BY delegator, delegatee ORDER BY source_op),
  w_desc AS (PARTITION BY delegator, delegatee ORDER BY source_op DESC)
```

## Edge Cases

1. **Pre-HF26 blocks**: Automatically skipped
2. **Invalid JSON**: C++ parser returns empty set, operation ignored
3. **Zero max_rc**: Treat as delegation removal (DELETE row)
4. **Multiple delegatees**: One operation expands to multiple rows
5. **Multiple changes**: Only keep final state per (delegator, delegatee) pair
6. **Unknown accounts**: Filtered out during account ID resolution

## API Endpoint

```
GET /balance-api/accounts/{account-name}/rc-delegations
```

Returns incoming and outgoing RC delegations for the specified account.

## Expansion Rules

When modifying RC delegation processing:
- Maintain HF26 check at function start
- Use the C++ parser (don't parse JSON manually)
- Update both `current_rc_delegations` AND `account_rc_delegations`
- Follow the squashing pattern for correct delta calculation

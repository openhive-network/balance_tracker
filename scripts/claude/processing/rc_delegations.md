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
| `withdraw_vesting`, `fill_vesting_withdraw` (weekly payout), `delegate_vesting_shares` (delegator and delegatee) of an account with RC delegations | hived's implicit reduction when the account's vesting falls below what it delegated (no operation is emitted) |

## Implicit Reductions (hived's RC delegation overflow)

hived (`resource_credits::update_account_after_vest_change` with the overflow check,
`libraries/chain/rc/rc_utility.cpp`) checks an account's RC delegations after a
`withdraw_vesting`, after a `delegate_vesting_shares` (delegator and delegatee) and
after each weekly power-down payout (once, after the payout's last
`fill_vesting_withdraw`). When

```
overflow = delegated_rc - (vesting_shares - delegated_vesting_shares
                           + received_vesting_shares - next_vesting_withdrawal) > 0
```

(`next_vesting_withdrawal = min(vesting_withdraw_rate, to_withdraw - withdrawn)`
while powering down, else 0), `remove_delegations` takes the overflow from the
account's delegations in delegatee account id order: each is reduced by what is
left of the overflow, removed when that consumes it. No operation is emitted.

btracker reproduces it in `process_rc_delegation_range`:

- `rc_delegation_vest_checks(_batch_from, _from, _to)` lists the checks of the range
  for accounts with RC delegations (before the batch or delegated in it) and each
  one's effective vests, replayed set-based: vesting delegations and power-down
  state from `account_delegations`, `current_accounts_delegations` and
  `account_withdraws` as they were before the batch plus the batch's
  `delegate_vesting_shares` / `return_vesting_delegation` / `withdraw_vesting` /
  `fill_vesting_withdraw` operations up to the check; the VESTS balance as
  `current_account_balances` minus the batch's later changes (`_btracker_vests_batch`,
  staged by `process_block_range_balances`). This is why the step runs between
  balances and withdrawals.
- For those accounts, their `delegate_rc` ops and the checks are applied one by one
  in operation order (`set_rc_delegation`, `remove_rc_delegation_overflow`), so a
  reduction lands at the operation that caused it (it becomes the pair's
  `source_op`) and a later `delegate_rc` acts on the reduced delegations. Every
  other delegator's `delegate_rc` ops are squashed set-based as before.
- hived removes at most `get_remove_threshold()` delegations per block and finishes
  the rest in later blocks (`rc_expired_delegation_object`, during which it rejects
  the account's `delegate_rc`). btracker applies the whole reduction at the
  triggering operation, which leaves the same delegations.
- The step returns before building its temp tables when the range has no
  `custom_json` with id `rc` and none of its `withdraw_vesting` /
  `fill_vesting_withdraw` / `delegate_vesting_shares` names an account with rows in
  `current_rc_delegations` (nearly every block): then there is neither a
  `delegate_rc` to apply nor a check that could reduce a delegation. The account
  lookup takes the op's account names as an array parameter so its plan stays
  cached across a recreation of `_btracker_ops_batch` (every massive-sync batch;
  live keeps the table, see `btracker_reset_batch_table` in `../processing.md`).
- The delegatee order is the HAF account id, which follows hived's account
  creation order like its `account_id_type`.
- A payout that leaves the account with 0 VESTS ends its power-down (as hived's
  `process_vesting_withdrawals` does): the check at that payout and every later
  check of the same power-down use a next withdrawal of 0, before the withdrawals
  step stores the ended power-down.

Existing installs: `repair_rc_delegation_overflow()` (`db/one_off_repairs.sql`)
applies the reduction once to every delegator over its effective vests now. A
delegation hived reduced at an earlier vest change, after which the delegator's
vesting grew back, is not visible in btracker's tables and stays until a resync.
Measured on steem-20 at block 111,303,569 against `rc_api.list_rc_direct_delegations`:
before the repair 2,074 of 3,920 delegators matched hived exactly, after it 2,959;
no delegator that matched stopped matching.

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
7. **Vesting below delegated RC**: implicit reduction in delegatee id order (see above)

## API Endpoint

```
GET /balance-api/accounts/{account-name}/rc-delegations
```

Returns incoming and outgoing RC delegations for the specified account.

## Expansion Rules

When modifying RC delegation processing:
- Keep the step between balances and withdrawals/delegations in every processing path
- Maintain HF26 check at function start
- Use the C++ parser (don't parse JSON manually)
- Update both `current_rc_delegations` AND `account_rc_delegations`
- Follow the squashing pattern for correct delta calculation

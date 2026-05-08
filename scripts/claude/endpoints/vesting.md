# Vesting Endpoints

Power-up (`transfer_to_vesting`) and power-down (`withdraw_vesting`,
`fill_vesting_withdraw`) analytics. Three endpoints — one global, two
per-account.

## Endpoint List

| Endpoint | Purpose |
|----------|---------|
| `GET /vesting-stats` | Global aggregated stats, daily/monthly/yearly, gap-filled |
| `GET /accounts/{name}/vesting-history` | Per-account event list, paginated, filtered |
| `GET /accounts/{name}/vesting-stats` | Per-account aggregated stats (computed on-the-fly) |

## Common Concepts

### Filter enum

Used by `vesting-history` to narrow results to one event kind:

- `all` — every kind
- `power_up` — `transfer_to_vesting` only
- `power_down_init` — `withdraw_vesting` only (cancellations excluded)
- `power_down_fill` — `fill_vesting_withdraw` only (weekly tranches)

### Direction labels in event rows

Each `vesting-history` row carries a `direction` field with the same enum
values (minus `all`). Useful when querying with `filter=all`.

### Amount shape

All amounts use `btracker_backend.amount` ({`nai`, `amount`, `precision`}):

- HIVE: NAI 21, precision 3
- VESTS: NAI 37, precision 6

In `vesting-history` rows, `amount_hive` / `amount_vests` are nullable —
exactly one is populated for `power_up` / `power_down_init`, both are
populated for `power_down_fill` (HIVE only when realised — routed-to-VESTS
fills have `amount_hive=null`).

## `GET /vesting-stats`

Modeled on `/transfer-statistics`. Reads from `vesting_stats_by_day` /
`_by_month`, gap-filled via `generate_series` so every period in the
requested range is represented.

Parameters:
- `granularity`: `daily | monthly | yearly` (default `daily`). Yearly
  computed on-the-fly from monthly via
  `btracker_backend.vesting_stats_by_year`.
- `direction`: `asc | desc` (default `desc`)
- `from-block` / `to-block`: integer or timestamp

Cache: 1 year for fully-irreversible ranges, 2 seconds otherwise (same as
`/transfer-statistics`).

## `GET /accounts/{name}/vesting-history`

Reads from btracker's `account_vesting_history` (populated during sync) —
NOT from `hive.account_operations_view`. This avoids per-query op-type
filtering and JSON body parsing on full mainnet (where high-activity
accounts have hundreds of thousands of unrelated ops).

Pagination uses `vesting_seq_no` (per-account monotonic), so:
- `filter=all` COUNT = `MAX(vesting_seq_no)` — O(1) via PK
- `filter=specific` COUNT = `COUNT(*) WHERE account=? AND kind=?` —
  index-only scan
- Block-range filter uses
  `(account, hafd.operation_id_to_block_num(source_op))` index

Parameters:
- `filter`: see Filter enum (default `all`)
- `page` / `page-size`: 1-based page; default page = 1; max page-size 1000
- `direction`: `asc | desc` (default `desc`)
- `from-block` / `to-block`: integer or timestamp

Returns `{total_operations, total_pages, operations_result[]}`. Each
event row exposes `block_num`, `operation_id`, `op_type_id`, `direction`,
`amount_hive`, `amount_vests`, `timestamp`. `amount_hive`/`amount_vests`
are always populated objects (zero-amount when not applicable).

Cancellations (`withdraw_vesting` with `vesting_shares.amount=0`) are
filtered at sync time, so they never appear anywhere.

## `GET /accounts/{name}/vesting-stats`

Same response shape as `/vesting-stats` but scoped to one account.
Reads from `account_vesting_by_day` / `account_vesting_by_month`
(populated during sync by `process_account_vesting_stats`) — no on-the-fly
scans. Yearly is rolled up from monthly via
`btracker_backend.account_vesting_stats_by_year`. Gap-filled the same way
as the global endpoint.

Parameters: `granularity`, `direction`, `from-block`, `to-block`.

## Backend layer

```
btracker_backend.get_vesting_aggregation()           -- /vesting-stats
btracker_backend.get_account_vesting_history()       -- /accounts/{name}/vesting-history
btracker_backend.get_account_vesting_aggregation()   -- /accounts/{name}/vesting-stats
btracker_backend.vesting_stats_by_year()             -- monthly→yearly rollup helper
btracker_backend.get_vesting_stats()                 -- table router (day|month|year)
```

## Related

- Processing: [processing/vesting_stats.md](../processing/vesting_stats.md)
- Withdrawals state (rates, routes): [processing/withdrawals.md](../processing/withdrawals.md)

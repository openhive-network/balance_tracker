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
- `power_down_fill` — `fill_vesting_withdraw`, the account's OWN power-down
- `power_down_route_received` — `fill_vesting_withdraw` routed to this account from
  another account's power-down (`to_account <> from_account`). Shares `op_type_id` 56
  with `power_down_fill`; branch on `direction`. (issue #54)

### Direction labels in event rows

Each `vesting-history` row carries a `direction` field with the same enum
values (minus `all`). Useful when querying with `filter=all`.

### Amount shape

All amounts use `btracker_backend.amount` ({`nai`, `amount`, `precision`}):

- HIVE: NAI 21, precision 3
- VESTS: NAI 37, precision 6

In `vesting-history` rows, `amount_hive` / `amount_vests` carry what the account
actually moved: `power_up` → HIVE; `power_down_init` → VESTS; `power_down_fill` (own
power-down) → VESTS withdrawn + HIVE realised (HIVE 0 when routed away);
`power_down_route_received` → the deposited asset received via a route (HIVE for
auto_vest=false, VESTS for auto_vest=true). The non-applicable side is a zero-amount object.

## `GET /vesting-stats`

Modeled on `/transfer-statistics`. Reads from `vesting_stats_by_day` /
`_by_month`, gap-filled via `generate_series` so every period in the
requested range is represented.

The `date` field labels the **start** of each bucket (the day/month/year the
events occurred), so same-period events are reported on that period — not the
next one (GitLab #55).

Parameters:
- `granularity`: `daily | monthly | yearly` (default `daily`). Yearly
  granularity is rolled up on the fly from the monthly table inside
  `btracker_backend.vesting_pivot` (`_trunc='year'`) — there is no
  standalone `*_by_year` function or table.
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
Reads from the **tall** `account_vesting_by_day` / `account_vesting_by_month`
(`(account, kind, period)` rows) populated during sync by
`process_account_vesting_stats`, pivoting kinds into the wide response
(`SUM(<col>) FILTER (WHERE kind = N)`) — no on-the-fly op scans. Yearly is rolled up from
monthly on the fly inside `btracker_backend.vesting_pivot` (`_trunc='year'`). Gap-filled like
the global endpoint.

vesting-stats (global AND per-account) cover only power_up / power_down_init /
power_down_fill. Routed receipts (kind=4 `power_down_route_received`) are history-only and do
NOT appear in either stats endpoint — they surface only in `/vesting-history` (issue #54).

Parameters: `granularity`, `direction`, `from-block`, `to-block`.

## Backend layer

```
btracker_backend.get_vesting_aggregation()           -- /vesting-stats (gap-fill wrapper)
btracker_backend.get_account_vesting_aggregation()   -- /accounts/{name}/vesting-stats (gap-fill wrapper)
btracker_backend.get_account_vesting_history()       -- /accounts/{name}/vesting-history
btracker_backend.get_vesting_stats()                 -- global table router (day|month|year)
btracker_backend.get_account_vesting_stats()         -- per-account table router (day|month|year)
btracker_backend.vesting_pivot()                     -- shared tall→wide pivot (incl. _trunc='year' yearly rollup)
```

## Related

- Processing: [processing/vesting_stats.md](../processing/vesting_stats.md)
- Withdrawals state (rates, routes): [processing/withdrawals.md](../processing/withdrawals.md)

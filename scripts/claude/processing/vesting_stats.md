# Vesting Stats Processing

Power-up and power-down activity tracking — split into two functions for
single responsibility, mirroring the existing convention (e.g.
`process_transfer_stats` is global-only;
`process_balances`/`process_savings` own per-account state).

## Functions

| Function | File | Outputs |
|----------|------|---------|
| `process_vesting_stats(_from, _to)` | `db/process_vesting_stats.sql` | `vesting_stats_by_day`, `vesting_stats_by_month` (global) |
| `process_account_vesting_stats(_from, _to)` | `db/process_account_vesting_stats.sql` | `account_vesting_history`, `account_vesting_by_day`, `account_vesting_by_month` |

Both run within `btracker_massive_processing()` / `btracker_single_processing()`
so they share the same transaction / fork-tracking guarantees as the rest of
btracker. They each scan `_btracker_ops_batch` independently; the redundant
parse cost is negligible because the temp table already lives in shared
buffers.

## Operations Processed

Aggregates are stored **TALL** — one row per `(kind[, account], period)` with generic
`op_count` / `hive_amount` / `vests_amount` columns (not a column per kind). The API
reconstructs its wide per-period shape with a pivot (`SUM(<col>) FILTER (WHERE kind = N)`)
at read time. Event kinds:

| Operation → kind | Effect |
|------------------|--------|
| `transfer_to_vesting` → kind=1 (power_up) | Global: one `(1, period)` row, `op_count` += 1, `hive_amount` += amount. Per-account: history row + per-account aggregate for BOTH `from` and `to`. |
| `withdraw_vesting` → kind=2 (power_down_init) | Global/per-account: `(2, …)` row, `op_count` += 1, `vests_amount` += scaled amount. Per-account: `account` only. Cancellations (`amount=0`) excluded. |
| `fill_vesting_withdraw` → kind=3 (power_down_fill) — the account's OWN power-down | Global: `(3, period)`, `op_count` += 1, `vests_amount` += withdrawn, `hive_amount` += deposited (only when deposited NAI=21). Per-account: attributed to **`from_account` only** — `vests_amount` += withdrawn; `hive_amount` += deposited HIVE **only for an own power-down** (`to_account = from_account`), so HIVE routed away is not credited to the sender. |
| `fill_vesting_withdraw` → kind=4 (power_down_route_received) — recipient of a routed fill | **History-only** — writes ONE `account_vesting_history` row for `to_account`, **only when routed** (`to_account <> from_account`): `hive_amount` = deposited (auto_vest=false) OR `vests_amount` = deposited (auto_vest=true). kind=4 contributes to **NO aggregate** — not the global `vesting_stats_by_*`, not the per-account `account_vesting_by_*` (a route recipient is not powering down). It is surfaced solely via `/vesting-history`. Keeps another account's withdrawal out of the recipient's power_down_fill (issue #54). |

## Tables Updated

| Table | Owner | Purpose |
|-------|-------|---------|
| `vesting_stats_by_day` | `process_vesting_stats` | Global daily aggregates |
| `vesting_stats_by_month` | `process_vesting_stats` | Global monthly aggregates |
| `account_vesting_history` | `process_account_vesting_stats` | Per-account event history (one row per impacted account per op) |
| `account_vesting_by_day` | `process_account_vesting_stats` | Per-account daily aggregates |
| `account_vesting_by_month` | `process_account_vesting_stats` | Per-account monthly aggregates |

Yearly granularity (both global and per-account) is rolled up on the fly
from the `_by_month` table — no `_by_year` table.

## Why VESTS columns are NUMERIC

`total_vesting_shares` is ~10^14 satoshi today; sum-of-month VESTS already
exceeds BIGINT range comfortably and yearly rollups blow past it. NUMERIC
keeps every aggregation safe regardless of horizon. HIVE columns stay
BIGINT (HIVE supply ≪ BIGINT max).

## HF1 precision

Pre-HF1 VESTS values need ×10^6 multiplier. Both functions look up the HF1
boundary block once and apply
`btracker_backend.vests_precision_multiplier(block_num > _hf_vests_precision_block)`
inline — same pattern as `process_withdrawals`.

## Why no HIVE-equivalent at withdraw_vesting init time

The chain converts VESTS→HIVE at *each fill*, not at the init block. A
"HIVE-equivalent" at init time would be fictional. Realised HIVE comes
from `power_down_fill_hive` (deposited amounts).

## Routed power-down fills (issue #54)

`fill_vesting_withdraw` has `from_account` (who powers down, loses `withdrawn` VESTS) and
`to_account` (who receives `deposited`). With a `set_withdraw_vesting_route` the two differ.
The deposited asset is HIVE (precision 3, NAI 21) when `auto_vest=false`, or VESTS
(precision 6, NAI 37) when `auto_vest=true`. The canonical balance split is in
`backend/operation_parsers/impacted_balances.sql` (`to +deposited`, `from -withdrawn`).

Per-account attribution (in `process_account_vesting_stats`):

- **from_account → kind=3 (power_down_fill):** `vests_amount` = withdrawn; `hive_amount` =
  deposited HIVE **only when not routed away** (`to_account = from_account`).
- **to_account → kind=4 (power_down_route_received), only when `to <> from`:** `hive_amount` =
  deposited (auto_vest=false) or `vests_amount` = deposited (auto_vest=true).

This fixes the previous double-attribution where both accounts were credited the full
`withdrawn` VESTS and the routed-away HIVE. The **global** `process_vesting_stats` has no
per-account fan-out (one row per op), so it still counts each fill once under kind=3 and
never emits kind=4.

## Per-account history table

`account_vesting_history` mirrors `account_balance_history` in design:

- Columns: `account`, `vesting_seq_no`, `kind` (1/2/3/4), `hive_amount` BIGINT,
  `vests_amount` NUMERIC, `source_op` BIGINT (kind=4 = power_down_route_received)
- `vesting_seq_no` is per-account, monotonic across all kinds, assigned at
  write time (`MAX(seq_no) + ROW_NUMBER() OVER (PARTITION BY account
  ORDER BY source_op)` per batch)
- UNLOGGED during massive sync; switched to LOGGED in `finalize_massive_sync`
- Indexes built post-sync via `create_btracker_indexes()`:
  - `(account, vesting_seq_no)` UNIQUE — fast `MAX(seq_no)` for
    filter='all' COUNT and pagination ordering
  - `(account, kind, vesting_seq_no)` — fast COUNT/pagination for
    filter=power_up | power_down_init | power_down_fill
  - `(account, hafd.operation_id_to_block_num(source_op))` — block-range
    filtering

## Per-account aggregation tables (tall)

`account_vesting_by_day` / `account_vesting_by_month` are **tall**:
`(account, kind, updated_at, op_count, hive_amount, vests_amount, last_block_num)`,
PK `(account, updated_at, kind)`. The global `vesting_stats_by_*` use the same shape
without `account`, PK `(updated_at, kind)` (and only ever hold kinds 1-3). Populated via
additive UPSERT-merge.

The global aggregation in `process_vesting_stats` reads from `parsed` (one row per op).
The per-account aggregation in `process_account_vesting_stats` reads from
`impacted_with_ids` (one row per impacted-account/op pair) — so a single
`transfer_to_vesting` with distinct `from`/`to` contributes ONCE to the global aggregate
but TWICE (once per account) to per-account aggregates.

Cancellations (`withdraw_vesting` with `vesting_shares.amount = 0`) are filtered before
BOTH functions' aggregations, so they never appear anywhere.

### Read-time pivot

The endpoints return the historical **wide** `vesting_stats` shape, rebuilt from the tall
rows by the shared tall→wide engine `backend/endpoint_helpers/shared_functions/vesting_pivot.sql`
(incl. yearly rollups), consumed by the per-endpoint routers `get_account_vesting_stats`
(`backend/endpoint_helpers/get_account_vesting_stats/`) and `get_vesting_stats`
(`backend/endpoint_helpers/get_vesting_stats/`):

```sql
SUM(op_count)     FILTER (WHERE kind = 1) AS power_up_count,
SUM(hive_amount)  FILTER (WHERE kind = 1) AS power_up_hive,
SUM(op_count)     FILTER (WHERE kind = 2) AS power_down_init_count,
SUM(vests_amount) FILTER (WHERE kind = 2) AS power_down_init_vests,
SUM(op_count)     FILTER (WHERE kind = 3) AS power_down_fill_count,
SUM(vests_amount) FILTER (WHERE kind = 3) AS power_down_fill_vests,
SUM(hive_amount)  FILTER (WHERE kind = 3) AS power_down_fill_hive
GROUP BY <period>
```

kind=4 (`power_down_route_received`) is intentionally NOT aggregated and NOT present in
`vesting_stats_return` — it is history-only, surfaced solely via `/vesting-history`. Both
the global and per-account stats tables hold only kinds 1/2/3, so the pivot output type
`vesting_stats_return` (`backend/endpoint_helpers/shared_functions/vesting_pivot.sql`) has
exactly the power_up / power_down_init / power_down_fill columns above — no
`power_down_route_received_*`. Adding a future stats kind = new rows + a new column here.

## GROUPING SETS

Both functions use single-pass two-level aggregation over the kind dimension:

- Global: `GROUP BY GROUPING SETS ((kind, by_day), (kind, by_month))`
- Per-account: `GROUP BY GROUPING SETS ((account, kind, by_day), (account, kind, by_month))`

In both cases the bitmap returned by `GROUPING(by_day, by_month)` separates
day rows (`grp_level=1`) from month rows (`grp_level=2`).

## Merge logic (UPSERT)

```sql
ON CONFLICT ... DO UPDATE SET
  op_count       = <tbl>.op_count     + EXCLUDED.op_count,
  hive_amount    = <tbl>.hive_amount  + EXCLUDED.hive_amount,
  vests_amount   = <tbl>.vests_amount + EXCLUDED.vests_amount,
  last_block_num = EXCLUDED.last_block_num
```

All counts and amounts are additive; `last_block_num` is replaced. This
makes incremental sync (live blocks one at a time) and re-sync of
overlapping ranges idempotent for additive metrics.

## Related Processing

- [withdrawals.md](withdrawals.md) — owns the withdrawal *state* tables
  (`account_withdraws`, `account_routes`); this file owns *stats* and
  *history*.
- [transfer_stats.md](transfer_stats.md) — global transfer volume aggregation
  (same pattern, no per-account variant).

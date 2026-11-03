# Order Endpoints

How the limit orders of the internal HBD/HIVE market ended, from `order_lifecycle`
and its per-creation-block counts (see [processing/orders.md](../processing/orders.md)).

## Endpoint List

| Endpoint | Purpose |
|----------|---------|
| `GET /accounts/{account-name}/order-stats?from-block=&to-block=` | Lifecycle counts of one account's orders |
| `GET /order-stats?from-block=&to-block=` | The same over every account |

## Response

`{created, filled, cancelled, open, fill_rate_pct}` for the orders **created** in the
window (`from-block`/`to-block`: a block number or a `YYYY-MM-DD HH:MI:SS` date; either
may be omitted). Outcomes count whenever they happened, also after the window, so
`created = filled + cancelled + open` for any window. An order partially filled and then
cancelled (or expired) counts once, as `cancelled`. `fill_rate_pct` is
`100 * filled / created` rounded to 2 places, `null` when nothing was created.

- Unknown account: 400 (`Account '...' does not exist`); `from-block`/`to-block` <= 0: 400.
- `from-block` after `to-block` (blocks or dates): 400, raised by HAF's
  `hive.convert_to_blocks_range` (`The starting block (...) must be less than or equal to
  the ending block (...)`; also for two dates with no block between them).
- Short cache (2 s) even for a past window: its orders can still fill or be cancelled.
- One statement per branch (`btracker_backend.get_order_stats`):
  - account: index-only scan of `idx_order_lifecycle_owner_block_created` `INCLUDE (outcome)`;
  - network: summed from `order_stats_by_block_group` for the whole groups of 10000
    blocks inside the window and `order_stats_by_block` for the blocks before the first and
    after the last of them, so at most 2 x 9999 block rows plus one row per 10000 blocks
    (~10k on mainnet) whatever the window. `open` = `created - filled - cancelled`.

## Upgraded installs

`order_lifecycle` and the order count tables are written by block processing only. An
install upgraded from a version without them gets them empty and counts only orders created
after the upgrade until it is resynced. No upgrade step backfills them.

## Implementation

| Layer | File |
|-------|------|
| Endpoints | `endpoints/orders/get_account_order_stats.sql`, `endpoints/orders/get_order_stats.sql` |
| Backend | `backend/endpoint_helpers/get_order_stats/order_stats.sql` (installed after the rollup tables' transaction, so an upgrade never runs it before they exist); `backend/shared/order_stats_group_blocks.sql` (the group size, also used by the writers and the seed, installed with `backend/shared/`) |
| Types | `endpoints/types/order_stats.sql` |

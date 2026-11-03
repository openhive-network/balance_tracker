# Order Endpoints

How the limit orders of the internal HBD/HIVE market ended, from `order_lifecycle`
(see [processing/orders.md](../processing/orders.md)).

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
- Short cache (2 s) even for a past window: its orders can still fill or be cancelled.
- One statement per branch (`btracker_backend.get_order_stats`): index-only scans of
  `idx_order_lifecycle_owner_block_created` (account) or `idx_order_lifecycle_block_created`
  (network), both `INCLUDE (outcome)`. A network-wide window over the whole chain reads
  the whole creation-block index (one entry per order, ~18M on mainnet).

## Upgraded installs

`order_lifecycle` is written by block processing only. An install upgraded from a version
without it gets the table empty and counts only orders created after the upgrade until it
is resynced. No upgrade step backfills it.

## Implementation

| Layer | File |
|-------|------|
| Endpoints | `endpoints/orders/get_account_order_stats.sql`, `endpoints/orders/get_order_stats.sql` |
| Backend | `backend/endpoint_helpers/get_order_stats/order_stats.sql` |
| Types | `endpoints/types/order_stats.sql` |

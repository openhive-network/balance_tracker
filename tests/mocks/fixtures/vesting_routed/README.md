# vesting_routed — routed power-down regression (issue #54)

Regression fixture for **#54** (routed `fill_vesting_withdraw` mis-attributed to the
recipient). `blocktrades` powers down with two withdraw routes; the routed fills must NOT
land in the recipients' `power_down_fill`, only in the new `power_down_route_received`
category, while `blocktrades` keeps the full withdrawn VESTS and only the HIVE it actually
received (own fill).

Blocks `90000054`-`90000057` (headers added to `fixtures/blocks/data.json` — a fresh range
above every other fixture so the routed ops never collide on `operation_id(block, op_pos)`).
Accounts `blocktrades` / `gtg` / `cvk` are part of the standard mock account set.

> op_type_ids: `4` = withdraw_vesting, `20` = set_withdraw_vesting_route, `56` =
> fill_vesting_withdraw (Hive protocol enum = `hafd.operation_types.id`; `delegate_vesting_shares=40`
> in the delegations fixture confirms the mapping). Verify against the target DB if in doubt.

## Operations

| block | op | from → to | withdrawn (VESTS) | deposited | auto_vest |
|------:|----|-----------|-------------------|-----------|-----------|
| 90000054 | withdraw_vesting | blocktrades | — (vesting_shares 300000000) | — | — |
| 90000054 | set_withdraw_vesting_route | blocktrades → gtg | — | — | false |
| 90000054 | set_withdraw_vesting_route | blocktrades → cvk | — | — | true |
| 90000055 | fill_vesting_withdraw | blocktrades → gtg | 150000000 | 7000 HIVE (prec 3) | false |
| 90000056 | fill_vesting_withdraw | blocktrades → cvk | 150000000 | 150000000 VESTS (prec 6) | true |
| 90000057 | fill_vesting_withdraw | blocktrades → blocktrades | 200000000 | 9500 HIVE (prec 3) | — |

`set_withdraw_vesting_route` is not consumed by the vesting-stats processors (only
transfer_to_vesting / withdraw_vesting / fill_vesting_withdraw are) — it is included for
fidelity and must be ignored by the aggregation.

## Expected per-account results (from-block 90000054) — the regression oracle

`account_vesting_by_day`/`_month` (tall: one row per kind), surfaced wide by the endpoints:

**blocktrades**
- `power_down_init` (kind 2): op_count 1, vests 300000000, hive 0
- `power_down_fill`  (kind 3): op_count **3**, vests **500000000** (150M+150M+200M), hive **9500**
  (only the OWN fill at 90000057 contributes HIVE; the two routed fills contribute hive 0)
- `power_down_route_received` (kind 4): none

**gtg** (routed recipient, auto_vest=false)
- `power_down_fill` (kind 3): **NONE** ← the bug fix: the routed 150000000 VESTS is NOT here
- `power_down_route_received` (kind 4): op_count 1, hive **7000**, vests 0

**cvk** (routed recipient, auto_vest=true)
- `power_down_fill` (kind 3): **NONE**
- `power_down_route_received` (kind 4): op_count 1, hive 0, vests **150000000**

Before the fix, gtg and cvk each carried the full 150000000 VESTS under `power_down_fill`
(and blocktrades' `power_down_fill_hive` was inflated by the routed-away HIVE).

## Tavern coverage

`tests/tavern/patterns-mock/get_account_vesting_history/` and `.../get_account_vesting_stats/`.

`gtg_power_down_fill_empty` is fully pinned (no volatile fields — proves the routed fill is
absent from the recipient's power_down_fill). The remaining `.pat.json` (route_received rows,
blocktrades fills, vesting-stats buckets) carry HAF-computed `operation_id`/`timestamp` and
gap-filled buckets, so they are captured the standard way: run the suite against the
resynced+mock DB and commit the saved responses. The expected amounts above are the oracle.

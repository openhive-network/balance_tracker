# Order lifecycle index

`order_lifecycle` retains one row per successful creation operation. Its key is
`create_op_id`; an owner can reuse the same `order_id` after the previous order
has ended. The existing `order_state` projection continues to serve locked
balances and is not the source of these statistics.

The reducer consumes canonical operation IDs in order. Each fill affects both
orders and only full consumption produces `filled`. Virtual
`limit_order_cancelled` provides the authoritative cancellation and refund;
the preceding real cancel is validated without counting a second outcome.
Expiration, dust refunds, user cancellation and HF23 account clearing all
produce `canceled`. HF23 needs its separate `hardfork_hive` handling because it
suppresses the ordinary cancellation virtual operation.

Creation timestamps and the indexed block timestamp are stored with the
lifecycle rows and checkpoint. Statistics remain available if HAF later prunes
the raw creation blocks. Both tables are registered in the owning HAF context,
so a fork reverts creations, partial fills, outcomes and coverage together.

The account and network `/order-stats` endpoints select orders by inclusive UTC
creation timestamps, then report outcomes through `indexed_through_block`.
`to-date` does not truncate later outcomes. The four counts reconcile, an empty
cohort has a null fill rate, and responses always have a two-second cache.
Incomplete migration or a checkpoint that differs from the context cursor
returns HTTP 503.

## Fresh synchronization

A new context starts with coverage at block zero. Every existing order-processing
call also runs the lifecycle reducer, including massive processing, the HF23
split and individual live blocks. No separate backfill is needed.

## Upgrade an existing context

Installing on an existing context creates the missing lifecycle tables without
resetting other projections. It marks historical coverage unavailable and
refuses further application processing until lifecycle backfill finishes.
Deployment therefore requires a maintenance window; prepare it before installing
this feature on a populated context.

1. Stop the owning application processor. For `hafbe_bal`, stop the parent
   HAFBE processor; both contexts are maintained as one group.
2. Keep HAF pruning disabled and retain the complete blocks and operations from
   genesis. The HAF version must provide the application install-lock API and
   preserve the context cursor when attaching a detached group.
3. Install the application update, then run the backfill on its dedicated
   connection:

   ```bash
   POSTGRES_URL='postgresql://haf_admin@database/haf_block_log' \
     ./scripts/backfill_order_lifecycle.sh --schema=btracker_app --batch-size=10000
   ```

   For the embedded instance, use `--schema=hafbe_bal`; the command selects
   `hafbe_owner`, which owns the parent context and inherits `btracker_owner`.
   The standalone default role is `btracker_owner`. A custom driver can provide
   its actual lock name with `--app-lock-name` and its owning maintenance role
   with `--role`. That role must be allowed to maintain every context in the group.
4. Restart the owning processor after the command reports completion. It
   replays the reversible tail through the ordinary application workflow.

The command holds the application's exclusive session install lock. It detaches
the entire registered group through HAF, which rewinds reversible projections
to their irreversible baseline, and records that fixed target. It then replays
only lifecycle operations in committed batches; it does not run balance reducers
over historical data or replay the parent context from genesis. Each batch has a consistent
source snapshot, validates block coverage and updates its checkpoint atomically.

An interruption leaves the group detached and coverage unavailable. Rerun the
same command to continue from the last committed checkpoint. Do not start the
processor between interrupted batches. Completion reattaches the group and
marks coverage ready in one transaction; a HAF version that changes the cursor
on attachment fails this step instead of silently skipping blocks.

Maintenance duration depends on retained history and hardware. Measure a
representative replay before scheduling the production window; the correctness
tests alone do not establish full-mainnet throughput.

An isolated replay of the complete market-operation prefix through block
3,200,000 processed 90,752 events and retained 42,701 creations. Three warmed
runs took 19.3–20.8 seconds; the median reducer throughput was about 4,626 events
per second. These timings include the reducer, parsers and three lifecycle
indexes, but exclude source decoding, full block-header scans, batch commits
and live fork tracking. They establish a baseline for sizing, rather than a
production maintenance duration.

At that size, the global full-cohort query took about 9.3 ms. A one-day global
cohort used the creation-time index and took 2.1 ms; account queries used the
owner/time index and took about 0.36 ms. Broad network cohorts still count every
selected creation and need measurement against the deployment's actual history.

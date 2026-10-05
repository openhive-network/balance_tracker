# Order lifecycle regression tests

Run from any working directory:

```sh
bash /path/to/balance_tracker/tests/order_lifecycle/run.sh
```

The runner creates and removes its own HAF container and database. It publishes
no ports, uses no existing database or Docker data volume, and copies current SQL
sources through `docker cp`, which also works with a Docker-in-Docker daemon.
Docker and registry access are required. The default image is
`registry.gitlab.syncad.com/hive/haf:3c237ec1`, the same HAF revision used by the
application CI. Override `ORDER_LIFECYCLE_HAF_IMAGE` with CI's `HAF_IMAGE_NAME` to
test the selected upstream image. `ORDER_LIFECYCLE_TEST_LOG_DIR` selects a retained
log directory; the default is a printed temporary directory. CI logs under
`tests/order_lifecycle/logs/` are ignored by Git.

The suite loads the actual lifecycle installer/reducer, operation parsers, account
validation, API types/endpoints, synchronization helper, lifecycle views and
maintenance procedure. It uses the image's actual HAF registration, shadow-table
rewind, context grouping, advisory locks, detach/attach and source views. HAF
functions are never stubbed. Accounts, block timestamps and operation payloads
are synthetic inputs in that isolated database.

Coverage includes:

- Canonical operation order, maker/taker partial and complete fills, successful
  immediate fill-or-kill/create2, explicit and virtual-only cancellations,
  expiration, rounded zero-paying fill followed by dust refund, and HF23 clearing.
- Reused IDs within/across ranges; invalid or unknown input rejection with
  unchanged lifecycle rows and watermark; gaps, duplicate ranges and missing
  source timestamps.
- Account/global creation cohorts, inclusive UTC dates, historical outcomes,
  reconciliation, rounding, empty results, account errors, short cache and 503
  for incomplete or mismatched coverage. Persisted timestamps keep indexed
  cohorts complete after physical removal of raw creation blocks.
- Actual HAF fork rewind restores original orders, timestamps and coverage after
  repeated ID reuse; an alternate branch replays the removed canonical IDs.
- Existing-context upgrade, a busy processor's shared advisory lock returning
  `55P03` before detach, lock release on connection close, pruning/retention
  rejection before detach, actual
  rewind of a reversible projection tail, fixed irreversible target, committed
  checkpoint surviving an intentional failure in a later batch, resume without
  duplicate history, cursor-preserving reattach, subsequent normal processing,
  and an embedded parent/tracker context group with distinct owners. The actual
  wrapper selects the parent `hafbe_owner` role, which inherits `btracker_owner`;
  the tracker role cannot impersonate the parent owner.

The intentionally failing batch exercises PostgreSQL transaction failure and
resume across real procedure commits. The separate fork tests exercise actual
HAF undo; transaction rollback alone is not used as evidence of fork safety.
Synthetic inputs do not verify hived's production of the operation stream or
represent a performance benchmark.

Validated on HAF commit `3c237ec1495da795679f767a7aa40c8b2d583dfa` with 123
assertions. The test logs print the installed HAF revision and assertion count.

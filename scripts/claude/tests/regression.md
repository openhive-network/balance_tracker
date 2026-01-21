# Regression Tests

## Overview

Regression tests compare Balance Tracker's computed account balances against expected values from a hived node snapshot. This validates that balance processing logic produces correct results for real blockchain data.

## Running These Tests

```bash
cd tests/regression

# Run with defaults (localhost:5432)
./run_test.sh

# Run with custom connection
./run_test.sh --host=haf-instance --port=5432 --user=haf_admin

# Run with custom schema
./run_test.sh --schema=btracker_app
```

## Test File Organization

```
tests/regression/
├── run_test.sh                  # Main test runner
├── install_test_schema.sh       # Creates btracker_test schema
├── load_expected_balances.py    # Python loader for expected values
├── accounts_dump.json.gz        # Compressed expected balance fixture
└── sql/
    ├── 00_schema.sql            # Test schema definition
    ├── 01_load_expected_data.sql # Data loading function
    └── 02_compare_accounts.sql   # Comparison function
```

## Key Test Files

| File | Purpose |
|------|---------|
| `run_test.sh` | Orchestrates the full test workflow |
| `accounts_dump.json.gz` | Expected balances from hived at block 5M |
| `load_expected_balances.py` | Parses JSON and bulk-inserts into database |
| `sql/02_compare_accounts.sql` | SQL function that compares expected vs actual |

## Test Workflow

```
1. Install test schema (btracker_test)
   └── Creates tables: expected_account_balances, differing_accounts

2. Clear previous test data
   └── Truncates test tables

3. Decompress fixture
   └── gunzip accounts_dump.json.gz

4. Load expected values
   └── Python script parses JSON → bulk insert

5. Run comparison
   └── SQL function compares expected vs btracker_app.current_account_balances

6. Check results
   └── Count differing_accounts → EXIT 0 (pass) or EXIT 1 (fail)
```

## How to Add New Regression Tests

### Recreating the Fixture

To create a new fixture for a different block height:

```bash
# 1. Configure hived with high API limit
# In hived config: api-limit = 10000000

# 2. Sync hived to target block and stabilize

# 3. Fetch account data
curl -s -o accounts_dump.json \
  --data '{"jsonrpc":"2.0", "method":"database_api.list_accounts", \
           "params": {"start":"", "limit":10000000, "order":"by_name"}, \
           "id":1}' \
  "http://localhost:8091"

# 4. Clean single quotes (can cause issues)
sed -i "s/'//g" accounts_dump.json

# 5. Compress
gzip accounts_dump.json

# 6. Sync Balance Tracker to SAME block height

# 7. Run test
./run_test.sh
```

### Adding New Comparison Fields

1. Update `sql/00_schema.sql` with new columns
2. Update `load_expected_balances.py` to extract new fields
3. Update `sql/02_compare_accounts.sql` comparison logic

## Comparison Logic

The `btracker_test.compare_accounts()` function:

```sql
-- For each expected account:
-- 1. Look up actual balance from btracker_app.current_account_balances
-- 2. Compare HIVE, HBD, VESTS balances
-- 3. Insert mismatches into differing_accounts table
```

Fields compared:
- `hive_balance` (NAI 21)
- `hbd_balance` (NAI 13)
- `vests_balance` (NAI 37)
- Savings balances
- Rewards

## Common Failure Causes

| Error | Likely Cause | Fix |
|-------|--------------|-----|
| "X accounts have discrepancies" | Balance calculation bug | Debug using `get_account_comparison()` |
| "Fixture file not found" | Missing `accounts_dump.json.gz` | Ensure fixture exists in test directory |
| Block mismatch | Balance Tracker at different block | Sync to same block as fixture |
| psycopg2 import error | Missing Python dependency | `pip install psycopg2-binary` |

## Debugging Failures

When accounts differ, use the debug function:

```sql
SELECT * FROM btracker_test.get_account_comparison(12345);
-- Returns: expected values, actual values, differences for account_id 12345
```

List all differing accounts:

```sql
SELECT * FROM btracker_test.differing_accounts LIMIT 20;
```

## Test Schema

The regression test creates a separate schema `btracker_test` to avoid polluting the main application schema:

```sql
CREATE SCHEMA IF NOT EXISTS btracker_test;

-- Expected values loaded from hived snapshot
CREATE TABLE btracker_test.expected_account_balances (
    account_id INT PRIMARY KEY,
    hive_balance BIGINT,
    hbd_balance BIGINT,
    vests_balance NUMERIC(30,6),
    ...
);

-- Accounts where expected != actual
CREATE TABLE btracker_test.differing_accounts (
    account_id INT PRIMARY KEY
);
```

## CI Integration

The `regression-test` job in `.gitlab-ci.yml`:

```yaml
regression-test:
  extends: .test-with-docker-compose
  script:
    - cd "${CI_PROJECT_DIR}/tests/regression"
    - ./run_test.sh --host=docker
  artifacts:
    paths:
      - tests/regression/regression_test.log
    when: always
```

### Prerequisites

- Balance Tracker must be synced to the same block height as the fixture
- The CI pipeline uses 5M block sync cache which matches the fixture

## Expansion Rules

When modifying regression tests:
1. Update fixture if adding new balance types
2. Update comparison SQL for new fields
3. Document any block height requirements
4. Ensure CI sync target matches fixture block height

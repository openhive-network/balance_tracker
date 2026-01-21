# Functional Tests

## Overview

Functional tests validate Balance Tracker's shell scripts and installation procedures. These tests ensure that the application can be correctly installed, reinstalled, and uninstalled without errors.

## Running These Tests

```bash
cd tests/functional

# Run with defaults (localhost)
./test_scripts.sh

# Run against remote host
./test_scripts.sh --host=haf-instance
```

## Test File Organization

```
tests/functional/
└── test_scripts.sh      # Main functional test script
```

## Key Test Scenarios

The `test_scripts.sh` runs these tests in sequence:

| Test | Description | Scripts Tested |
|------|-------------|----------------|
| 1. Generate version | Creates version.sql from git info | `scripts/generate_version_sql.sh` |
| 2. Reinstall app | Full reinstall of Balance Tracker schema | `scripts/install_app.sh` |
| 3. Uninstall app | Clean removal of all Balance Tracker objects | `scripts/uninstall_app.sh` |

## Test Workflow

```
1. Generate version SQL
   └── scripts/generate_version_sql.sh creates db/version.sql

2. Install app
   └── scripts/install_app.sh creates schemas, tables, functions

3. Uninstall app
   └── scripts/uninstall_app.sh drops all Balance Tracker objects
```

## How to Add New Functional Tests

### 1. Edit test_scripts.sh

Add a new test section:

```bash
echo "Test N. Description..."
./path/to/script.sh --postgres-host="$POSTGRES_HOST"
echo "Description completed successfully"
```

### 2. Test Pattern

Each test follows this pattern:
1. Print test description
2. Execute script with appropriate flags
3. Script exits non-zero on failure (bash `set -e`)
4. Print success message

### 3. Exit on First Failure

The script uses `set -euo pipefail` which:
- `-e`: Exit immediately on any command failure
- `-u`: Treat unset variables as errors
- `-o pipefail`: Pipeline fails if any command fails

## Scripts Tested

| Script | Purpose | Key Options |
|--------|---------|-------------|
| `generate_version_sql.sh` | Creates version SQL from git | Project root path |
| `install_app.sh` | Installs Balance Tracker schema | `--postgres-host`, `--postgres-port` |
| `uninstall_app.sh` | Removes Balance Tracker schema | `--host`, `--port` |

## Common Failure Causes

| Error | Likely Cause | Fix |
|-------|--------------|-----|
| "psql: connection refused" | PostgreSQL not running | Start database |
| "role does not exist" | Missing HAF roles | Ensure HAF is installed |
| "permission denied" | Wrong user | Use `haf_admin` user |
| "git: command not found" | Git not installed | Install git |
| Schema exists error | Previous install | Run uninstall first |

## Prerequisites

For these tests to pass:

1. **HAF installed**: The HAF application framework must be set up
2. **Database accessible**: PostgreSQL running and accessible
3. **Git available**: For version generation
4. **haf_admin role**: Must exist with appropriate permissions

## CI Integration

The `setup-scripts-test` job in `.gitlab-ci.yml`:

```yaml
setup-scripts-test:
  extends: .test-with-docker-compose
  script:
    - cd "${CI_PROJECT_DIR}/tests/functional"
    - ./test_scripts.sh --host=docker
```

### CI Environment

In CI, the test runs after:
1. HAF data is prepared
2. Balance Tracker is synced
3. Docker Compose environment is running

The test validates that:
- Scripts work correctly in the CI environment
- Install/uninstall cycle doesn't leave orphan objects
- Version generation works with CI git state

## Expansion Rules

When adding new functional tests:
1. Add test section to `test_scripts.sh`
2. Follow the existing test pattern (echo, run, echo success)
3. Ensure test is idempotent (can run multiple times)
4. Update this documentation with new test scenarios

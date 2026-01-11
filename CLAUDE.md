# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Balance Tracker is a HAF (Hive Application Framework) application that indexes and tracks HBD/Hive account balances across blocks and time. It provides a PostgREST API backend and React web UI for querying balance history.

## Common Commands

### Setup & Running (PostgREST backend - recommended)
```bash
./balance-tracker.sh install backend-runtime-dependencies
./balance-tracker.sh install postgrest
./balance-tracker.sh install-app  # requires HAF database on localhost:5432
./balance-tracker.sh process-blocks --number-of-blocks=5000000
./balance-tracker.sh serve postgrest-backend
./balance-tracker.sh install frontend-runtime-dependencies
./balance-tracker.sh serve frontend
```

### Linting
```bash
# SQL linting (sqlfluff)
sqlfluff lint --format yaml .

# Bash script linting
shellcheck scripts/*.sh
```

### Testing
```bash
# Install test dependencies
./balance-tracker.sh install test-dependencies
./balance-tracker.sh install jmeter

# Run JMeter performance tests
./balance-tracker.sh run-tests

# Tavern API tests (requires running PostgREST backend)
cd tests/tavern
BTRACKER_ADDRESS=localhost BTRACKER_PORT=3000 pytest -n 16 --junitxml report.xml .

# Run single Tavern test
BTRACKER_ADDRESS=localhost BTRACKER_PORT=3000 pytest tests/tavern/test_balances.tavern.yaml -v

# Functional tests
tests/functional/test_scripts.sh --host=localhost
```

### Docker
```bash
# Quick start with 5M block_log
curl https://gtg.openhive.network/get/blockchain/block_log.5M -o docker/blockchain/block_log
cd docker && docker compose up -d

# Build images
docker buildx bake full
```

## Architecture

**Two Backend Implementations:**
- **PostgREST (primary)**: SQL procedures exposed as REST API via `postgrest.conf`
- **Python (legacy)**: `main.py` → `server/serve.py` → `db/backend.py`

**Key Directories:**
- `db/` - Database schema (`btracker_app.sql`) and block processing procedures (`process_*.sql`)
- `backend/` - SQL modules for queries (aggregations, transfers, delegations, rewards, savings, withdrawals)
- `endpoints/` - API endpoint SQL (`endpoint_schema.sql` contains OpenAPI spec embedded in SQL comments)
- `server/` - Python HTTP server (legacy)
- `gui/` - React frontend (Material-UI, Chart.js)
- `tests/tavern/` - REST API integration tests (YAML-based)
- `tests/performance/` - JMeter performance tests

**Database Schema:** `btracker_app` with tables:
- `current_account_balances` - Current balance state per account/currency
- `account_balance_history` - Full balance change history
- `balance_history_by_month/day/hour` - Aggregated historical data
- Tables for: delegations, rewards, savings, withdrawals, transfers, escrows, orders

**Data Flow:**
1. HAF provides blockchain data via `hive.operations_view`
2. `process_*.sql` procedures extract and transform operations into balance_tracker tables
3. PostgREST exposes `endpoints/` SQL functions as REST API
4. React frontend queries the API

## HAF Submodule

The `haf/` submodule must stay synchronized across three locations:
- `.gitlab-ci.yml` variable `HAF_COMMIT`
- `.gitlab-ci.yml` include `ref:`
- `docker/.env` variable `HAF_VERSION`

To disable if not needed: `git config --local submodule.haf.update none`

## CI/CD Architecture

### Pipeline Stages Overview

```
detect → lint → build → sync → test → cleanup → publish
```

### Stage Details

**DETECT stage:**
- `detect_changes` - Checks if only docs/tests changed (sets `AUTO_SKIP_SYNC`)
- `find_haf_image` - Resolves `HAF_IMAGE_NAME` from `HAF_COMMIT`

**BUILD stage:**
- `prepare_haf_image` - Ensures HAF image is available in registry
- `prepare_haf_data` - Gets pre-replayed HAF data location from HAF's cache
- `docker-*-build` - Builds CI runner images with required tools
- `generate-wax-spec` - Generates OpenAPI spec from SQL endpoints
- `generate_python_api_client` - Builds Python API client from OpenAPI spec

**SYNC stage:**
- `sync` - Syncs balance_tracker on top of pre-replayed HAF data
- `sync_with_mock_data` - Same but with deterministic mock data for pattern tests

**TEST stage:** (all use Docker-in-Docker)
- `regression-test` - Account balance dump verification
- `setup-scripts-test` - Tests install/setup scripts
- `performance-test` - JMeter load tests against PostgREST
- `pattern-test` - Tavern API tests against real synced data
- `pattern-test-with-mock-data` - Tavern tests against deterministic mock data

### Sync Job Architecture

The sync job does NOT replay HAF from scratch. It uses pre-replayed HAF data and only syncs balance_tracker on top:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ 1. CACHE LOOKUP                                                         │
│    Check NFS: /nfs/ci-cache/haf_btracker_sync/<cache_key>.tar           │
│    Cache key = f(HAF_COMMIT, btracker commit, block count)              │
│                                                                         │
│    HIT  → Extract tarball (already has HAF + btracker synced)           │
│    MISS → Get base HAF data from haf/ cache (pre-replayed, no app data) │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 2. START ENVIRONMENT (docker-compose up)                                │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ HAF container (PostgreSQL)                                      │    │
│  │  - Mounts pre-replayed pgdata                                   │    │
│  │  - command: --skip-hived (no replay needed)                     │    │
│  │  - Contains: hive schema with 5M blocks of operations           │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                          │ depends_on: healthy                          │
│                          ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ backend-setup container                                         │    │
│  │  - Runs: install_app                                            │    │
│  │  - Creates btracker_app schema, tables, functions               │    │
│  │  - One-shot, exits when done                                    │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                          │ depends_on: completed_successfully           │
│                          ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ backend-block-processing container                              │    │
│  │  - Runs: process_blocks                                         │    │
│  │  - Reads hive.operations_view → writes to btracker_app tables   │    │
│  │  - Processes 5M blocks of balance operations (~5 minutes)       │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 3. WAIT & SAVE                                                          │
│    - wait-for-bt-startup.sh polls until processing reaches head         │
│    - Tar pgdata (now has HAF + btracker data) → push to NFS cache       │
└─────────────────────────────────────────────────────────────────────────┘
```

### Test Job Architecture (Docker-in-Docker)

All test jobs use the same DinD pattern to avoid race conditions:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ GitLab Runner (CI job container)                                        │
│                                                                         │
│  1. Get BTRACKER_CACHE_KEY from sync job artifacts                      │
│  2. Extract NFS cache → ${CI_PROJECT_DIR}/${CI_JOB_ID}/                  │
│  3. docker-compose up (inside DinD)                                     │
│                                                                         │
│     ┌─────────────────────────────────────────────────────────────┐     │
│     │ Docker-in-Docker network                                    │     │
│     │                                                             │     │
│     │  ┌─────────────────┐      ┌─────────────────┐               │     │
│     │  │ HAF container   │      │ PostgREST       │               │     │
│     │  │ (PostgreSQL)    │◄─────│ (API gateway)   │               │     │
│     │  │ port 5432       │      │ port 3000       │               │     │
│     │  └────────┬────────┘      └─────────────────┘               │     │
│     │           │ volume bind                                     │     │
│     │           ▼                                                 │     │
│     │  ${CI_PROJECT_DIR}/${CI_JOB_ID}/datadir                     │     │
│     └─────────────────────────────────────────────────────────────┘     │
│                                                                         │
│  4. Run tests against docker:5432 / docker:3000                         │
│  5. docker-compose down, cleanup                                        │
└─────────────────────────────────────────────────────────────────────────┘
```

### Cache Architecture

**Two-level caching:**
- **HAF cache** (`haf_sync`): Pre-replayed HAF data (hive schema + 5M blocks)
- **App cache** (`haf_btracker_sync`): HAF + balance_tracker schema + processed blocks

**Cache locations:**
- NFS shared: `/nfs/ci-cache/<cache_type>/<key>.tar`
- Local per-builder: `/cache/<cache_type>/<key>/` (extracted)

**Cache key components:**
- HAF commit SHA
- App commit SHA
- Block count (5M)

### CI/CD Notes

**Quick Test Mode:** Set `QUICK_TEST=true` and `QUICK_TEST_HAF_COMMIT=<sha>` to skip sync and use cached data for faster iteration on SQL/test changes.

**Cache Types:** Use `haf_` prefix for cache types (e.g., `haf_btracker_sync`) - cache-manager only auto-relaxes pgdata permissions for types starting with `haf*`.

**Python Compatibility:** Jobs using pydantic-core (like `generate_python_api_client`) use `PYTHON_COMPAT_IMAGE` (Python 3.12) since pydantic-core doesn't support Python 3.14 yet.

**PostgREST Configuration:** PostgREST requires different roles:
- `PGRST_DB_URI`: Uses `_owner` role (e.g., `btracker_owner`) for schema introspection
- `PGRST_DB_ANON_ROLE`: Uses `_user` role (e.g., `btracker_user`) for API request security

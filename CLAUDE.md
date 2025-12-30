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

## CI/CD Notes

Pipeline stages: detect → lint → build → sync → test → cleanup → publish

**Quick Test Mode:** Set `QUICK_TEST=true` and `QUICK_TEST_HAF_COMMIT=<sha>` to skip sync and use cached data for faster iteration on SQL/test changes.

**Cache Types:** Use `haf_` prefix for cache types (e.g., `haf_btracker_sync`) - cache-manager only auto-relaxes pgdata permissions for types starting with `haf*`.

**Python Compatibility:** Jobs using pydantic-core (like `generate_python_api_client`) use `PYTHON_COMPAT_IMAGE` (Python 3.12) since pydantic-core doesn't support Python 3.14 yet.

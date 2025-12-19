# Balance Tracker

A HAF (Hive Application Framework) application that tracks HBD and Hive balances for Hive blockchain accounts.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start with Docker](#quick-start-with-docker)
- [Local Installation](#local-installation)
- [Running Tests](#running-tests)

## Overview

Balance Tracker indexes balance-related data from the Hive blockchain for fast querying. It tracks:

- **Account Balances** - HIVE, HBD, and VESTS with full history
- **Savings** - Savings balances and pending withdrawals
- **Delegations** - Vesting share delegations between accounts
- **Rewards** - Pending and earned rewards (author, curation, benefactor)
- **Withdrawals** - Power-down state and routing
- **Recurrent Transfers** - Scheduled recurring transfers
- **Transfer Statistics** - Aggregated volume by hour/day/month

## Architecture

Balance Tracker uses a two-tier architecture:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   REST Client   │────▶│    PostgREST    │────▶│   PostgreSQL    │
│                 │     │   (API Layer)   │     │   (HAF + App)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

- **Database Layer**: PostgreSQL with HAF extensions. All business logic lives in SQL stored procedures.
- **API Layer**: PostgREST exposes SQL functions as REST endpoints with zero application code.

### Directory Structure

| Directory | Purpose |
|-----------|---------|
| `db/` | Core SQL: table definitions, block processing functions |
| `backend/` | SQL backend helpers: balance queries, utilities |
| `endpoints/` | PostgREST API definitions and type definitions |
| `scripts/` | Shell scripts for installation, processing, CI |
| `docker/` | Docker Compose setup and configuration |
| `tests/` | All test suites (see [Running Tests](#running-tests)) |

### Database Schemas

| Schema | Role | Purpose |
|--------|------|---------|
| `btracker_app` | `btracker_owner` | Core tables and processing functions |
| `btracker_backend` | `btracker_owner` | Backend helper functions |
| `btracker_endpoints` | `btracker_user` | PostgREST-exposed API functions |

## Quick Start with Docker

The fastest way to run Balance Tracker with a demo dataset (5M blocks):

```bash
# Clone repository
git clone https://gitlab.syncad.com/hive/balance_tracker.git
cd balance_tracker

# Download demo blockchain data
curl https://gtg.openhive.network/get/blockchain/block_log.5M -o docker/blockchain/block_log

# Start all services
cd docker
docker compose up -d
```

The API will be available at `http://localhost:3000`.

### Docker Commands

```bash
# Stop services (preserves data)
docker compose stop

# Stop and remove containers
docker compose down

# Stop and remove ALL data (clean slate)
docker compose down -v

# Include Swagger UI (port 8080)
docker compose --profile swagger up -d

# Include database tools (PgHero:2080, PgAdmin:1080)
docker compose --profile db-tools up -d

# View logs
docker compose logs -f
```

### Custom Configuration

Create a `.env.local` file to override defaults:

```bash
cd docker

cat <<EOF > .env.local
HAF_REGISTRY=hiveio/haf
HAF_VERSION=v1.27.5.0
HAF_COMMAND=--shared-file-size=2G --plugin database_api --replay --stop-at-block=10000000
EOF

docker compose --env-file .env.local up -d
```

See [docker/README.md](docker/README.md) for advanced configuration options.

## Local Installation

For development or connecting to an existing HAF instance.

### Prerequisites

- HAF instance running with PostgreSQL accessible
- Ubuntu 20.04+ (tested)

### Install Dependencies

```bash
sudo apt-get update
sudo apt-get install -y apache2-utils curl postgresql-client wget xz-utils
```

### Install PostgREST

```bash
wget https://github.com/PostgREST/postgrest/releases/download/v12.2.3/postgrest-v12.2.3-linux-static-x64.tar.xz -O postgrest.tar.xz
tar -xJf postgrest.tar.xz
sudo mv postgrest /usr/local/bin/
rm postgrest.tar.xz
```

### Setup Database

```bash
# Clone repository
git clone https://gitlab.syncad.com/hive/balance_tracker.git
cd balance_tracker

# Install schema (connects to localhost:5432 by default)
./scripts/install_app.sh

# Or specify custom connection
./scripts/install_app.sh --postgres-host=192.168.1.100
```

### Process Blocks

```bash
# Process specific number of blocks
./scripts/process_blocks.sh --blocks=5000000

# Process all available blocks and wait for new ones (live sync)
./scripts/process_blocks.sh
```

### Start API Server

```bash
PGRST_DB_URI="postgres://btracker_user@localhost/haf_block_log" \
PGRST_DB_SCHEMA="btracker_endpoints" \
PGRST_DB_ANON_ROLE="btracker_user" \
PGRST_DB_EXTRA_SEARCH_PATH="btracker_app" \
postgrest
```

The API is now available at `http://localhost:3000`.

### Uninstall

```bash
./scripts/uninstall_app.sh
```

## Running Tests

Balance Tracker includes multiple test suites:

| Test Suite | Purpose | Requirements |
|------------|---------|--------------|
| **Tavern API** | REST endpoint validation | Running API server |
| **Performance** | Load testing with JMeter | Running API server |
| **Regression** | Compare against hived snapshots | Database access |
| **Functional** | Shell script validation | None |

### Tavern API Tests

Pattern-based API tests using the Tavern framework.

```bash
# Install dependencies
pip install tavern pytest

# Set target server
export BTRACKER_ADDRESS=localhost
export BTRACKER_PORT=3000

# Run mainnet tests (requires synced data)
cd tests/tavern/patterns-mainnet
pytest

# Run mock tests (requires mock data loaded)
cd tests/tavern/patterns-mock
pytest

# Run specific endpoint
pytest get_account_balances/ -v
```

See [tests/tavern/README.md](tests/tavern/README.md) for details.

### Performance Tests

JMeter load tests for the PostgREST API.

```bash
# Install JMeter
sudo apt-get install -y openjdk-11-jdk-headless
wget https://downloads.apache.org/jmeter/binaries/apache-jmeter-5.6.3.zip -O jmeter.zip
unzip jmeter.zip && sudo mv apache-jmeter-5.6.3 /usr/local/src/
sudo ln -sf /usr/local/src/apache-jmeter-5.6.3/bin/jmeter.sh /usr/local/bin/jmeter
rm jmeter.zip

# Run tests (API must be running on port 3000)
./scripts/ci-helpers/run_performance_tests.sh

# View HTML report
python3 -m http.server --directory tests/performance/result_report 8000
# Open http://localhost:8000 in browser
```

### Regression Tests

Compare computed balances against expected values from a hived node snapshot.

```bash
cd tests/regression
./run_test.sh --host=localhost --port=5432
```

### Functional Tests

Validate shell scripts and utilities.

```bash
cd tests/functional
./test_scripts.sh
```

### Running All Tests in CI

The GitLab CI pipeline runs all test suites automatically. Key jobs:

- `pattern-test` - Tavern tests against synced mainnet data
- `pattern-test-with-mock-data` - Tavern tests against mock fixtures
- `performance-test` - JMeter load tests
- `regression-test` - Balance comparison tests

## API Examples

```bash
# Get account balances
curl -X POST http://localhost:3000/rpc/get_account_balances \
  -H "Content-Type: application/json" \
  -d '{"_account_name": "gtg"}'

# Get balance history
curl -X POST http://localhost:3000/rpc/get_balance_history \
  -H "Content-Type: application/json" \
  -d '{"_account_name": "gtg", "_balance_type": "HIVE", "_page_size": 10}'

# Get delegations
curl -X POST http://localhost:3000/rpc/get_balance_delegations \
  -H "Content-Type: application/json" \
  -d '{"_account_name": "blocktrades"}'
```

## License

See [LICENSE](LICENSE) file.

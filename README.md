# Balance Tracker

A HAF (Hive Application Framework) application that tracks comprehensive balance information for Hive blockchain accounts. Balance Tracker indexes balance-related blockchain operations in real-time, storing both current state and full historical data for fast querying via REST API.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Tracked Balances](#tracked-balances)
- [Architecture](#architecture)
- [Balance Processing](#balance-processing)
- [Integration with HAFBE](#integration-with-hafbe)
- [Quick Start with Docker](#quick-start-with-docker)
- [Local Installation](#local-installation)
- [Running Tests](#running-tests)
- [API Reference](#api-reference)

## Overview

Balance Tracker processes Hive blockchain operations to extract and index all balance-related data. It maintains both current state (latest balances) and full history (every balance change), enabling queries like "What was account X's balance at block Y?" or "Show me all balance changes in the last month."

The application uses a batch processing architecture that processes thousands of blocks at once during initial sync, then switches to single-block processing for live updates. This design enables fast initial sync while maintaining consistency with the blockchain head.

## Features

- **Real-time balance tracking** - Processes new blocks as they arrive on the Hive blockchain
- **Full history** - Stores every balance change with source operation reference
- **Time-based aggregation** - Pre-computed daily and monthly balance snapshots
- **Multi-asset support** - Tracks HIVE, HBD, and VESTS (Hive Power)
- **Comprehensive coverage** - Savings, delegations, rewards, power-down, recurrent transfers, market orders, conversions, escrows
- **REST API** - PostgREST-based API with OpenAPI specification
- **Fast sync** - Batch processing architecture syncs millions of blocks efficiently

## Tracked Balances

| Category | Description |
|----------|-------------|
| **Liquid Balances** | HIVE, HBD, and VESTS holdings with complete transaction history |
| **Savings** | Savings account balances and pending 3-day withdrawal requests |
| **Rewards** | Pending unclaimed rewards (author, curation, benefactor) and lifetime earned |
| **Delegations** | Vesting share delegations between accounts (delegator ↔ delegatee pairs) |
| **Power-down** | Vesting withdrawal state: rate, remaining amount, routing rules |
| **Recurrent Transfers** | Scheduled recurring transfers with execution tracking |
| **Conversions** | Pending HBD↔HIVE conversions (3.5-day delay) |
| **Market Orders** | Open limit orders on the internal HIVE/HBD market |
| **Escrows** | Multi-party escrow agreements with approval/dispute status |
| **Transfer Statistics** | Aggregated transfer volumes by hour, day, and month |

## Architecture

Balance Tracker uses a two-tier architecture with all business logic in SQL:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   REST Client   │────▶│    PostgREST    │────▶│   PostgreSQL    │
│                 │     │   (API Layer)   │     │   (HAF + App)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

- **Database Layer**: PostgreSQL with HAF extensions. All business logic lives in SQL stored procedures that process blockchain operations and maintain balance state.
- **API Layer**: PostgREST exposes SQL functions as REST endpoints with zero application code.

### Database Schemas

| Schema | Role | Purpose |
|--------|------|---------|
| `btracker_app` | `btracker_owner` | Core tables and block processing functions |
| `btracker_backend` | `btracker_owner` | Backend helper functions (not directly exposed) |
| `btracker_endpoints` | `btracker_user` | PostgREST-exposed API functions (read-only) |

### Directory Structure

| Directory | Purpose |
|-----------|---------|
| `db/` | Core SQL: table definitions, block processing functions |
| `backend/` | SQL backend helpers: balance queries, operation parsers, utilities |
| `endpoints/` | PostgREST API definitions and OpenAPI type definitions |
| `scripts/` | Shell scripts for installation, processing, CI/CD |
| `docker/` | Docker Compose setup and configuration |
| `tests/` | All test suites (Tavern API, regression, performance) |

## Balance Processing

### Overview

Balance updates are processed via SQL stored procedures that extract operations from HAF's `operations_view` and update state tables. Processing uses the "squashing" pattern to efficiently handle large block ranges during initial sync.

### Operations That Affect Balances

| Operation | Effect |
|-----------|--------|
| `transfer` | Move HIVE/HBD between accounts |
| `transfer_to_vesting` | Power up (HIVE → VESTS) |
| `withdraw_vesting` | Start power-down |
| `fill_vesting_withdraw` | Weekly power-down payout |
| `delegate_vesting_shares` | Delegate/undelegate VESTS |
| `claim_reward_balance` | Claim pending rewards |
| `author_reward` / `curation_reward` | Post/voting rewards credited |
| `transfer_to_savings` / `transfer_from_savings` | Savings operations |
| `recurrent_transfer` / `fill_recurrent_transfer` | Scheduled transfers |
| `limit_order_create` / `fill_order` | Market orders |
| `convert` / `fill_convert_request` | HBD↔HIVE conversions |
| `escrow_transfer` / `escrow_release` | Escrow operations |

### Processing Flow

```
HAFBE (hafbe_app.sql)
  └─> btracker_process_blocks(_context, _block_range)
      ├─> process_balances()         # Core HIVE/HBD/VESTS
      ├─> process_delegations()      # Delegation pairs
      ├─> process_rewards()          # Pending rewards
      ├─> process_savings()          # Savings accounts
      ├─> process_withdrawals()      # Power-down state
      ├─> process_block_range_recurrent_transfers()
      ├─> process_block_range_orders()
      ├─> process_block_range_converts()
      ├─> process_block_range_escrows()
      └─> process_transfer_stats()   # Volume aggregation
```

### Key Processing Functions

| Function | File | Purpose |
|----------|------|---------|
| `process_balances` | `db/process_balances.sql` | Core balance tracking with history |
| `process_delegations` | `db/process_delegations.sql` | Delegation pairs with HF23 handling |
| `process_rewards` | `db/process_rewards.sql` | Reward tracking with recursive claim processing |
| `process_block_range_recurrent_transfers` | `db/process_recurrent_transfers.sql` | Scheduled transfer lifecycle |
| `process_block_range_orders` | `db/process_block_range_orders.sql` | Market order state |
| `process_block_range_converts` | `db/process_block_range_converts.sql` | Conversion tracking |
| `process_block_range_escrows` | `db/process_block_range_escrows.sql` | Escrow state and fees |

## Integration with HAFBE

Balance Tracker is designed as a submodule of HAF Block Explorer (HAFBE). When used within HAFBE:

1. **Block Processing**: HAFBE dispatches block ranges to Balance Tracker alongside other submodules (hafah, reptracker)
2. **Data Sharing**: All submodules write to the same PostgreSQL database, enabling cross-module queries
3. **Unified API**: HAFBE endpoints can query Balance Tracker tables directly for combined results

### Standalone Usage

Balance Tracker can also run independently with its own PostgREST instance, providing a focused balance-only API.

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

## API Reference

Balance Tracker exposes SQL functions via PostgREST. For detailed documentation, see `scripts/claude/endpoints.md`.

### Endpoint Summary

| Endpoint | Description |
|----------|-------------|
| `/accounts/{name}/balances` | Full balance snapshot for an account |
| `/accounts/{name}/balance-history` | Paginated balance change history |
| `/accounts/{name}/aggregated-history` | Daily/monthly/yearly balance summaries |
| `/accounts/{name}/delegations` | Incoming and outgoing delegations |
| `/accounts/{name}/recurrent-transfers` | Scheduled recurring transfers |
| `/top-holders` | Leaderboard of top asset holders |
| `/transfer-statistics` | Network-wide transfer volume stats |
| `/version` | Git commit hash |
| `/last-synced-block` | Sync status |
| `/total-value-locked` | TVL metrics |

### Balance Functions

#### get_account_balances

Returns comprehensive balance information for a single account.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| account-name | TEXT | Yes | Hive account name |

**Returns**: HIVE, HBD, VESTS balances + delegations + rewards + savings + power-down state + conversions + market orders + escrows

#### get_balance_history

Returns paginated history of balance changes.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| account-name | TEXT | Yes | - | Account name |
| coin-type | TEXT | Yes | - | HBD, HIVE, or VESTS |
| balance-type | TEXT | No | 'balance' | 'balance' or 'savings_balance' |
| page | INT | No | NULL | Page number |
| page-size | INT | No | 100 | Results per page (max 1000) |

#### get_top_holders

Returns ranked leaderboard of top asset holders.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| coin-type | TEXT | Yes | - | HBD, HIVE, or VESTS |
| balance-type | TEXT | No | 'balance' | 'balance' or 'savings_balance' |
| page | INT | No | 1 | Page number |
| page-size | INT | No | 100 | Results per page |
| min-vests | BIGINT | No | - | Inclusive lower VESTS balance bound; valid only for VESTS |
| max-vests | BIGINT | No | - | Exclusive upper VESTS balance bound; valid only for VESTS |

### Delegation Functions

#### get_balance_delegations

Returns incoming and outgoing VESTS delegations.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| account-name | TEXT | Yes | Hive account name |

**Returns**: Arrays of `{delegator/delegatee, amount, operation_id, block_num}`

### Transfer Functions

#### get_transfer_statistics

Returns aggregated transfer volume statistics.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| coin-type | TEXT | Yes | - | HBD or HIVE |
| granularity | TEXT | No | 'yearly' | hourly/daily/monthly/yearly |
| direction | TEXT | No | 'desc' | asc or desc |

#### get_recurrent_transfers

Returns scheduled recurring transfers for an account.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| account-name | TEXT | Yes | Hive account name |

### Utility Functions

| Function | Returns | Description |
|----------|---------|-------------|
| `get_btracker_version()` | TEXT | Git commit hash |
| `get_btracker_last_synced_block()` | INT | Last processed block |
| `get_total_value_locked()` | RECORD | TVL (VESTS + savings) |

### Usage Examples

#### Query Balance via SQL

```sql
-- Get full balance snapshot
SELECT * FROM btracker_endpoints.get_account_balances('blocktrades');

-- Get HIVE balance history
SELECT * FROM btracker_endpoints.get_balance_history('gtg', 'HIVE', 'balance', 1, 100);

-- Get delegations
SELECT * FROM btracker_endpoints.get_balance_delegations('blocktrades');

-- Get top VESTS holders
SELECT * FROM btracker_endpoints.get_top_holders('VESTS', 'balance', 1, 100);
```

#### Query via REST API

```bash
# Get account balances
curl 'http://localhost:3000/balance-api/accounts/blocktrades/balances'

# Get balance history with filters
curl 'http://localhost:3000/balance-api/accounts/gtg/balance-history?coin-type=HIVE&page-size=50'

# Get delegations
curl 'http://localhost:3000/balance-api/accounts/blocktrades/delegations'

# Get top holders
curl 'http://localhost:3000/balance-api/top-holders?coin-type=HIVE&page-size=100'

# Get transfer statistics
curl 'http://localhost:3000/balance-api/transfer-statistics?coin-type=HBD&granularity=daily'

# Get sync status
curl 'http://localhost:3000/balance-api/last-synced-block'

# Get version
curl 'http://localhost:3000/balance-api/version'

# Get TVL
curl 'http://localhost:3000/balance-api/total-value-locked'
```

## Contributing

### Development Workflow

1. **Clone the repository**
   ```bash
   git clone https://gitlab.syncad.com/hive/balance_tracker.git
   cd balance_tracker
   ```

2. **Set up development environment**
   ```bash
   cd docker
   docker compose up -d
   ```

3. **Make changes** to SQL files in `db/`, `backend/`, or `endpoints/`

4. **Reinstall schema** to apply changes
   ```bash
   ./scripts/install_app.sh --postgres-host=localhost
   ```

5. **Run tests** to verify
   ```bash
   export BTRACKER_ADDRESS=localhost BTRACKER_PORT=3000
   cd tests/tavern/patterns-mainnet && pytest
   ```

### Code Style

- **SQL**: Use lowercase keywords, 2-space indentation, descriptive function names
- **Shell**: Follow shellcheck recommendations, use `set -euo pipefail`
- **Python**: Follow PEP 8, use type hints

### Submitting Changes

1. Create a feature branch from `develop`
2. Make atomic commits with conventional commit messages
3. Ensure all tests pass locally
4. Create a merge request to `develop`
5. Address review feedback

### Documentation

When adding new features:
- Update `scripts/claude/*.md` documentation
- Add tests for new endpoints
- Update this README if adding user-facing functionality

## License

See [LICENSE](LICENSE) file.

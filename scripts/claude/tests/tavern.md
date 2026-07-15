# Tavern API Tests

## Overview

Tavern tests validate Balance Tracker's REST API endpoints using pattern-based response matching. Tests are organized into two categories: mainnet tests (real blockchain data) and mock tests (predictable fixture data).

## Running These Tests

```bash
# Install dependencies
pip install tavern pytest pytest-xdist

# Set environment variables
export BTRACKER_ADDRESS=localhost
export BTRACKER_PORT=3000

# Run mainnet pattern tests
cd tests/tavern/patterns-mainnet
pytest

# Run mock pattern tests
cd tests/tavern/patterns-mock
pytest

# Run with parallel workers
pytest -n 16

# Run specific endpoint tests
pytest get_account_balances/ -v

# Run tests matching a pattern
pytest -k "gtg" -v

# Generate JUnit report (CI)
pytest --junitxml report.xml
```

## Test File Organization

```
tests/tavern/
├── common.yaml                    # Shared config (server address, port)
├── pytest.ini                     # Pytest markers configuration
├── README.md                      # User documentation
├── patterns-mainnet/              # Tests against synced blockchain data
│   ├── get_account_balances/
│   │   ├── blocktrades.tavern.yaml
│   │   ├── blocktrades.pat.json   # Expected response pattern
│   │   ├── gtg.tavern.yaml
│   │   ├── gtg.pat.json
│   │   └── non_existent_account.tavern.yaml
│   ├── get_balance_history/
│   │   ├── positive/              # Success case tests
│   │   │   ├── hive.tavern.yaml
│   │   │   └── hbd.tavern.yaml
│   │   └── negative/              # Error case tests
│   │       ├── invalid_timestamp.tavern.yaml
│   │       └── exceeds_page_size.tavern.yaml
│   ├── get_balance_delegations/
│   ├── get_history_aggregation/
│   ├── get_recurrent_transfers/
│   ├── get_top_holders/
│   └── get_transfer_statistics/
└── patterns-mock/                 # Tests against mock fixture data
    └── {endpoint}/
        └── *.tavern.yaml
```

## Key Test Files

| Endpoint | Test Directory | Purpose |
|----------|----------------|---------|
| `get_account_balances` | `patterns-mainnet/get_account_balances/` | Full balance snapshot queries |
| `get_balance_history` | `patterns-mainnet/get_balance_history/` | Balance change pagination |
| `get_balance_delegations` | `patterns-mainnet/get_balance_delegations/` | Delegation pair queries |
| `get_history_aggregation` | `patterns-mainnet/get_history_aggregation/` | Daily/monthly aggregates |
| `get_recurrent_transfers` | `patterns-mainnet/get_recurrent_transfers/` | Scheduled transfer queries |
| `get_top_holders` | `patterns-mainnet/get_top_holders/` | Leaderboard queries |
| `get_transfer_statistics` | `patterns-mainnet/get_transfer_statistics/` | Volume statistics |

## How to Add New Tavern Tests

### 1. Create Test File

Create a `.tavern.yaml` file in the appropriate endpoint directory:

```yaml
test_name: Get account balances for my_account
marks:
  - patterntest
includes:
  - !include ../../common.yaml

stages:
  - name: Request account balances
    request:
      url: "{service.proto}://{service.server}:{service.port}/rpc/get_account_balances"
      method: POST
      json:
        _account_name: "my_account"
    response:
      status_code: 200
      verify_response_with:
        function: tavern.helpers:validate_regex
        extra_kwargs:
          expression: '.*"hive_balance".*'
```

### 2. Add Pattern File (Optional)

For complex response validation, create a `.pat.json` file:

```json
{
  "hive_balance": "\\d+",
  "hbd_balance": "\\d+",
  "vests_balance": "\\d+\\.\\d{6}"
}
```

### 3. Test Categories

| Category | Location | Purpose |
|----------|----------|---------|
| Positive tests | `{endpoint}/positive/` | Valid request scenarios |
| Negative tests | `{endpoint}/negative/` | Error handling verification |
| Pattern tests | Root of endpoint dir | General validation |

**Paginated endpoints** (those taking `page` / `page-size`, e.g. `get_balance_history`,
`get_account_vesting_history`, `get_top_holders`) must include negative tests for each
invalid page-size boundary: `null`, `<= 0`, and `> 1000`. The `null` case must be sent
via an RPC POST body (`page-size: null`) — a query string cannot express SQL NULL, and a
null page-size otherwise flows through to `LIMIT NULL` (an unbounded scan). All are
expected to return `400` (`btracker_backend.validate_negative_limit` / `validate_limit`).

### 4. Pytest Markers

Available markers in `pytest.ini`:

- `patterntest` - Standard pattern-based tests
- `negative` - Tests expecting error responses

## Common Test Patterns

### Basic Endpoint Test

```yaml
test_name: Get balances for known account
stages:
  - name: Request
    request:
      url: "{service.proto}://{service.server}:{service.port}/rpc/get_account_balances"
      method: POST
      json:
        _account_name: "blocktrades"
    response:
      status_code: 200
```

### Testing Error Responses

```yaml
test_name: Invalid page size returns error
marks:
  - negative
stages:
  - name: Request with invalid page size
    request:
      url: "{service.proto}://{service.server}:{service.port}/rpc/get_balance_history"
      method: POST
      json:
        _account_name: "gtg"
        _coin_type: "HIVE"
        _page_size: 10000  # Exceeds maximum
    response:
      status_code: 400
```

### Using Pattern Files

```yaml
stages:
  - name: Request
    request: ...
    response:
      status_code: 200
      verify_response_with:
        function: tests_api.patterns:match_pattern_file
        extra_kwargs:
          pattern_file: "gtg.pat.json"
```

## Mock Data System

Mock tests use pre-defined fixture data for predictable, fast testing.

### Loading Mock Data

```bash
# Install mock operations and blocks
./tests/mocks/install_mock_data.sh --host=localhost

# Process mock blocks
./scripts/process_blocks.sh
```

### Mock Data Structure

```
tests/mocks/
├── fixtures/
│   ├── blocks/               # Mock block headers (required)
│   ├── savings/              # Savings operation fixtures
│   ├── rewards/              # Reward operation fixtures
│   ├── recurrent_transfers/  # Recurrent transfer fixtures
│   ├── delegations/          # Delegation fixtures
│   ├── delays/               # HF24 delayed voting fixtures
│   └── escrow/               # Escrow operation fixtures
├── sql/
│   └── *.sql                 # Insertion functions
└── install_mock_data.sh      # Installation script
```

### Block Range Convention

Mock blocks use block numbers >= 90,000,000 to avoid conflicts with real blockchain data.

## Common Failure Causes

| Error | Likely Cause | Fix |
|-------|--------------|-----|
| Connection refused | PostgREST not running | Start API: `docker compose up -d` |
| 404 Not Found | Wrong endpoint path | Check `endpoints/` SQL function names |
| Empty response | Data not synced | Wait for sync or load mock data |
| Pattern mismatch | Response format changed | Update `.pat.json` pattern file |
| Timeout | Slow query | Check indexes, increase timeout |
| `BTRACKER_ADDRESS` undefined | Env vars not set | Export required environment variables |

## CI Integration

### Mainnet Tests (`pattern-test` job)

```yaml
pattern-test:
  extends: .test-with-docker-compose-tavern
  variables:
    TAVERN_DIR: $CI_PROJECT_DIR/tests/tavern/patterns-mainnet
    PYTEST_WORKERS: "16"
```

### Mock Tests (`pattern-test-with-mock-data` job)

```yaml
pattern-test-with-mock-data:
  extends: .test-with-docker-compose-tavern
  variables:
    APP_SYNC_CACHE_TYPE: "${BTRACKER_MOCK_CACHE_TYPE}"
    TAVERN_DIR: $CI_PROJECT_DIR/tests/tavern/patterns-mock
```

## Expansion Rules

When adding new endpoint tests:
1. Create test directory under `patterns-mainnet/{endpoint_name}/`
2. Add positive/negative subdirectories if needed
3. Include pattern files for complex response validation
4. Update CI if new markers are needed

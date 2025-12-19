# Tavern API Tests

API integration tests using the [Tavern](https://tavern.readthedocs.io/) framework for testing PostgREST endpoints.

## Structure

```
tests/tavern/
├── common.yaml          # Shared configuration (server address, port)
├── pytest.ini           # Pytest markers configuration
├── patterns-mainnet/    # Tests against real synced blockchain data
│   └── {endpoint}/
│       ├── *.tavern.yaml
│       ├── negative/    # Error case tests (optional)
│       └── positive/    # Success case tests (optional)
└── patterns-mock/       # Tests against mock fixture data
    └── {endpoint}/
        └── *.tavern.yaml
```

## Test Types

| Directory | Purpose | Data Source |
|-----------|---------|-------------|
| `patterns-mainnet` | Validate API against real blockchain | HAF synced to mainnet |
| `patterns-mock` | Fast CI tests with predictable data | Mock SQL fixtures |

## Running Tests

```bash
# Set environment variables
export BTRACKER_ADDRESS=localhost
export BTRACKER_PORT=3000

# Run mainnet pattern tests
cd tests/tavern/patterns-mainnet
pytest

# Run mock pattern tests
cd tests/tavern/patterns-mock
pytest

# Run specific endpoint tests
pytest get_account_balances/

# Run with verbose output
pytest -v
```

## Writing Tests

Each `.tavern.yaml` file defines one or more test stages:

```yaml
test_name: Get account balances for gtg
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
        _account_name: "gtg"
    response:
      status_code: 200
      verify_response_with:
        function: tavern.helpers:validate_regex
        extra_kwargs:
          expression: '.*"hive_balance".*'
```

## Markers

- `patterntest` - Tests using regex patterns to validate responses
- `negative` - Tests expecting error responses

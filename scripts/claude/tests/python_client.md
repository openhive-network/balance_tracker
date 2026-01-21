# Python API Client Tests

## Overview

Tests for the auto-generated Python API client that provides type-safe access to Balance Tracker endpoints. The client is generated from the OpenAPI specification extracted from the PostgREST schema.

## Running These Tests

```bash
cd scripts/python_api_package

# Install dependencies
poetry install

# Run tests
poetry run pytest tests/

# Run with verbose output
poetry run pytest tests/ -v
```

## Test File Organization

```
scripts/python_api_package/
├── pyproject.toml                     # Poetry project configuration
├── generate_balance_tracker_api_client.sh  # Client generator script
├── balance_api/
│   └── balance_api_client/            # Generated client code
└── tests/
    ├── __init__.py
    ├── api_caller.py                  # HTTP request utilities
    ├── api_collection.py              # Test endpoint definitions
    └── test_generated_api_client.py   # Main test file
```

## Key Test Files

| File | Purpose |
|------|---------|
| `test_generated_api_client.py` | Validates generated client works against live API |
| `api_collection.py` | Defines test cases and expected responses |
| `api_caller.py` | HTTP utilities for making API requests |

## Test Scenarios

The tests validate:

1. **Client Generation**: The OpenAPI-to-Python generation succeeds
2. **Type Safety**: Generated types match API responses
3. **Endpoint Access**: All exposed endpoints are accessible
4. **Response Parsing**: JSON responses deserialize correctly

## How to Add New Client Tests

### 1. Add Test Case to api_collection.py

```python
TEST_CASES = [
    {
        "endpoint": "get_account_balances",
        "params": {"account_name": "blocktrades"},
        "expected_fields": ["hive_balance", "hbd_balance", "vests_balance"]
    },
    # Add new test case here
]
```

### 2. Add Test Method (if needed)

```python
def test_new_endpoint():
    client = BalanceApiClient(base_url=API_URL)
    response = client.new_endpoint(param="value")
    assert response.expected_field is not None
```

## Client Generation

The client is generated from the OpenAPI spec:

```bash
# Generate OpenAPI JSON from PostgREST
./scripts/python_api_package/generate_balance_tracker_api_client.sh
```

This script:
1. Extracts OpenAPI spec from `endpoints/endpoint_schema.sql`
2. Generates Python client using `datamodel-codegen`
3. Outputs to `balance_api/balance_api_client/`

## Common Failure Causes

| Error | Likely Cause | Fix |
|-------|--------------|-----|
| Connection refused | API not running | Tests use external `api.syncad.com` |
| Import error | Client not generated | Run generation script first |
| Type mismatch | API response changed | Regenerate client |
| Missing dependency | Poetry deps not installed | Run `poetry install` |

## CI Integration

The `python_api_client_test` job in `.gitlab-ci.yml`:

```yaml
python_api_client_test:
  extends: .project_develop_configuration_template
  image: "${PYTHON_COMPAT_IMAGE}"
  allow_failure: true  # Depends on external api.syncad.com
  needs:
    - job: generate_python_api_client
  script:
    - poetry run -C "${PYPROJECT_DIR}" pytest "${PYPROJECT_DIR}/tests"
```

### CI Notes

- **External Dependency**: Tests run against `api.syncad.com`
- **Allow Failure**: Marked as `allow_failure: true` since external API may be unavailable
- **Python Version**: Uses Python 3.12 compatibility image (pydantic-core requirement)

## Package Publishing

After tests pass, the client is published:

1. **Build wheel**: `build_python_api_client_wheel` job
2. **Deploy to GitLab**: `deploy_python_api_packages_to_gitlab` job

Published package: `@hive/balance-tracker-api-client`

## Expansion Rules

When modifying Python client tests:
1. Update `api_collection.py` for new endpoints
2. Regenerate client if API spec changes
3. Ensure tests remain independent of local infrastructure
4. Update this documentation with new test scenarios

# Python API Client Tests

## Overview

Tests for the auto-generated Python API client that provides type-safe access to Balance Tracker endpoints. The client is generated from the OpenAPI specification extracted from the PostgREST schema.

## Running These Tests

The package does not pin pytest or pytest-asyncio: they come from common-ci's
`python-test-tools` image (versions set in common-ci-configuration's
`docker-bake.hcl`). Run the tests in that image, where poetry installs the
package's dependencies into the image venv next to the tools:

```bash
docker run --rm -it -v "$PWD:/src" -w /src \
  registry.gitlab.syncad.com/hive/common-ci-configuration/python-test-tools:develop bash -c '
    poetry -C scripts/python_api_package install &&
    pytest scripts/python_api_package/tests/test_generated_api_client.py -v'
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

`python_api_client_test` and `test_python_api_wheel` in `.gitlab-ci.yml` extend
`.python_api_package_test_template`, which runs in `${PYTHON_TEST_TOOLS_IMAGE}`:

```yaml
python_api_client_test:
  extends: .python_api_package_test_template
  needs:
  - job: generate_python_api_client
  before_script:
  - !reference [.python_api_package_test_template, before_script]
  - poetry -C "${PYPROJECT_DIR}" install
  script:
  - pytest "${PYPROJECT_DIR}/tests/test_generated_api_client.py"
```

### CI Notes

- **External Dependency**: Tests run against `api.syncad.com` (falling back to `api.hive.blog`)
- **Test tools**: pytest and pytest-asyncio are the image's; the job log lists their versions. Do not add them to `pyproject.toml`
- **Python Version**: the image's (3.14). It has no C compiler, so every locked dependency needs a wheel for that Python

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

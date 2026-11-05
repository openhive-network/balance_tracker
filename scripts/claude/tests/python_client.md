# Python API Client Tests

## Overview

The Python client of the REST API (distribution `hiveio-balance-api`, import package `hiveio_balance_api`) is
generated from the OpenAPI specification in `endpoints/endpoint_schema.sql`. This repository holds no Poetry project or
lockfile for it: generation, the wheel, its tests and publishing come from the `python_api_package` CI template of
hive/wax-spec-generator (`templates/python_api_package.gitlab-ci.yml`), included by `.gitlab-ci.yml`. The template
pins the generator and the runtime package (`hiveio_api_client`) the wheel depends on; the wheel does not depend on
beekeeper libraries, so their releases do not require changes here.

## CI Jobs

| Job | Template | What it does |
|-----|----------|--------------|
| `generate_python_api_client` | `.python_api_package_generate` | Generates `python-api-package/` (client + `pyproject.toml`) from `build/swagger-doc.json` of `extract-swagger-json` |
| `build_python_api_client_wheel` | `.python_api_package_build` | Builds the wheel; the version comes from this repo's tags (poetry-dynamic-versioning) |
| `test_python_api_wheel` | `.python_api_package_test` | Installs the wheel, imports every client module, checks the version is not `0.0.0` and no forbidden dependency is pulled |
| `python_api_client_test` | `.python_api_package_base` | Installs the wheel and runs `tests/python_api_client/call_endpoint.py` against the public API nodes |
| `deploy_python_api_packages_to_gitlab` | `.python_api_package_publish` | Uploads the wheel to this project's GitLab PyPI registry |

The template's inputs are the global variables `PYTHON_API_SWAGGER_JSON`, `PYTHON_API_DISTRIBUTION_NAME`,
`PYTHON_API_PACKAGE_NAME` and `PYTHON_API_NAMESPACE` (`balance_api_client`, the subpackage holding the client).

## Versions

The version scheme is the one the package always had: the tag on a tag checkout, tag + `.post{timestamp}` on
develop/master/main, tag + `.dev{timestamp}+unstable` on other branches - so published versions keep increasing.
`python-api-package/` is in `.gitignore`; an untracked generated tree would mark the version `+dirty`.

## Live Endpoint Check

`tests/python_api_client/call_endpoint.py` calls `accounts_delegations` through
`hiveio_api_client.AsyncRemoteHandle` and checks the response is parsed into the generated model. Without arguments it
tries `https://api.syncad.com`, then `https://api.hive.blog`; pass endpoint URLs to call another instance:

```bash
python3 tests/python_api_client/call_endpoint.py http://localhost:3000
```

Usage of the client:

```python
from hiveio_api_client import AsyncRemoteHandle, HttpUrl, RemoteHandleSettings
from hiveio_balance_api.balance_api_client.balance_api_client import BalanceApi

handle = AsyncRemoteHandle(RemoteHandleSettings(http_endpoint=HttpUrl("https://api.syncad.com")), api=BalanceApi)
delegations = await handle.api.accounts_delegations("gtg")
```

## Common Failure Causes

| Error | Likely Cause | Fix |
|-------|--------------|-----|
| `No endpoint answered` | Public API nodes down | Retry; the job retries twice |
| Import error in `test_python_api_wheel` | Generator cannot handle a schema change | Fix the schema or the generator (hive/wax-spec-generator) |
| Version `0.0.0` | Dynamic versioning did not run (no tags fetched) | The build job needs `GIT_DEPTH: 0` (set by the template) |
| Runtime package not found | No release of `hiveio_api_client` matches the template's pin | Update the template in hive/wax-spec-generator |

## Expansion Rules

When modifying the Python client:
1. Generator, runtime and packaging changes belong in hive/wax-spec-generator's template, not here
2. Keep `call_endpoint.py` independent of local infrastructure by default
3. Update this documentation with new jobs or checks

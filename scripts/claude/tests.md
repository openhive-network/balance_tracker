# Balance Tracker Testing Documentation

## Overview

Balance Tracker uses multiple test types to ensure correctness across different layers: API behavior, performance characteristics, balance calculation accuracy, and installation reliability.

## Quick Reference

```bash
# Run Tavern API tests (mainnet data)
export BTRACKER_ADDRESS=localhost BTRACKER_PORT=3000
cd tests/tavern/patterns-mainnet && pytest

# Run Tavern API tests (mock data)
cd tests/tavern/patterns-mock && pytest

# Run performance tests
./scripts/ci-helpers/run_performance_tests.sh --backend-host=localhost

# Run regression tests
cd tests/regression && ./run_test.sh --host=localhost

# Run functional tests
cd tests/functional && ./test_scripts.sh --host=localhost
```

## Test Types

| Type | Documentation | Purpose | Framework |
|------|---------------|---------|-----------|
| Tavern API | [tests/tavern.md](tests/tavern.md) | REST endpoint validation | Tavern/pytest |
| Performance | [tests/performance.md](tests/performance.md) | Load testing API | JMeter |
| Regression | [tests/regression.md](tests/regression.md) | Balance accuracy verification | Python/SQL |
| Functional | [tests/functional.md](tests/functional.md) | Script and installation testing | Shell |
| Python API Client | [tests/python_client.md](tests/python_client.md) | Generated client validation | pytest |

## Test Directory Structure

```
tests/
├── tavern/                    # API integration tests
│   ├── common.yaml            # Shared configuration
│   ├── pytest.ini             # Pytest markers
│   ├── patterns-mainnet/      # Tests against synced blockchain
│   └── patterns-mock/         # Tests against mock fixtures
├── performance/               # JMeter load tests
│   └── test_scenarios.jmx     # Test scenario definitions
├── regression/                # Balance verification tests
│   ├── run_test.sh            # Main test runner
│   ├── accounts_dump.json.gz  # Expected balance fixture
│   └── sql/                   # Comparison SQL
├── functional/                # Script validation tests
│   └── test_scripts.sh        # Installation script tests
├── mocks/                     # Mock data infrastructure
│   ├── install_mock_data.sh   # Mock data installer
│   ├── fixtures/              # Mock operation data
│   └── sql/                   # Mock insertion functions
└── tests_api/                 # Shared test utilities
```

## CI Pipeline Integration

Tests run automatically in GitLab CI. Key jobs in `.gitlab-ci.yml`:

| CI Job | Test Type | Data Source |
|--------|-----------|-------------|
| `pattern-test` | Tavern API | Synced mainnet (5M blocks) |
| `pattern-test-with-mock-data` | Tavern API | Mock fixtures |
| `performance-test` | JMeter | Synced mainnet |
| `regression-test` | Balance comparison | hived snapshot |
| `setup-scripts-test` | Functional | Fresh install |
| `python_api_client_test` | Python client | External API |

### Pipeline Stages

```
detect → lint → build → sync → test → publish
                         ↓
                   Tests run here
```

Tests depend on the `sync` stage which:
1. Prepares HAF data cache
2. Syncs Balance Tracker to 5M blocks
3. Optionally loads mock data for mock tests

### Cache Handling

CI uses NFS caching to avoid re-syncing for every pipeline:
- `haf_btracker_sync` - HAF + Balance Tracker synced state
- `haf_btracker_mock` - Above + mock data loaded

## Running Tests Locally

### Prerequisites

1. **Database**: HAF instance with Balance Tracker installed
2. **API**: PostgREST running on port 3000
3. **Dependencies**: See individual test documentation for framework requirements

### Docker Environment

```bash
cd docker
docker compose up -d

# Wait for sync to complete
docker compose logs -f backend-block-processing

# Run tests against dockerized services
export BTRACKER_ADDRESS=localhost BTRACKER_PORT=3000
cd ../tests/tavern/patterns-mainnet && pytest
```

### With Mock Data

```bash
# Load mock data (after normal sync)
./tests/mocks/install_mock_data.sh --host=localhost

# Run mock tests
cd tests/tavern/patterns-mock && pytest
```

## Writing New Tests

See individual test type documentation for detailed guides:

- **API endpoint tests**: Add `.tavern.yaml` files in `tests/tavern/`
- **Performance scenarios**: Edit `tests/performance/test_scenarios.jmx`
- **Balance verification**: Update regression fixtures
- **Script tests**: Extend `tests/functional/test_scripts.sh`

## Expansion Rules

When modifying test infrastructure:
- **New test category**: Create `scripts/claude/tests/<type>.md`, add to this index
- **New Tavern endpoint tests**: Update [tests/tavern.md](tests/tavern.md)
- **New CI test job**: Document in the appropriate test type file
- **Mock data changes**: Update [tests/tavern.md](tests/tavern.md) mock data section

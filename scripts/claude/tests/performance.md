# Performance Tests

## Overview

JMeter-based load tests that measure API response times under concurrent load. Tests cover key endpoints including balance history queries, which are the most resource-intensive operations.

## Running These Tests

```bash
# Install JMeter (if not installed)
sudo apt-get install -y openjdk-11-jdk-headless
wget https://downloads.apache.org/jmeter/binaries/apache-jmeter-5.6.3.zip -O jmeter.zip
unzip jmeter.zip && sudo mv apache-jmeter-5.6.3 /usr/local/src/
sudo ln -sf /usr/local/src/apache-jmeter-5.6.3/bin/jmeter.sh /usr/local/bin/jmeter
rm jmeter.zip

# Run tests (API must be running)
./scripts/ci-helpers/run_performance_tests.sh

# Run with custom settings
./scripts/ci-helpers/run_performance_tests.sh \
    --backend-host=localhost \
    --backend-port=3000 \
    --test-thread-count=16 \
    --test-loop-count=100

# View HTML report
python3 -m http.server --directory tests/performance/result_report 8000
# Open http://localhost:8000
```

## Test File Organization

```
tests/performance/
├── test_scenarios.jmx       # JMeter test plan
├── result.jtl               # Raw test results (generated)
├── result.xml               # Summary report (generated)
├── result_report/           # HTML report directory (generated)
└── results.tar.gz           # CI artifact archive (generated)
```

## Key Test Scenarios

The `test_scenarios.jmx` file defines these test groups:

| Thread Group | Endpoint | Purpose |
|--------------|----------|---------|
| `get_balance_history by_block` | `/rpc/get_balance_history` | History pagination performance |
| Balance queries | `/rpc/get_account_balances` | Snapshot query performance |

### Test Configuration

Default settings in `test_scenarios.jmx`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `THREAD_COUNT` | 8 | Concurrent users |
| `LOOP_COUNT` | 60 | Iterations per thread |
| `backend.host` | localhost | API hostname |
| `backend.port` | 3000 | API port |

## How to Add New Performance Tests

### 1. Open JMeter GUI

```bash
jmeter
```

### 2. Load Test Plan

Open `tests/performance/test_scenarios.jmx`

### 3. Add Thread Group

1. Right-click Test Plan > Add > Threads > Thread Group
2. Configure thread count and loop count using variables:
   - Threads: `${THREAD_COUNT}`
   - Loops: `${LOOP_COUNT}`

### 4. Add HTTP Sampler

1. Right-click Thread Group > Add > Sampler > HTTP Request
2. Configure:
   - Protocol: (leave empty, uses default)
   - Server: `${__P(backend.host,localhost)}`
   - Port: `${__P(backend.port,3000)}`
   - Method: POST
   - Path: `rpc/endpoint_name`

### 5. Add Request Parameters

For POST requests, add parameters via the GUI or JSON body.

### 6. Save and Run

```bash
# Run non-GUI mode
jmeter --nongui --testfile tests/performance/test_scenarios.jmx \
    --logfile tests/performance/result.jtl \
    --reportatendofloadtests --reportoutputfolder tests/performance/result_report
```

## Performance Test Script

The main runner script: `scripts/ci-helpers/run_performance_tests.sh`

### Options

```
--test-report-dir=PATH       HTML report output directory
--test-result-path=PATH      JTL result file path
--test-thread-count=NUMBER   Concurrent threads (default: 8)
--test-loop-count=NUMBER     Iterations per thread (default: 60)
--backend-host=HOSTNAME      API hostname (default: localhost)
--backend-port=PORT          API port (default: 3000)
--postgres-host=HOSTNAME     Database hostname (default: localhost)
--postgres-port=PORT         Database port (default: 5432)
```

### Pre-Test Index Creation

The script automatically creates performance indexes before running tests:

```sql
SELECT btracker_app.create_btracker_indexes();
```

This ensures queries have optimal index support during load testing.

## Common Failure Causes

| Error | Likely Cause | Fix |
|-------|--------------|-----|
| Connection refused | API not running | Start PostgREST |
| All requests timeout | Database overloaded | Reduce thread count, check indexes |
| High error rate | Query errors | Check PostgreSQL logs |
| OOM during test | Too many threads | Reduce `THREAD_COUNT` |
| jmeter not found | JMeter not installed | Run installation commands above |

## Interpreting Results

### Key Metrics

| Metric | Description | Target |
|--------|-------------|--------|
| Avg Response Time | Mean response time in ms | < 500ms |
| 90th Percentile | 90% of requests complete within | < 1000ms |
| Throughput | Requests per second | > 10 rps |
| Error Rate | Percentage of failed requests | < 1% |

### HTML Report Sections

- **Dashboard**: High-level summary with charts
- **Statistics**: Per-sampler breakdown
- **Response Times**: Distribution histograms
- **Errors**: Failed request details

## CI Integration

The `performance-test` job in `.gitlab-ci.yml`:

```yaml
performance-test:
  extends: .test-with-docker-compose
  script:
    - timeout -k 1m 10m ./scripts/ci-helpers/run_performance_tests.sh \
        --backend-host=docker --postgres-host=docker
    - tar -czvf tests/performance/results.tar.gz $(pwd)/tests/performance/*result.*
    - cat jmeter.log | python3 docker/ci/parse-jmeter-output.py
    - m2u --input result.xml --output junit-result.xml
  artifacts:
    paths:
      - tests/performance/result_report/
      - tests/performance/results.tar.gz
    reports:
      junit: tests/performance/junit-result.xml
```

### CI Timeouts

- Test timeout: 10 minutes
- Job timeout: 30 minutes

## Expansion Rules

When modifying performance tests:
1. Add new thread groups to `test_scenarios.jmx`
2. Test locally before committing
3. Update this documentation with new scenarios
4. Consider CI timeout implications for long-running tests

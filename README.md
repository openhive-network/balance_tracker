# Balance tracker

1. [Intro](#intro)
1. [Starting](#starting)
    1. [Requirements](#requirements)
    1. [Cloning](#cloning)
    1. [Setup with PostgREST backend](#setup-with-postgrest-backend)
    1. [After Setup](#after-setup)
    1. [Dockerized setup](#run-with-docker)
1. [Architecture](#architecture)
1. [Tests](#tests)

## Intro

Balance Tracker app is used to track HBD and Hive balances for Hive accounts. This app was created as a simple example of how to create HAF apps.
Balance Tracker uses PostgREST to expose the database API as a REST service.

## Starting

Tested on Ubuntu 20.04 and 22.04

### Requirements

1. HAF instance

### Cloning

To clone the repository run the following command

```bash
git clone https://gitlab.syncad.com/hive/balance_tracker.git # or git@gitlab.syncad.com:hive/balance_tracker.git
cd balance_tracker
git submodule update --init --recursive
```

#### Note on HAF submodule

HAF submodule is not necessary to build or run the balance_tracker.

If you already have an instance of HAF running or are planning to set it up separately (for example using a prebuilt Docker image), you can safely disable it by running:

```bash
git config --local submodule.haf.update none
```

before checking out the submodules (that is running `git submodule update --init --recursive`).

The submodule is, however, necessary, for test run in CI. As such when updating it please update the commit hash in files [.gitlab-ci.yml](.gitlab-ci.yml) and [docker/.env](docker/.env).

### Setup with PostgREST backend

1. Install backend runtime dependencies:
   ```bash
   sudo apt-get update && sudo apt-get -y install apache2-utils curl postgresql-client wget xz-utils
   ```
1. Install PostgREST (see [PostgREST releases](https://github.com/PostgREST/postgrest/releases) for latest version):
   ```bash
   wget https://github.com/PostgREST/postgrest/releases/download/v11.1.0/postgrest-v11.1.0-linux-static-x64.tar.xz -O postgrest.tar.xz
   tar -xJf postgrest.tar.xz && sudo mv postgrest /usr/local/bin/ && rm postgrest.tar.xz
   ```
1. Set up the database: `./scripts/install_app.sh` (assuming HAF database runs on localhost on port 5432)
1. Process blocks: `./scripts/process_blocks.sh --blocks=5000000` (assuming HAF database runs on localhost on port 5432 and contains 5'000'000 blocks)
   If you skip `--blocks` parameter balance tracker will process all blocks in the database and then wait for more. You can keep this running in the background.
1. Start backend server:
   ```bash
   PGRST_DB_URI="postgres://btracker_user@localhost/haf_block_log" \
   PGRST_DB_SCHEMA="btracker_endpoints" \
   PGRST_DB_ANON_ROLE="btracker_user" \
   PGRST_DB_EXTRA_SEARCH_PATH="btracker_app" \
   postgrest
   ```

### After Setup

- After blocks were added to *btracker_app* schema, PostgREST backend server can be started up quickly with the command above.
- If you have added more blocks to `haf_block_log` db, or you want to run live sync, you can run:

```bash
./scripts/process_blocks.sh --blocks=0
```

This will keep indexer waiting for new blocks and process them while the backend is running.

### Run with Docker

Running *Balance tracker* with Docker is described in Docker-specific [README](docker/README.md).

## Architecture

`db/` - contains SQL backend code (table definitions, block processing functions)

`backend/` - contains SQL backend helper functions

`endpoints/` - contains endpoint SQL code exposed via PostgREST

`scripts/` - contains shell scripts for installation, block processing, and CI helpers

`tests/regression/` - regression test infrastructure for comparing against hived snapshots

## Tests

### Performance Tests

JMeter performance tests are available for the PostgREST backend.

1. Start PostgREST backend server on port 3000
1. Install test dependencies:
   ```bash
   sudo apt-get update && sudo apt-get -y install openjdk-8-jdk-headless python3 unzip
   ```
1. Install JMeter (version 5.6.2 or later):
   ```bash
   wget https://downloads.apache.org/jmeter/binaries/apache-jmeter-5.6.2.zip -O jmeter.zip
   unzip jmeter.zip && sudo mv apache-jmeter-5.6.2 /usr/local/src/
   sudo ln -sf /usr/local/src/apache-jmeter-5.6.2/bin/jmeter.sh /usr/local/bin/jmeter
   ```
1. Run tests: `./scripts/ci-helpers/run_performance_tests.sh`

**Note:** If you already have jmeter installed and on `$PATH` you can skip straight to the last point.

If you want to serve the HTML report on port 8000:
```bash
python3 -m http.server --directory tests/performance/result_report 8000
```
You can also simply open *tests/performance/result_report/index.html* in your browser.

### Regression Tests

Regression tests compare Balance Tracker's computed values against expected values from a hived node snapshot.

To run the regression test:
```bash
cd tests/regression
./run_test.sh --host=localhost --port=5432
```

The test installs a temporary `btracker_test` schema, loads expected values, and reports any discrepancies.

# Balance tracker

1. [Intro](#intro)
1. [Starting](#starting)
    1. [Requirements](#requirements)
    1. [Cloning](#cloning)
    1. [Setup with postgREST backend](#setup-with-postgrest-backend)
    1. [Setup with python backend](#setup-with-python-backend)
    1. [After Setup](#after-setup)
    1. [Dockerized setup](#run-with-docker)
1. [Architecture](#architecture)
    1. [Python version](#python-version)
    1. [PostgREST version](#postgrest-version)
1. [Tests](#tests)
    1. [PostgREST backend](#postgrest-backend)
    1. [Python backend](#postgrest-backend)

## Intro

Balance Tracker app is used to graph HBD and Hive balances for Hive account by increment of block or time. This app was created as a simple example of how to create  HAF apps.
Balance Tracker uses JavaScript's REACT for web app style UI, python's BaseHTTPRequestHandler for forking HTTP server and postgREST server. There are two versions of backend that can be run for the same app but not in parallel.

## Starting

Tested on Ubuntu 20.04 and 22.04

### Requirements

1. HAF instance
1. Node version higher or equal 14.0.0. Check version `node-v` or `node --version`.

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

The submodule is, however, necessary, for test run in CI. As such when updating it please update the commit hash in files [.gitlab-ci.yml](.gitlab-ci.yml) and [doocker/.env](docker/.env).

### Setup with PostgREST backend

1. Install backend runtime dependencies: `./balance-tracker.sh install backend-runtime-dependencies`
1. Install PostgREST: `./balance-tracker.sh install postgrest`
1. Set up the database: `./balance-tracker.sh set-up-database` (assuming HAF database runs on localhost on port 5432)
1. Process blocks: `./balance-tracker.sh process-blocks --number-of-blocks=5000000` (assuming HAF database runs on localhost on port 5432 and contains 5'000'000 blocks)  
  If you skip `--number-of-blocks` parameter balance tracker will process all blocks in the database and then wait for more. You can keep this running in the background.
1. Start backend server: `./balance-tracker.sh serve postgrest-backend`
1. Install frontend runtime dependencies: `./balance-tracker.sh install frontend-runtime-dependencies`
1. Set up `$PATH`: `export VOLTA_HOME="$HOME/.volta"; export PATH="$VOLTA_HOME/bin:$PATH"` - or you can simply reopen the terminal
1. Start frontend server: `./balance-tracker.sh serve frontend`

#### Building the frontend

Command `./balance-tracker.sh serve frontend` will build and serve the frontend in development mode. It can be used in production, but it is recommended to build and deploy the app in production mode.

To do so, follow the instructions below:

1. Set the public URL of the frontend in the [gui/.env](gui/.env) file.
1. Adjust the values of the other environment variables in the same file.
1. Build the frontend in production mode: `./balance-tracker.sh build frontend`
1. Deploy the built frontend (located in [gui/build](gui/build) directory) using one of the methods described in the [official React documentation](https://create-react-app.dev/docs/deployment/).

**Note**: Do not set the *homepage* field in [gui/package.json](gui/package.json) - it's currently [buggy](https://github.com/facebook/create-react-app/issues/8813).

### Setup with Python backend

Note: python backend is not configured to be used with web UI!

1. Create btracker_app schema, process blocks. `./run.sh re-db ${n_blocks}`.  
`n_blocks` is the number of blocks you have in `haf_block_log` db.
1. Start the server `python3 main.py --host=localhost --port=3000`.
1. Call server with `curl` GET requests as in examples:

```bash
curl "http://localhost:3000/rpc/find_matching_accounts/?_partial_account_name=d"

curl "http://localhost:3000/rpc/get_balance_for_coin_by_block/?_account_name=dantheman&_coin_type=21&_start_block=0&_end_block=10000"

curl "http://localhost:3000/rpc/get_balance_for_coin_by_time/?_account_name=dantheman&_coin_type=21&_start_time=2016-03-24%2016:05:00&_end_time=2016-03-25%2000:34:48"
```

### After Setup

- After blocks were added to *btracker_app* schema, PostgREST backend server can be started up quickly with `./balance-tracker.sh serve postgrest-backend`.
- If you have added more blocks to `haf_block_log` db, or you want to run live sync, you can run:

```bash
./balance-tracker.sh process-blocks --number-of-blocks=0
```

This will keep indexer waiting for new blocks and process them while the backend is running.

### Run with Docker

Running *Balance tracker* with Docker is described in Docker-specific [README](docker/README.md).

## Architecture

`db/btracker_app.sql` contains code for processing raw block data into separate db, to be used by app.

`db/builtin_roles.sql` contains database role setup.

`py_config.ini` and `postgrest.conf` are configs for Python and PostgREST versions of backend. They contain parameters for PostgreSQL access.

`balance-tracker.sh` can be used to start up PostgREST backend versions and UI, or run tests. Execute `./balance-tracker.sh help` for detailed list of parameters.

`run.sh` is a legacy script for starting both backend versions, UI and running tests.

### Python version

- `server/serve.py` - Listens for requests, parses parameters, sends requests to postgres, returns response.
- `server/adapter.py` - Used to connect python backend with postgres server.
- `db/backend.py` - Contains queries for psql, sent through `adapter.py`
- `main.py` - parses arguments and starts server

### PostgREST version

- `api/btracker_api.sql` - Contains the same queries as `db/backend.py`

## Tests

JMeter performance tests are configured for different backends. To run tests:

### PostgREST backend

1. Start PostgREST backend server on port 3000
1. Install test dependencies: `./balance-tracker.sh install test-dependencies`
1. Install jmeter: `./balance-tracker.sh install jmeter`
1. Run tests: `./balance-tracker.sh run-tests`

**Note:** If you already have jmeter installed and on `$PATH` you can skip straight to the last point.

If you want to serve the HTTP report on port 8000 run `./balance-tracker.sh serve test-results`.  
You can also simply open *tests/performance/result_report/index.html* in your browser.

### Python backend

1. Start Python backend server on port 3000
1. Install JMeter `./run.sh install-jmeter`
1. Run tests for python `./run.sh test-py`

Result report dashboard (HTML) will be generated at `tests/performance/result_report/index.html`

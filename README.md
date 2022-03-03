# Intro

Balance Tracker app is used to graph HBD and Hive balances for Hive account by increment of block or time. This app was created as a simple example of how to create  HAF apps.
Balance Tracker uses JavaScript's REACT for web app style UI, python's BaseHTTPRequestHandler for forking HTTP server and postgREST server. There are two versions of backend that can be run for the same app but not in parallel.


# Starting

Tested on Ubuntu 20.04

## Requirements

1. Built haf as submodule of [HAfAH](https://gitlab.syncad.com/hive/HAfAH/-/tree/develop)
2. Node version higher or equal 14.0.0. Check version `node-v` or `node --version`.

## Setup with postgREST backend

1. Install postgREST to /usr/local/bin `./run.sh install-postgrest`
2. Create btracker_app schema, process blocks, create api, start server. `./run.sh re-all-start ${n_blocks}`.<br>
`n_blocks` is the number of blocks you have in `haf_block_log` db.
3. Install node_modules . `cd gui ; npm install ; cd ..`
4. Start application . `./run.sh start-ui`

## Setup with python backend

Note: python backend is not configured to be used with web UI!

1. Create btracker_app schema, process blocks. `./run.sh re-db ${n_blocks}`.<br>
`n_blocks` is the number of blocks you have in `haf_block_log` db.
2. Start the server `python3 main.py --host=localhost --port=3000`.
3. Call server with `curl` GET requests as in examples:

```
curl http://localhost:3000/rpc/find_matching_accounts/?_partial_account_name=d
```
```
curl http://localhost:3000/rpc/get_balance_for_coin_by_block/?_account_name=dantheman&_coin_type=21&_start_block=0&_end_block=10000
```
```
curl http://localhost:3000/rpc/get_balance_for_coin_by_time/?_account_name=dantheman&_coin_type=21&_start_time=2016-03-24%2016:05:00&_end_time=2016-03-25%2000:34:48
```

## After Setup

- After blocks were added to btracker_app schema, postgREST backend server can be started up quickly with `./run.sh start`
- If you have added more blocks to `haf_block_log` db, or you are running live sync, you can run <br>
```
psql -v "ON_ERROR_STOP=1" -d haf_block_log -c "call btracker_app.main('btracker_app', 0);"
```
This will keep indexer waiting for new blocks and process them as backend is running.

# Architecture

`db/btracker_app.sql` contains code for processing raw block data into separate db, to be used by app.

`py_config.ini` and `postgrest.conf` are configs for python and postgREST versions of backend. They contain parameters for postgres access.

`run.sh` can be used to start up both backend versions and ui, or run tests.

## Python version

- `server/serve.py` - Listens for requests, parses parameters, sends requests to postgres, returns response.
- `server/adapter.py` - Used to connect python backend with postgres server.
- `db/backend.py` - Contains queries for psql, sent through `adapter.py`
- `main.py` - parses arguments and starts server

## PostgREST version

- `api/btracker_api.sql` - Contains the same queries as `db/backend.py`

# Tests

JMeter performance tests are configured for different backends. To run tests:
1. Start one of backend servers on port 3000
2. Install JMeter `./run.sh install-postgres`
3. Run tests for python `./run.sh test-py` or postgREST `./run.sh test`

Result report dashboard (Apache) will be generated at `tests/performance/result_report/index.html`

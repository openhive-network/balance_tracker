# Intro

Balance_tracker app was mainly created as a simple example of how to make a HAF app. This app can be used to graph the HBD and Hive balances of any Hive account as those balances changes each block.

# Start UI

## Requirements

1. Tested on Ubuntu 20.04
2. Node version higher or equal 14.0.0. Check version `node-v` or `node --version`

## Local Setup

1. Install postgREST to /usr/local/bin `./run.sh install-postgrest`
2. Create btracker_app schema & api and will start web server with 5M blocks . `./run.sh re-all-start 5000000`
3. Open another tab in terminal, change directory to gui . `cd gui`
4. Install node_modules . `npm install`
5. Start application . `npm start`

## After Setup

1. After blocks were added to btracker_app schema, web server can be started up quickly with `./run.sh start`

# Task

Current status:

At the moment, this application will need to use two tables:

- hive.accounts (created by hived sql_serializer plugin)
- btracker_app.account_balance_history (created by sync process of balance tracker)

Work to be done:

- Add jsonrpc API calls required by new web app:
  `find_matching_accounts(partial_account_name)` returns a list of all accounts that match against partial_account name. May want to limit max amount of names returned.
  `get_balance_for_coin(account_name, coin_type, starting_block, count`) return the balance values for account_name for the required coin_type starting with starting_block and returning count number of balances.

- Create web UI that has an edit box where user can type name of account to graph. After some number of characters have been typed, dropdown should appear below edit box with possible account names that match partially typed name (dropdown filled using find_matching_accounts). Pressing enter or button beside the edit box (called "show balances") with a valid account name should call get_balance_for_coin twice (with hbd and with hive passed). These two tuples of (block_number, balance) should then be graphed for each coin.

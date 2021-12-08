///////////////////////////////////////////////////////////

HOW TO START UI :

1. Check node version in terminal, node version must be > 14.0.
   In terminal write : node -v or node --version
2. Run server.
   In terminal write : ./run.sh re-all-start 10000 <===// this number is block count, if you don't write any number, function will run all blocks.
3. Open another terminal window and change directory to /gui.
   In terminal write : cd gui
4. Install node_modules package:
   In terminal write : npm install
5. Start ui :
   In terminal write : npm start

   Note : building still in progress.

///////////////////////////////////////////////////////////

Balance_tracker app was mainly created as a simple example of how to make a HAF app. This app can be used to graph the HBD and Hive balances of any Hive account as those balances changes each block.

Current status:

At the moment, this application will need to use two tables:

- hive.accounts (created by hived sql_serializer plugin)
- btracker_app.account_balance_history (created by sync process of balance tracker)

Work to be done:

- Add jsonrpc API calls required by new web app:
  `find_matching_accounts(partial_account_name)` returns a list of all accounts that match against partial_account name. May want to limit max amount of names returned.
  `get_balance_for_coin(account_name, coin_type, starting_block, count`) return the balance values for account_name for the required coin_type starting with starting_block and returning count number of balances.

- Create web UI that has an edit box where user can type name of account to graph. After some number of characters have been typed, dropdown should appear below edit box with possible account names that match partially typed name (dropdown filled using find_matching_accounts). Pressing enter or button beside the edit box (called "show balances") with a valid account name should call get_balance_for_coin twice (with hbd and with hive passed). These two tuples of (block_number, balance) should then be graphed for each coin.

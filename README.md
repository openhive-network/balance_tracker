Simple python server for balance tracker to compare performance with postgREST

First, add 5M blocks to `btracker_app` schema by checking out to develop branch and running 
```
./run.sh re-start-all 5000000
```

Then stop the server and checkout this branch (`4-create-python-backend-2`)

To startup the server password to `api_user` must be added
```
psql -d haf_block_log -c "ALTER USER api_user WITH PASSWORD 'api_user';"
psql -d haf_block_log -c "ALTER USER api_user WITH LOGIN;"
```

If you are logging in with other user/password, change `config.ini`

Start the server with:
```
python3 main.py
```

Default address is localhost:8080 which can be changed by providing arguments `--port`, `--host`

Example requests:

```
curl http://localhost:8080/rpc/find_matching_accounts/?_partial_account_name=d
```
```
curl http://localhost:8080/rpc/get_balance_for_coin_by_block/?_account_name=dantheman&_coin_type=21&_start_block=0&_end_block=10000
```
```
curl http://localhost:8080/rpc/get_balance_for_coin_by_time/?_account_name=dantheman&_coin_type=21&_start_time=2016-03-24%2016:05:00&_end_time=2016-03-25%2000:34:48
```

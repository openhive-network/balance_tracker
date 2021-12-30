Simple python server for balance tracker to compare performance with postgREST

Example requests:

```
curl http://localhost:8096/rpc/find_matching_accounts/?_partial_account_name=d
```
```
http://localhost:8081/rpc/get_balance_for_coin_by_block/?_account_name=dantheman&_coin_type=21&_start_block=0&_end_block=10000
```
```
http://localhost:8097/rpc/get_balance_for_coin_by_time/?_account_name=dantheman&_coin_type=21&_start_time=2016-03-24%2016:05:00&_end_time=2016-03-25%2000:34:48
```

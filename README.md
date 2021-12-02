This branch contains example balance_tracker api response data in json format.

Example how GUI would fetch data (share your ideas if you see what can be changed):

When searching for account `dantheman` letters are typed into edit box one by one and `partial_account_name` parameter is formed (`d`, `da`, `dan`, `dant`, etc).

With every new letter a request is sent to `find_matching_accounts()` and it responds with a json array of account names. Query is limited to 2000. The account names are selected by rule: *if `account_name` starts with `partial_account_name`*

Names would then be displayed as a list bellow edit box and GUI user would choose the one by clicking on it. Just like on google search bar.

Example request with curl

```
curl -X POST http://localhost:3000/rpc/find_matching_accounts \
	-H 'Content-Type: application/json' \
	-H 'Prefer: params=single-object' \
	-d '{ "partial_account_name": "dantheman" }' 
```

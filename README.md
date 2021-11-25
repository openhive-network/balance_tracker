This branch contains example balance_tracker api response data in json format.
Examples of `POST /find_matching_accounts?fts.<search_string>` are in `find_matching_accounts*.json`
Examples of `POST /get_balance_for_coin_by_block?account=<account_name>` are in `get_balance_for_coin_by_block*.json`
To generate examples two `<search_string>`'s were used: `dan` and `lukas` for `find_matching_accounts`. 
The `<search_string>` would be typed by balance_tracker user. Then a single `<account_name>` would be chosen from given list and balance history would be fetched for desired currency and account to plot graphs. Currently, in examples, currencies are named as their nai (13, 21, 37)

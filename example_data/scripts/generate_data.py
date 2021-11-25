import os
import json
import requests


def find_matching_accounts(search_str):
    url = "http://localhost:3000/current_account_balances?account=fts.%s" % search_str

    res = requests.get(url, headers=headers)
    res_data = json.loads(res.content.decode())
    account_names = list(set([entry["account"] for entry in res_data]))

    out_f_name = "find_matching_accounts_search-%s.json" %search_str
    with open(os.path.join(data_dir, out_f_name), "w") as f:
        json.dump(account_names, f, indent=2)


def get_balance_for_coin_by_block(account_name):
    url = "http://localhost:3000/account_balance_history?account=like.%s" % account_name

    res = requests.get(url, headers=headers)
    res_data = json.loads(res.content.decode())
    for i in range(len(res_data)):
        del res_data[i]["hive_rowid"]
        del res_data[i]["account"]
        del res_data[i]["source_op"]

    nais = [21, 37, 13]
    added_idxs = []
    for nai in nais:
        nai_data = []
        for i in range(len(res_data)):
            if i not in added_idxs and res_data[i]["nai"] == nai:
                del res_data[i]["nai"]
                nai_data.append(res_data[i])
                added_idxs.append(i)

        out_f_name = "get_balance_for_coin_by_block_account-%s_currency-%d.json" % (
            account_name, nai)
        with open(os.path.join(data_dir, out_f_name), "w") as f:
            json.dump(nai_data, f, indent=4)

        #print("~~~~~~~~~~~~ %d ~~~~~~~~~~~~" % nai)
        # for i in range(len(nai_data)):
        #    print(nai_data[i])


# TODO: get_balance_for_coin_by_time

if __name__ == "__main__":
    f_path = os.path.realpath(__file__)
    data_dir = os.sep.join(f_path.split(os.sep)[:-2])

    headers = {'content-type': 'application/json'}

    search_str = "lukas"
    find_matching_accounts(search_str)
    get_balance_for_coin_by_block(search_str)

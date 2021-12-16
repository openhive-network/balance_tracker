import os
import requests
import json
from time import time
from tqdm import tqdm
import numpy as np
import matplotlib.pyplot as plt


def draw_graph(n, durations, res_lens, method_name):
    f, ax = plt.subplots(2, 1)
    f.set_size_inches(15, 10)
    f.subplots_adjust(top=1.5)

    ax[0].bar(list(durations.keys()), [np.mean(v)
              for v in list(durations.values())])
    ax[1].bar(list(res_lens.keys()), list(res_lens.values()))

    ax[0].set_title("Number of iterations: %d" % n)

    ax[0].set_xlabel("Parameters")
    ax[0].set_ylabel("Duration, ms")
    ax[1].set_xlabel("Parameters")
    ax[1].set_ylabel("Number of entries returned")

    direc = os.path.join("plots", method_name)
    if os.path.isdir(direc) is False:
        os.mkdir(direc)
    name = "%d.png" % (len(os.listdir(direc)) + 1)
    plt.savefig(os.path.join(direc, name), bbox_inches='tight')


def test_find_matching_accounts(n, name, method_name):
    name_list = [name[:i+1] for i in range(len(name))]
    durations = {name: [] for name in name_list}
    res_lens = {}
    for i in tqdm(range(n), disable=False):
        for name in name_list:
            data = {"_partial_account_name": name}
            duration, res_len = post(data, method_name)
            durations[name].append(duration)
            res_lens[name] = res_len
    return durations, res_lens


def generate_block_key(array_data, i):
    return "%s\n%s\n%s\n%s\n%s" % (
        array_data["_account_name"][i],
        array_data["_coin_type"][i],
        array_data["_start_block"][i],
        array_data["_end_block"][i],
        array_data["_block_increment"][i]
    )


def generate_block_data(array_data, i):
    return {"_account_name": array_data["_account_name"][i],
            "_coin_type": array_data["_coin_type"][i],
            "_start_block": array_data["_start_block"][i],
            "_end_block": array_data["_end_block"][i],
            "_block_increment": array_data["_block_increment"][i]}


def test_get_balance_for_coin_by_block(n, array_data, method_name):
    n_sub_tests = len(array_data["_account_name"])
    durations = {generate_block_key(array_data, i): []
                 for i in range(n_sub_tests)}
    res_lens = {}
    for i in tqdm(range(n), disable=False):
        for j in range(n_sub_tests):
            key = generate_block_key(array_data, j)
            data = generate_block_data(array_data, j)
            duration, res_len = post(data, method_name)
            durations[key].append(duration)
            res_lens[key] = res_len
    return durations, res_lens


def generate_time_key(array_data, i):
    return "%s\n%s\n%s\n%s\n%s" % (
        array_data["_account_name"][i],
        array_data["_coin_type"][i],
        array_data["_start_time"][i],
        array_data["_end_time"][i],
        array_data["_time_increment"][i]
    )


def generate_time_data(array_data, i):
    return {"_account_name": array_data["_account_name"][i],
            "_coin_type": array_data["_coin_type"][i],
            "_start_time": array_data["_start_time"][i],
            "_end_time": array_data["_end_time"][i],
            "_time_increment": array_data["_time_increment"][i]}


def test_get_balance_for_coin_by_time(n, array_data, method_name):
    n_sub_tests = len(array_data["_account_name"])
    durations = {generate_time_key(array_data, i): []
                 for i in range(n_sub_tests)}
    res_lens = {}
    for i in tqdm(range(n), disable=False):
        for j in range(n_sub_tests):
            key = generate_time_key(array_data, j)
            data = generate_time_data(array_data, j)
            duration, res_len = post(data, method_name)
            durations[key].append(duration)
            res_lens[key] = res_len
    return durations, res_lens


def post(data, method_name):
    start = time()
    response = requests.post("%s/rpc/%s" % (webserver_url, method_name),
                             json=data, headers={"content-type": "application/json"})
    end = time()
    res_data = response._content.decode("utf-8")
    while type(res_data) not in [dict, list]:
        res_data = json.loads(res_data)
    if type(res_data) is list:
        res_len = len(res_data)
    else:
        res_len = len(res_data["balance"])
    return (end - start) * 1000, res_len


if __name__ == "__main__":
    webserver_url = "http://localhost:3000"
    
    method_name = "find_matching_accounts"
    n = 1000
    name = "dantheman"
    durations, res_lens = test_find_matching_accounts(n, name, method_name)
    draw_graph(n, durations, res_lens, method_name)

    method_name = "get_balance_for_coin_by_block"
    n = 1000
    array_data = {
        "_account_name": ["dantheman", "dantheman", "dantheman", "dantheman", "dantheman", "dantheman"],
        "_coin_type": ["21", "21", "37", "13", "21", "21"],
        "_start_block": ["0", "0", "0", "0", "0", "1250000"],
        "_end_block": ["5000000", "5000000", "5000000", "5000000", "2500000", "3750000"],
        "_block_increment": ["5000", "50000", "5000", "5000", "5000", "5000"]
    }
    durations, res_lens = test_get_balance_for_coin_by_block(
        n, array_data, method_name)
    draw_graph(n, durations, res_lens, method_name)

    '''
    method_name = "get_balance_for_coin_by_time"
    n = 1
    array_data = {
        "_account_name": ["dantheman", "dantheman", "dantheman", "dantheman", "dantheman", "dantheman"],
        "_coin_type": ["21", "21", "37", "13", "21", "21"],
        "_start_time": ["2016-03-24 16:05:00", "2016-03-24 16:05:00", "2016-03-24 16:05:00", "2016-03-24 16:05:00", "2016-03-24 16:05:00", "1250000"],
        "_end_time": ["2016-09-15 19:47:21","2016-09-15 19:47:21","2016-09-15 19:47:21", "2016-09-15 19:47:21", "2016-06-20 10:35:45", "2016-08-03 04:28:18"],
        "_time_increment": ["04:12:13.341", "50000", "04:12:13.341", "04:12:13.341", "04:12:13.341", "04:12:13.341"]
    }
    durations, res_lens = test_get_balance_for_coin_by_time(
        n, array_data, method_name)
    draw_graph(n, durations, res_lens, method_name)
    '''
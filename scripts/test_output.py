# TODO: limit psql 1001 because of start bellow
import os
import json
import numpy as np

def load_response_data(f_name):
    with open(os.path.join(data_dir, f_name), "r") as j:
        data = j.read()
        while type(data) == str:
            data = json.loads(data)
    return data

def get_truth_values(data_truth):
    truth = [[b_n, bal] for b_n, bal in zip(data_truth["block_number"], data_truth["account_balance"]) if b_n > cur_block and b_n <= next_block]
    if len(truth) >= 1: return truth[np.argmax([l[0] for l in truth])]
    return [None, None]

repo_dir = os.sep.join((os.path.realpath(__file__).split(os.sep))[:-2])
data_dir = os.path.join(repo_dir, "data")

start_block = 0
end_block = 10000
block_increment = 100
#comment = "from_table"
comment = "single_query"

f_name_truth = "%d_%d_%d_truth.json" % (start_block, end_block, block_increment)
data_truth = load_response_data(f_name_truth)

f_name_test = "%d_%d_%d_%s.json" % (start_block, end_block, block_increment, comment)
data_test = load_response_data(f_name_test)

first_block = min(data_truth["block_number"])
cur_block = first_block - block_increment
next_block = first_block
for i in range((end_block - start_block) // block_increment + 1):
    truth = get_truth_values(data_truth)
    if truth[0] != None: rep = "<"
    else: rep = ""
    print("Index %d | Start: %d | End: %d | Block_Truth: %s | Balance_Truth: %s | Balance_Test: %s | %s " \
        % (i+1, cur_block, next_block, str(truth[0]), str(truth[1]), data_test[i], rep))
        
    if truth[1] != None and truth[1] != data_test[i]:
        print("missmatch!")
        break

    cur_block += block_increment
    next_block += block_increment
import os
import matplotlib.pyplot as plt
from argparse import ArgumentParser

argument_parser = ArgumentParser()
argument_parser.add_argument('--log_n', type=int, default=0)
args = argument_parser.parse_args()
log_n = args.log_n

durations_path = os.path.join(os.getcwd(), "logs", "durations_log_%d.csv" %log_n)
with open(durations_path, "r") as f:
    data = f.read()[1:].split("\n")

plots_dir = os.path.join(os.getcwd(), "plots")

f, ax = plt.subplots(4, 3)
f.set_size_inches(25, 15)
plt.subplots_adjust(hspace=0.4)
colors = ["g", "b", "r"]
for i, endpoint in enumerate(["find_matching_accounts", "get_balance_for_coin_by_block", "get_balance_for_coin_by_time"]):
    for j, server_func in enumerate([endpoint, "parse_params", "send_reponse", "total"]):
        ax[j, i].plot([float(entry.split(", ")[2]) for entry in data if "%s, %s" %(endpoint, server_func) in entry], c = colors[i])
        ax[j, i].set_title(server_func)
        ax[j, i].grid()

durations_path = os.path.join(plots_dir, "durations_%d.png" %log_n)
plt.savefig(durations_path, bbox_inches='tight')

plt.figure(figsize=(10, 5))
plt.plot([float(entry.split(", ")[1]) for entry in data if "connect" in entry])
connect_durations_path = os.path.join(plots_dir, "connect_durations_%d.png" %log_n)
plt.savefig(connect_durations_path, bbox_inches='tight')
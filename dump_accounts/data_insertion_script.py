import json
import os
import psycopg2
import sys  


if len(sys.argv) != 2:
    print("Usage: python script.py <argument1>")
    sys.exit(1)

script_dir = sys.argv[1]
json_file_path = os.path.join(script_dir, "accounts_dump.json")


host = "172.17.0.2"
port = 5432
database = "haf_block_log"
user = "haf_admin"
password = ""

with open(json_file_path) as file:
    data = json.load(file)

accounts = data["result"]["accounts"]

connection = psycopg2.connect(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password
)

cursor = connection.cursor()


query = "SELECT btracker_account_dump.dump_current_account_stats(%s)"

# Iterate over objects inside 'accounts[]'
for account in accounts:

    object_value = json.dumps(account)

    cursor.execute(query, (object_value,))


connection.commit()
cursor.close()
connection.close()
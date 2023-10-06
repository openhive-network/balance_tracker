import json
import os
import psycopg2
import argparse

def main():
    parser = argparse.ArgumentParser(description="Script to process JSON data and interact with PostgreSQL")
    parser.add_argument("script_dir", help="Path to the directory containing 'accounts_dump.json'")
    parser.add_argument("--host", default="docker", help="PostgreSQL host (default: docker)")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port (default: 5432)")
    parser.add_argument("--database", default="haf_block_log", help="PostgreSQL database name (default: haf_block_log)")
    parser.add_argument("--user", default="haf_admin", help="PostgreSQL user (default: haf_admin)")
    parser.add_argument("--password", default="", help="PostgreSQL password (default: empty)")

    args = parser.parse_args()
    
    json_file_path = os.path.join(args.script_dir, "accounts_dump.json")

    with open(json_file_path) as file:
        data = json.load(file)

    accounts = data["result"]["accounts"]

    try:
        connection = psycopg2.connect(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password
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

    except psycopg2.Error as e:
        print("Error: Unable to connect to the PostgreSQL database.")
        print(e)

if __name__ == "__main__":
    main()
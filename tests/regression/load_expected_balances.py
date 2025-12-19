#!/usr/bin/env python3
"""
Balance Tracker Regression Test - Expected Data Loader

PURPOSE:
    Loads expected account balance data from a hived node JSON snapshot into
    the btracker_test.expected_account_balances table for regression testing.

USAGE:
    python3 load_expected_balances.py <json_directory> [OPTIONS]

    Arguments:
        json_directory    Path to directory containing 'accounts_dump.json'

    Options:
        --host HOST       PostgreSQL hostname (default: localhost)
        --port PORT       PostgreSQL port (default: 5432)
        --database DB     Database name (default: haf_block_log)
        --user USER       PostgreSQL username (default: haf_admin)
        --password PASS   PostgreSQL password (default: empty)

JSON FORMAT:
    The accounts_dump.json file should be the response from hived's
    database_api.list_accounts endpoint:

    {
        "result": {
            "accounts": [
                {
                    "id": 12345,
                    "balance": {"amount": "1000000", ...},
                    "hbd_balance": {"amount": "500000", ...},
                    ...
                },
                ...
            ]
        }
    }

WORKFLOW:
    1. Parse the JSON file containing account data from hived
    2. Connect to PostgreSQL database
    3. For each account, call btracker_test.load_expected_account(json)
    4. Commit the transaction

NOTE:
    The regression test schema must be installed before running this script.
    Use: tests/regression/install_test_schema.sh
"""

import argparse
import json
import os
import sys

try:
    import psycopg2
except ImportError:
    print("Error: psycopg2 not installed. Install with: pip install psycopg2-binary")
    sys.exit(1)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Load expected account balances from hived JSON snapshot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
    python3 load_expected_balances.py ./fixtures --host=localhost --port=5432
        """
    )
    parser.add_argument(
        "json_directory",
        help="Path to directory containing 'accounts_dump.json'"
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="PostgreSQL hostname (default: localhost)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5432,
        help="PostgreSQL port (default: 5432)"
    )
    parser.add_argument(
        "--database",
        default="haf_block_log",
        help="PostgreSQL database name (default: haf_block_log)"
    )
    parser.add_argument(
        "--user",
        default="haf_admin",
        help="PostgreSQL username (default: haf_admin)"
    )
    parser.add_argument(
        "--password",
        default="",
        help="PostgreSQL password (default: empty)"
    )
    return parser.parse_args()


def load_json_data(json_path):
    """Load and parse the accounts JSON file."""
    print(f"Loading JSON data from: {json_path}")

    if not os.path.exists(json_path):
        print(f"Error: File not found: {json_path}")
        sys.exit(1)

    with open(json_path, 'r') as f:
        data = json.load(f)

    accounts = data.get("result", {}).get("accounts", [])
    print(f"Loaded {len(accounts)} accounts from JSON")

    return accounts


def load_accounts_to_database(accounts, connection):
    """Insert all accounts into the expected_account_balances table."""
    cursor = connection.cursor()

    # SQL query to call the load function
    query = "SELECT btracker_test.load_expected_account(%s)"

    total = len(accounts)
    loaded = 0
    errors = 0

    print(f"Loading {total} accounts into database...")

    for i, account in enumerate(accounts):
        try:
            account_json = json.dumps(account)
            cursor.execute(query, (account_json,))
            loaded += 1
        except psycopg2.Error as e:
            errors += 1
            account_id = account.get('id', 'unknown')
            print(f"  Warning: Failed to load account {account_id}: {e}")

        # Progress indicator every 10000 accounts
        if (i + 1) % 10000 == 0:
            print(f"  Progress: {i + 1}/{total} accounts...")

    connection.commit()
    cursor.close()

    print(f"Loaded {loaded} accounts successfully ({errors} errors)")
    return loaded, errors


def main():
    """Main entry point."""
    args = parse_arguments()

    # Construct JSON file path
    json_path = os.path.join(args.json_directory, "accounts_dump.json")

    # Load JSON data
    accounts = load_json_data(json_path)

    if not accounts:
        print("Error: No accounts found in JSON file")
        sys.exit(1)

    # Connect to database
    print(f"Connecting to PostgreSQL at {args.host}:{args.port}/{args.database}")

    try:
        connection = psycopg2.connect(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password
        )
    except psycopg2.Error as e:
        print(f"Error: Failed to connect to database: {e}")
        sys.exit(1)

    try:
        # Load accounts into database
        loaded, errors = load_accounts_to_database(accounts, connection)

        if errors > 0:
            print(f"Warning: {errors} accounts failed to load")
            sys.exit(1)

        print("Done!")

    finally:
        connection.close()


if __name__ == "__main__":
    main()

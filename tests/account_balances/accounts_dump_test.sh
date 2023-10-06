#!/usr/bin/env bash

set -euo pipefail

SCRIPTDIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"


POSTGRES_HOST="localhost"
POSTGRES_PORT=5432
POSTGRES_USER="haf_admin"

print_help () {
    echo "Usage: $0 [OPTION[=VALUE]]..."
    echo
    echo "Allows to setup a database already filled by HAF instance, to work with haf_be application."
    echo "OPTIONS:"
    echo "  --host=VALUE             Allows to specify a PostgreSQL host location (defaults to localhost)"
    echo "  --port=NUMBER            Allows to specify a PostgreSQL operating port (defaults to 5432)"
    echo "  --user=VALUE             Allows to specify a PostgreSQL user (defaults to haf_admin)"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --host=*)
        POSTGRES_HOST="${1#*=}"
        ;;
    --port=*)
        POSTGRES_PORT="${1#*=}"
        ;;
    --user=*)
        POSTGRES_USER="${1#*=}"
        ;;
    --help)
        print_help
        exit 0
        ;;
    -*)
        echo "ERROR: '$1' is not a valid option"
        echo
        print_help
        exit 1
        ;;
    *)
        echo "ERROR: '$1' is not a valid argument"
        echo
        print_help
        exit 2
        ;;
    esac
    shift
done

POSTGRES_ACCESS_ADMIN="postgresql://$POSTGRES_USER@$POSTGRES_HOST:$POSTGRES_PORT/haf_block_log"

    echo "Clearing tables..."
    psql "$POSTGRES_ACCESS_ADMIN" -v "ON_ERROR_STOP=on" -c "TRUNCATE btracker_account_dump.account_balances;"
    psql "$POSTGRES_ACCESS_ADMIN" -v "ON_ERROR_STOP=on" -c "TRUNCATE btracker_account_dump.differing_accounts;"

    echo "Installing dependecies..."

    gunzip "${SCRIPTDIR}/accounts_dump.json.gz"

    pip install psycopg2-binary
 
    echo "Starting data_insertion_stript.py..."
    python3 ../../dump_accounts/data_insertion_script.py "$SCRIPTDIR" --host $POSTGRES_HOST --port $POSTGRES_PORT --user $POSTGRES_USER

    echo "Looking for diffrences between hived node and btracker stats..."
    psql "$POSTGRES_ACCESS_ADMIN" -v "ON_ERROR_STOP=on" -c "SELECT btracker_account_dump.compare_accounts();"

    DIFFERING_ACCOUNTS=$(psql "$POSTGRES_ACCESS_ADMIN" -v "ON_ERROR_STOP=on" -t -A  -c "SELECT * FROM btracker_account_dump.differing_accounts;")

    if [ -z "$DIFFERING_ACCOUNTS" ]; then
        echo "Account balances are correct!"

    else
        echo "Account balances are incorrect..."
        exit 1
    fi

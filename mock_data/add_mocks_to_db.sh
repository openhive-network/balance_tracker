#! /bin/bash -e

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 || exit 1; pwd -P )"

print_help () {
    cat <<EOF
Usage: $0 [OPTION[=VALUE]]...

Script for mocking data in the balance tracker database
OPTIONS:
    --postgres-host=HOSTNAME              PostgreSQL hostname (default: localhost)
    --postgres-port=PORT                  PostgreSQL port (default: 5432)
    --postgres-user=USERNAME              PostgreSQL user name (default: haf_admin)
    --postgres-url=URL                    PostgreSQL URL (if set, overrides three previous options, empty by default)
    --help,-h,-?                          Displays this help message
EOF
}

POSTGRES_USER=${POSTGRES_USER:-"haf_admin"}
POSTGRES_HOST=${POSTGRES_HOST:-"localhost"}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_URL=${POSTGRES_URL:-""}

while [ $# -gt 0 ]; do
  case "$1" in
    --postgres-host=*)
        POSTGRES_HOST="${1#*=}"
        ;;
    --postgres-port=*)
        POSTGRES_PORT="${1#*=}"
        ;;
    --postgres-user=*)
        POSTGRES_USER="${1#*=}"
        ;;
    --postgres-url=*)
        POSTGRES_URL="${1#*=}"
        ;;
    --help|-h|-?)
        print_help
        exit 0
        ;;
    -*)
        echo "ERROR: '$1' is not a valid option"
        echo
        exit 1
        ;;
    *)
        echo "ERROR: '$1' is not a valid argument"
        echo
        exit 2
        ;;
    esac
    shift
done

POSTGRES_ACCESS=${POSTGRES_URL:-"postgresql://$POSTGRES_USER@$POSTGRES_HOST:$POSTGRES_PORT/haf_block_log?application_name=btracker_mocks"}

# Load mock data from JSON files
MOCKED_BLOCKS=$(cat "$SCRIPTPATH/blocks/mock_blocks.json")
MOCKED_SAVINGS=$(cat "$SCRIPTPATH/savings/mock_savings.json")
MOCK_START=90000000
MOCK_END=90000030

# Execute SQL scripts and functions
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -f "$SCRIPTPATH/fill_tables_with_mocks.sql"

psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -c "SELECT btracker_backend.insert_mock_blocks('$MOCKED_BLOCKS')"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -c "SELECT btracker_backend.insert_mock_operations('$MOCKED_SAVINGS')"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -c "SELECT btracker_backend.update_irreversible_block($MOCK_START,$MOCK_END)"
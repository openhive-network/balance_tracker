#! /bin/bash -e

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 || exit 1; pwd -P )"

print_help () {
    cat <<EOF
Usage: $0 [OPTION[=VALUE]]...

Script for mocking data in the balance tracker database
OPTIONS:
    --host=HOSTNAME              PostgreSQL hostname (default: localhost)
    --port=PORT                  PostgreSQL port (default: 5432)
    --user=USERNAME              PostgreSQL user name (default: haf_admin)
    --url=URL                    PostgreSQL URL (if set, overrides three previous options, empty by default)
    --help,-h,-?                          Displays this help message
EOF
}

POSTGRES_USER=${POSTGRES_USER:-"haf_admin"}
POSTGRES_HOST=${POSTGRES_HOST:-"localhost"}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_URL=${POSTGRES_URL:-""}

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
MOCKED_BLOCKS=$(cat "$SCRIPTPATH/../mock_data/blocks/mock_blocks.json")
MOCKED_SAVINGS=$(cat "$SCRIPTPATH/../mock_data/savings/mock_savings.json")
MOCKED_REWARDS=$(cat "$SCRIPTPATH/../mock_data/rewards/mock_rewards.json")
MOCKED_TRANSFERS=$(cat "$SCRIPTPATH/../mock_data/recurrent_transfers/mock_recurrent_transfers.json")
MOCKED_DELEGATIONS=$(cat "$SCRIPTPATH/../mock_data/delegations/mock_delegations.json")
MOCKED_DELAYS=$(cat "$SCRIPTPATH/../mock_data/delays/mock_delays.json")
MOCKED_ESCROW=$(cat "$SCRIPTPATH/../mock_data/escrow/mock_escrow.json")
MOCK_START=90000000
MOCK_END=90000048

# Create SQL scripts and functions
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -f "$SCRIPTPATH/../mock_data/fill_tables_with_mocks.sql"

# Insert mock blocks
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -c "SELECT btracker_backend.insert_mock_blocks('$MOCKED_BLOCKS')"

# Insert mock operations
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -c "SELECT btracker_backend.insert_mock_operations('$MOCKED_SAVINGS')"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -c "SELECT btracker_backend.insert_mock_operations('$MOCKED_REWARDS')"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -c "SELECT btracker_backend.insert_mock_operations('$MOCKED_TRANSFERS')"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -c "SELECT btracker_backend.insert_mock_operations('$MOCKED_DELEGATIONS')"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -c "SELECT btracker_backend.insert_mock_operations('$MOCKED_DELAYS')"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -c "SELECT btracker_backend.insert_mock_operations('$MOCKED_ESCROW')"
# Update app last block
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -c "SELECT btracker_backend.update_irreversible_block($MOCK_START,$MOCK_END)"
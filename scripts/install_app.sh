#! /bin/sh -e

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 || exit 1; pwd -P )"

print_help () {
    cat <<EOF
Usage: $0 [OPTION[=VALUE]]...

Script for setting up the balance tracker database
OPTIONS:
    --postgres-host=HOSTNAME              PostgreSQL hostname (default: localhost)
    --postgres-port=PORT                  PostgreSQL port (default: 5432)
    --postgres-user=USERNAME              PostgreSQL user name (default: haf_admin)
    --postgres-url=URL                    PostgreSQL URL (if set, overrides three previous options, empty by default)
    --no-context=true|false               When set to true, do not create context (default: false)
    --no-context                          The same as '--no-context=true'
    --help,-h,-?                          Displays this help message
EOF
}

POSTGRES_USER=${POSTGRES_USER:-"haf_admin"}
POSTGRES_HOST=${POSTGRES_HOST:-"localhost"}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_URL=${POSTGRES_URL:-""}
BTRACKER_SCHEMA=${BTRACKER_SCHEMA:-"btracker_app"}

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
    --schema=*)
        BTRACKER_SCHEMA="${1#*=}"
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

POSTGRES_ACCESS=${POSTGRES_URL:-"postgresql://$POSTGRES_USER@$POSTGRES_HOST:$POSTGRES_PORT/haf_block_log"}

echo "Installing app..."
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -v BTRACKER_SCHEMA="${BTRACKER_SCHEMA}" -f "$SCRIPTPATH/../db/builtin_roles.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -v BTRACKER_SCHEMA="${BTRACKER_SCHEMA}" -c "SET ROLE btracker_owner;CREATE SCHEMA IF NOT EXISTS ${BTRACKER_SCHEMA} AUTHORIZATION btracker_owner;"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -v BTRACKER_SCHEMA="${BTRACKER_SCHEMA}" -c "SET ROLE btracker_owner;GRANT USAGE ON SCHEMA ${BTRACKER_SCHEMA} to btracker_user;"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -v BTRACKER_SCHEMA="${BTRACKER_SCHEMA}" -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../db/btracker_app.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -v BTRACKER_SCHEMA="${BTRACKER_SCHEMA}" -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../db/process_block_range.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -v BTRACKER_SCHEMA="${BTRACKER_SCHEMA}" -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../db/process_delegations.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -v BTRACKER_SCHEMA="${BTRACKER_SCHEMA}" -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../db/process_delayed.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -v BTRACKER_SCHEMA="${BTRACKER_SCHEMA}" -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../db/process_savings.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -v BTRACKER_SCHEMA="${BTRACKER_SCHEMA}" -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../db/process_rewards.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -v BTRACKER_SCHEMA="${BTRACKER_SCHEMA}" -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../db/process_withdraws.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -v BTRACKER_SCHEMA="${BTRACKER_SCHEMA}" -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../api/btracker_api.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -v BTRACKER_SCHEMA="${BTRACKER_SCHEMA}" -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/get_account_balances.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -v BTRACKER_SCHEMA="${BTRACKER_SCHEMA}" -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../dump_accounts/account_dump_schema.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -v BTRACKER_SCHEMA="${BTRACKER_SCHEMA}" -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../dump_accounts/account_stats_btracker.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -v BTRACKER_SCHEMA="${BTRACKER_SCHEMA}" -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../dump_accounts/compare_accounts.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -v BTRACKER_SCHEMA="${BTRACKER_SCHEMA}" -c "SET ROLE btracker_owner;GRANT SELECT ON ALL TABLES IN SCHEMA ${BTRACKER_SCHEMA} TO btracker_user;"

#! /bin/bash -e

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
    --swagger-url=URL                     Allows to specify a server URL
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
SWAGGER_URL=${SWAGGER_URL:-"{btracker-host}"}


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
    --swagger-url=*)
        SWAGGER_URL="${1#*=}"
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

POSTGRES_ACCESS=${POSTGRES_URL:-"postgresql://$POSTGRES_USER@$POSTGRES_HOST:$POSTGRES_PORT/haf_block_log?application_name=btracker_install"}

echo "Installing app..."
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -f "$SCRIPTPATH/../db/builtin_roles.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET ROLE btracker_owner;CREATE SCHEMA IF NOT EXISTS ${BTRACKER_SCHEMA} AUTHORIZATION btracker_owner;"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET custom.swagger_url = '$SWAGGER_URL'; SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/endpoint_schema.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/types/coin_type.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/types/granularity.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/types/sort_direction.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/types/balances.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/types/delegations.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/types/recurrent_transfers.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/types/transfer_stats.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/types/ranked_holder.sql"

psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/aggregated_history.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/delays.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/delegations.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/hardforks.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/rewards.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/savings.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/withdrawals.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/endpoint_helpers.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/recurrent_transfers.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/transfers.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/aggregated_transfers.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/utilities/account.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/utilities/coin_type.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/utilities/validators.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/utilities/exceptions.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/utilities/paging.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/balance_history/filtering_functions/liquid_balance_history.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/balance_history/filtering_functions/savings_balance_history.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../backend/balance_history/balance_history.sql"

psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../db/btracker_app.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../db/process_balances.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../db/process_savings.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../db/process_rewards.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../db/process_delegations.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../db/process_withdrawals.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../db/process_recurrent_transfers.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../db/process_transfer_stats.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../db/process_block_range_converts.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../db/process_block_range_orders.sql"

psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/account-balances/get_balance_history.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/account-balances/get_account_balances.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/account-balances/get_history_aggregation.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/account-balances/get_account_delegations.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/account-balances/get_recurrent_transfers.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/account-balances/get_top_holders.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/transfers/get_transfer_statistics.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/other/get_btracker_last_synced_block.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../endpoints/other/get_btracker_version.sql"

psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../dump_accounts/account_dump_schema.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../dump_accounts/account_stats_btracker.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/../dump_accounts/compare_accounts.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" -f "$SCRIPTPATH/set_version_in_sql.pgsql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET ROLE btracker_owner;GRANT USAGE ON SCHEMA ${BTRACKER_SCHEMA} to btracker_user;"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET ROLE btracker_owner;GRANT USAGE ON SCHEMA btracker_endpoints to btracker_user;"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET ROLE btracker_owner;GRANT USAGE ON SCHEMA btracker_backend to btracker_user;"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET ROLE btracker_owner;GRANT SELECT ON ALL TABLES IN SCHEMA ${BTRACKER_SCHEMA} TO btracker_user;"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET ROLE btracker_owner;GRANT SELECT ON ALL TABLES IN SCHEMA btracker_endpoints TO btracker_user;"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET ROLE btracker_owner;GRANT SELECT ON ALL TABLES IN SCHEMA btracker_backend TO btracker_user;"

# Allow hived to fullfil vacuum full requests
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET ROLE btracker_owner;GRANT MAINTAIN ON ALL TABLES IN SCHEMA ${BTRACKER_SCHEMA} TO hived_group;"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on  -c "SET ROLE btracker_owner;GRANT ALL ON SCHEMA ${BTRACKER_SCHEMA} TO hived_group;"
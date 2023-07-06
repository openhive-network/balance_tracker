#! /bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 || exit 1; pwd -P )"

POSTGRES_USER=${POSTGRES_USER:-"haf_admin"}
POSTGRES_HOST=${POSTGRES_HOST:-"localhost"}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_URL=${POSTGRES_URL:-""}
REMOVE_CONTEXT=${REMOVE_CONTEXT:-"false"}
SKIP_IF_DB_EXISTS=${SKIP_IF_DB_EXISTS:-"false"}

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
    --remove-context=*)
        REMOVE_CONTEXT="${1#*=}"
        ;;
    --remove-context)
        REMOVE_CONTEXT="true"
        ;;
    --skip-if-db-exists=*)
        SKIP_IF_DB_EXISTS="${1#*=}"
        ;;
    --skip-if-db-exists)
        SKIP_IF_DB_EXISTS="true"
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

if [[ "$REMOVE_CONTEXT" == "true" && "$SKIP_IF_DB_EXISTS" == "true" ]]; then
    echo "'--remove-context(=true)' and '--skip-if-db-exists(=true)' are mutually exclusive"
    exit 3
fi

if [[ "$SKIP_IF_DB_EXISTS" == "true" ]]; then
    psql "$POSTGRES_ACCESS" -c "SELECT 'btracker_app.main'::regproc"
    DB_EXISTS=$?
    [[ "$DB_EXISTS" == "0" ]] && echo "Database already exists, exiting..." && exit 0
fi


if [[ "$REMOVE_CONTEXT" == "true" ]]; then
    echo "Deleting app..."
    psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -c "do \$\$ BEGIN if hive.app_context_exists('btracker_app') THEN perform hive.app_remove_context('btracker_app'); end if; END \$\$"
fi

echo "Installing app..."
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -f "$SCRIPTPATH/../db/builtin_roles.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -f "$SCRIPTPATH/../db/btracker_app.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -f "$SCRIPTPATH/../db/process_block_range.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -f "$SCRIPTPATH/../db/process_delegations.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -f "$SCRIPTPATH/../db/process_savings.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -f "$SCRIPTPATH/../db/process_rewards.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -f "$SCRIPTPATH/../db/process_withdraws.sql"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -f "$SCRIPTPATH/../api/btracker_api.sql"

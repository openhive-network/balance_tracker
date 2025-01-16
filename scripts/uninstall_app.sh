#! /bin/sh

set -e

print_help () {
    cat <<EOF
Usage: $0 [OPTION[=VALUE]]...

Allows to setup a database already filled by HAF instance, to work with haf_be application.
OPTIONS:
    --host=VALUE             Allows to specify a PostgreSQL host location (defaults to localhost)
    --port=NUMBER            Allows to specify a PostgreSQL operating port (defaults to 5432)
    --user=VALUE             Allows to specify a PostgreSQL user (defaults to haf_admin)
    --url=URL                Allows to specify a PostgreSQL URL (empty by default, overrides options above)
    --help,-h,-?             Displays this help message
EOF
}

POSTGRES_USER=${POSTGRES_USER:-"haf_admin"}
POSTGRES_HOST=${POSTGRES_HOST:-"localhost"}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_URL=${POSTGRES_URL:-""}
BTRACKER_SCHEMA=${BTRACKER_SCHEMA:-"btracker_app"}

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

POSTGRES_ACCESS_ADMIN=${POSTGRES_URL:-"postgresql://$POSTGRES_USER@$POSTGRES_HOST:$POSTGRES_PORT/haf_block_log"}


uninstall_app() {
    psql "$POSTGRES_ACCESS_ADMIN" -v "ON_ERROR_STOP=OFF" -c "do \$\$ BEGIN if hive.app_context_exists('${BTRACKER_SCHEMA}') THEN perform hive.app_remove_context('${BTRACKER_SCHEMA}'); end if; END \$\$"
    psql "$POSTGRES_ACCESS_ADMIN" -v "ON_ERROR_STOP=OFF" -c "DROP SCHEMA IF EXISTS ${BTRACKER_SCHEMA} CASCADE;"
    psql "$POSTGRES_ACCESS_ADMIN" -v "ON_ERROR_STOP=OFF" -c "DROP SCHEMA IF EXISTS btracker_account_dump CASCADE;"
    psql "$POSTGRES_ACCESS_ADMIN" -v "ON_ERROR_STOP=OFF" -c "DROP SCHEMA IF EXISTS btracker_endpoints CASCADE;"

    psql "$POSTGRES_ACCESS_ADMIN" -c "DROP OWNED BY btracker_owner CASCADE" || true
    psql "$POSTGRES_ACCESS_ADMIN" -c "DROP ROLE btracker_owner" || true

    psql "$POSTGRES_ACCESS_ADMIN" -c "DROP OWNED BY btracker_user CASCADE" || true
    psql "$POSTGRES_ACCESS_ADMIN" -c "DROP ROLE btracker_user" || true

}

uninstall_app

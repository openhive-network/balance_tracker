#! /bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"


POSTGRES_USER="haf_admin"
POSTGRES_HOST="localhost"
POSTGRES_PORT=5432
POSTGRES_URL=""
NO_CONTEXT=""

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
    --no-context=*)
        NO_CONTEXT="${1#*=}"
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

if [ -z "$POSTGRES_URL" ]; then
  POSTGRES_ACCESS="postgresql://$POSTGRES_USER@$POSTGRES_HOST:$POSTGRES_PORT/haf_block_log"
else
  POSTGRES_ACCESS=$POSTGRES_URL
fi

if [ -z "$NO_CONTEXT" ]; then
psql $POSTGRES_ACCESS -v ON_ERROR_STOP=on -c "do \$\$ BEGIN if hive.app_context_exists('btracker_app') THEN perform hive.app_remove_context('btracker_app'); end if; END \$\$"
fi

psql $POSTGRES_ACCESS -v ON_ERROR_STOP=on -f $SCRIPTPATH/../db/builtin_roles.sql
psql $POSTGRES_ACCESS -v ON_ERROR_STOP=on -f $SCRIPTPATH/../db/btracker_app.sql

psql $POSTGRES_ACCESS -v ON_ERROR_STOP=on -f $SCRIPTPATH/../db/process_delegations.sql

psql $POSTGRES_ACCESS -v ON_ERROR_STOP=on -f $SCRIPTPATH/../db/process_savings.sql

psql $POSTGRES_ACCESS -v ON_ERROR_STOP=on -f $SCRIPTPATH/../db/process_rewards.sql

psql $POSTGRES_ACCESS -v ON_ERROR_STOP=on -f $SCRIPTPATH/../api/btracker_api.sql

#! /bin/sh
set -e
cd /app/scripts

if [ "$1" = "setup_db" ]; then
  shift
  exec /app/scripts/setup_db.sh --postgres-host="${POSTGRES_HOST:-haf}" "$@"
elif [ "$1" = "process_blocks" ]; then
  shift
  exec /app/balance-tracker.sh process-blocks "$@"
elif [ "$1" = "drop_db" ]; then
  shift
  exec /app/scripts/drop_db.sh --host="${POSTGRES_HOST:-haf}" --user="${POSTGRES_USER:-haf_app_admin}" "$@"
else
  echo "usage: $0 setup_db|process_blocks|drop_db"
  exit 1
fi



#! /bin/sh
set -e
cd /app/scripts

if [ "$1" = "install_app" ]; then
  shift
  exec /app/scripts/install_app.sh --postgres-host="${POSTGRES_HOST:-haf}" "$@"
elif [ "$1" = "process_blocks" ]; then
  shift
  exec /app/balance-tracker.sh process-blocks "$@"
elif [ "$1" = "uninstall_app" ]; then
  shift
  exec /app/scripts/uninstall_app.sh --host="${POSTGRES_HOST:-haf}" --user="${POSTGRES_USER:-btracker_user}" "$@"
else
  echo "usage: $0 install_app|process_blocks|uninstall_app"
  exit 1
fi



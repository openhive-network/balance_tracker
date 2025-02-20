#! /bin/sh
set -e
cd /app/scripts

install_timescaledb() {
  echo "Installing TimescaleDB extension..."
  until nc -z "$POSTGRES_HOST" 5432; do
    echo "Waiting for PostgreSQL to start..."
    sleep 1
  done

  psql -h "${POSTGRES_HOST:-haf}" -U "${POSTGRES_USER:-haf_admin}" -d "haf_block_log" -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
}

if [ "$1" = "install_app" ]; then
  shift
  exec /app/scripts/install_app.sh --postgres-host="${POSTGRES_HOST:-haf}" "$@"
elif [ "$1" = "process_blocks" ]; then
  shift
  exec /app/scripts/process_blocks.sh --host="${POSTGRES_HOST:-haf}" "$@"
elif [ "$1" = "uninstall_app" ]; then
  shift
  exec /app/scripts/uninstall_app.sh --host="${POSTGRES_HOST:-haf}" --user="${POSTGRES_USER:-haf_admin}" "$@"
elif [ "$1" = "install_timescaledb" ]; then
  shift
  install_timescaledb
else
  echo "usage: $0 install_app|process_blocks|uninstall_app"
  exit 1
fi



#! /bin/bash
set -e
cd /app/scripts

if [ "$1" = "install_app" ]; then
  shift
  exec ./install_app.sh --postgres-host="${POSTGRES_HOST:-haf}" "$@"
elif [ "$1" = "process_blocks" ]; then
  shift
  exec ./process_blocks.sh --host="${POSTGRES_HOST:-haf}" "$@"
elif [ "$1" = "uninstall_app" ]; then
  shift
  exec ./uninstall_app.sh --host="${POSTGRES_HOST:-haf}" --user="${POSTGRES_USER:-haf_admin}" "$@"
else
  echo "usage: $0 install_app|process_blocks|uninstall_app"
  exit 1
fi

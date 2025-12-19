#!/usr/bin/env bash
# =============================================================================
# Balance Tracker Docker Entrypoint
# =============================================================================
#
# PURPOSE:
#   Routes Docker container commands to the appropriate Balance Tracker scripts.
#
# USAGE:
#   docker run <image> <command> [options]
#
# COMMANDS:
#   install_app     Install Balance Tracker schema into HAF database
#   process_blocks  Process blockchain blocks and update balance data
#   uninstall_app   Remove Balance Tracker schema from database
#   install_mocks   Load mock data for testing (from tests/mocks/)
#
# ENVIRONMENT:
#   POSTGRES_HOST   PostgreSQL hostname (default: haf)
#   POSTGRES_USER   PostgreSQL username (default: haf_admin)
# =============================================================================

set -e

if [ "$1" = "install_app" ]; then
  shift
  exec /app/scripts/install_app.sh --postgres-host="${POSTGRES_HOST:-haf}" "$@"
elif [ "$1" = "process_blocks" ]; then
  shift
  exec /app/scripts/process_blocks.sh --host="${POSTGRES_HOST:-haf}" "$@"
elif [ "$1" = "uninstall_app" ]; then
  shift
  exec /app/scripts/uninstall_app.sh --host="${POSTGRES_HOST:-haf}" --user="${POSTGRES_USER:-haf_admin}" "$@"
elif [ "$1" = "install_mocks" ]; then
  shift
  exec /app/tests/mocks/install_mock_data.sh --host="${POSTGRES_HOST:-haf}" --user="${POSTGRES_USER:-haf_admin}" "$@"
else
  echo "Balance Tracker Docker Commands:"
  echo ""
  echo "  install_app     Install Balance Tracker schema"
  echo "  process_blocks  Process blockchain blocks"
  echo "  uninstall_app   Remove Balance Tracker schema"
  echo "  install_mocks   Load mock test data"
  echo ""
  echo "Usage: docker run <image> <command> [options]"
  exit 1
fi

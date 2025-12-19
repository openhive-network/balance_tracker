#!/bin/bash
# =============================================================================
# Balance Tracker Regression Test Schema Installer
# =============================================================================
#
# PURPOSE:
#   Installs the regression test schema (btracker_test) which provides
#   infrastructure for comparing Balance Tracker's computed values against
#   expected values from a hived node snapshot.
#
# USAGE:
#   ./install_test_schema.sh [OPTIONS]
#
# OPTIONS:
#   --host=HOSTNAME     PostgreSQL hostname (default: localhost)
#   --port=NUMBER       PostgreSQL port (default: 5432)
#   --user=USERNAME     PostgreSQL user (default: haf_admin)
#   --url=URL           PostgreSQL URL (overrides host/port/user)
#
# NOTE:
#   This schema should only be installed for testing, not in production.
#   The main install_app.sh does NOT install this schema.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Default values
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-haf_admin}"
POSTGRES_URL="${POSTGRES_URL:-}"

print_help() {
    cat <<EOF
Usage: $0 [OPTIONS]

Installs the regression test schema for Balance Tracker.

OPTIONS:
    --host=HOSTNAME     PostgreSQL hostname (default: localhost)
    --port=NUMBER       PostgreSQL port (default: 5432)
    --user=USERNAME     PostgreSQL user (default: haf_admin)
    --url=URL           PostgreSQL URL (overrides host/port/user)
    --help              Show this help message
EOF
}

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
        --url=*)
            POSTGRES_URL="${1#*=}"
            ;;
        --help|-h)
            print_help
            exit 0
            ;;
        *)
            echo "ERROR: Unknown option: $1"
            print_help
            exit 1
            ;;
    esac
    shift
done

# Build connection string
POSTGRES_ACCESS="${POSTGRES_URL:-postgresql://$POSTGRES_USER@$POSTGRES_HOST:$POSTGRES_PORT/haf_block_log}"

echo "Installing regression test schema..."
echo "  Connection: $POSTGRES_ACCESS"

# Install SQL files in order (numbered prefix ensures correct order)
for sql_file in "$SCRIPT_DIR/sql/"*.sql; do
    echo "  Installing: $(basename "$sql_file")"
    psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -f "$sql_file"
done

echo "Regression test schema installed successfully."

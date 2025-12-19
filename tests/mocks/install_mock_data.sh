#!/usr/bin/env bash
# =============================================================================
# Balance Tracker Mock Data Installation Script
# =============================================================================
#
# PURPOSE:
#   Loads mock blockchain data into the HAF database for testing Balance
#   Tracker functionality. This allows testing without a live blockchain
#   by simulating specific operation sequences.
#
# USAGE:
#   ./install_mock_data.sh [OPTIONS]
#
# OPTIONS:
#   --host=HOSTNAME    PostgreSQL hostname (default: localhost)
#   --port=PORT        PostgreSQL port (default: 5432)
#   --user=USERNAME    PostgreSQL user (default: haf_admin)
#   --url=URL          PostgreSQL URL (overrides host/port/user)
#   --help, -h         Show this help message
#
# PREREQUISITES:
#   - HAF database must be running and accessible
#   - Balance Tracker schema must be installed (run install_app.sh first)
#   - Database must contain basic accounts (blocktrades, gtg, steem, cvk, etc.)
#
# MOCK DATA STRUCTURE:
#   tests/mocks/
#   ├── install_mock_data.sh     # This script
#   ├── sql/
#   │   ├── types.sql            # Type definitions for JSON parsing
#   │   ├── insert_blocks.sql    # insert_mock_blocks() function
#   │   ├── insert_operations.sql # insert_mock_operations() function
#   │   └── update_haf_state.sql # HAF state update functions
#   └── fixtures/
#       ├── blocks/              # Mock block headers
#       ├── savings/             # Savings operations test data
#       ├── rewards/             # Rewards operations test data
#       ├── recurrent_transfers/ # Recurrent transfer test data
#       ├── delegations/         # Vesting delegation test data
#       ├── delays/              # Delayed voting (HF24) test data
#       └── escrow/              # Escrow operations test data
#
# BLOCK RANGE:
#   Mock blocks use block numbers >= 90000000 to avoid conflicts with
#   real blockchain data. The actual range is determined dynamically
#   from the inserted fixture data - no hardcoded values needed.
#
# WORKFLOW:
#   1. Install SQL helper functions (insert_mock_blocks, insert_mock_operations)
#   2. Insert mock block headers from fixtures/blocks/data.json
#   3. Insert mock operations from each fixture category
#   4. Update HAF state to recognize mock blocks as processable
#
# AFTER RUNNING:
#   Run Balance Tracker's process_blocks.sh to process the mock operations.
#   Then use tests/tavern_mocks/ to validate API responses.
#
# SEE ALSO:
#   - tests/tavern_mocks/         API tests using mock data
#   - docker-compose-mocks.yml    Docker setup for mock testing
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# -----------------------------------------------------------------------------
# Configuration Defaults
# -----------------------------------------------------------------------------
POSTGRES_USER="${POSTGRES_USER:-haf_admin}"
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_URL="${POSTGRES_URL:-}"

# -----------------------------------------------------------------------------
# Help
# -----------------------------------------------------------------------------
print_help() {
    cat <<EOF
Usage: $0 [OPTIONS]

Loads mock blockchain data into the HAF database for testing Balance Tracker.

OPTIONS:
    --host=HOSTNAME    PostgreSQL hostname (default: localhost)
    --port=PORT        PostgreSQL port (default: 5432)
    --user=USERNAME    PostgreSQL user (default: haf_admin)
    --url=URL          PostgreSQL URL (overrides host/port/user)
    --help, -h         Show this help message

EXAMPLES:
    # Local development
    ./install_mock_data.sh

    # CI environment with Docker
    ./install_mock_data.sh --host=docker --user=haf_admin

    # Custom PostgreSQL URL
    ./install_mock_data.sh --url=postgresql://user@host:5432/haf_block_log
EOF
}

# -----------------------------------------------------------------------------
# Parse Arguments
# -----------------------------------------------------------------------------
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
        --help|-h|-?)
            print_help
            exit 0
            ;;
        -*)
            echo "ERROR: Unknown option: $1"
            echo
            print_help
            exit 1
            ;;
        *)
            echo "ERROR: Unexpected argument: $1"
            echo
            print_help
            exit 2
            ;;
    esac
    shift
done

# Build connection string
POSTGRES_ACCESS="${POSTGRES_URL:-postgresql://$POSTGRES_USER@$POSTGRES_HOST:$POSTGRES_PORT/haf_block_log?application_name=btracker_mocks}"

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
echo "=============================================="
echo "Balance Tracker Mock Data Installation"
echo "=============================================="
echo "  Host: $POSTGRES_HOST:$POSTGRES_PORT"
echo "  User: $POSTGRES_USER"
echo ""

# Step 1: Install SQL helper functions (in dependency order)
echo "Step 1: Installing mock data SQL functions..."
echo "  - Loading type definitions..."
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -f "$SCRIPT_DIR/sql/types.sql"
echo "  - Loading insert_blocks function..."
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -f "$SCRIPT_DIR/sql/insert_blocks.sql"
echo "  - Loading insert_operations function..."
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -f "$SCRIPT_DIR/sql/insert_operations.sql"
echo "  - Loading HAF state functions..."
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -f "$SCRIPT_DIR/sql/update_haf_state.sql"
echo ""

# Step 2: Load fixture data from JSON files
echo "Step 2: Loading fixture data..."

FIXTURES_DIR="$SCRIPT_DIR/fixtures"
BLOCKS_DATA=$(cat "$FIXTURES_DIR/blocks/data.json")
SAVINGS_DATA=$(cat "$FIXTURES_DIR/savings/data.json")
REWARDS_DATA=$(cat "$FIXTURES_DIR/rewards/data.json")
TRANSFERS_DATA=$(cat "$FIXTURES_DIR/recurrent_transfers/data.json")
DELEGATIONS_DATA=$(cat "$FIXTURES_DIR/delegations/data.json")
DELAYS_DATA=$(cat "$FIXTURES_DIR/delays/data.json")
ESCROW_DATA=$(cat "$FIXTURES_DIR/escrow/data.json")

# Step 3: Insert mock blocks
echo "Step 3: Inserting mock block headers..."
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
    -c "SELECT btracker_backend.insert_mock_blocks('$BLOCKS_DATA')"
echo ""

# Step 4: Insert mock operations by category
echo "Step 4: Inserting mock operations..."
echo "  - Savings operations..."
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
    -c "SELECT btracker_backend.insert_mock_operations('$SAVINGS_DATA')"

echo "  - Rewards operations..."
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
    -c "SELECT btracker_backend.insert_mock_operations('$REWARDS_DATA')"

echo "  - Recurrent transfer operations..."
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
    -c "SELECT btracker_backend.insert_mock_operations('$TRANSFERS_DATA')"

echo "  - Delegation operations..."
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
    -c "SELECT btracker_backend.insert_mock_operations('$DELEGATIONS_DATA')"

echo "  - Delayed voting operations..."
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
    -c "SELECT btracker_backend.insert_mock_operations('$DELAYS_DATA')"

echo "  - Escrow operations..."
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
    -c "SELECT btracker_backend.insert_mock_operations('$ESCROW_DATA')"
echo ""

# Step 5: Update HAF state to recognize mock blocks
# The function dynamically detects block range from inserted data and returns it
echo "Step 5: Updating HAF state for mock block range..."
BLOCK_RANGE=$(psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -t -A \
    -c "SELECT start_block || '-' || end_block FROM btracker_backend.update_irreversible_block()")
echo "  Detected block range: $BLOCK_RANGE"
echo ""

echo "=============================================="
echo "SUCCESS: Mock data installed"
echo "=============================================="
echo "  Block range: $BLOCK_RANGE"
echo ""
echo "Next steps:"
echo "  1. Run: ./scripts/process_blocks.sh"
echo "  2. Test: cd tests/tavern_mocks && pytest"
echo ""

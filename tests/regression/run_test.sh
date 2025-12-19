#!/usr/bin/env bash
# =============================================================================
# Balance Tracker Regression Test
# =============================================================================
#
# PURPOSE:
#   Compares Balance Tracker's computed account balances against expected
#   values from a hived node snapshot to detect regressions.
#
# WORKFLOW:
#   1. Install the regression test schema (btracker_test)
#   2. Clear any previous test data
#   3. Decompress the accounts_dump.json.gz fixture
#   4. Load expected values into the database
#   5. Run comparison and check for discrepancies
#
# PREREQUISITES:
#   - Balance Tracker schema must be installed and synced
#   - accounts_dump.json.gz fixture must exist in this directory
#
# EXIT CODES:
#   0 - All account balances match
#   1 - One or more accounts have discrepancies
#
# -----------------------------------------------------------------------------
# RECREATING THE FIXTURE (accounts_dump.json.gz)
# -----------------------------------------------------------------------------
#
# The fixture contains account data from hived's database_api.list_accounts.
# To create a new fixture for a different block height:
#
# 1. Configure hived with increased API limits:
#    In your hived config, set: api-limit = 10000000
#    (The default limit is much lower and won't return all accounts)
#
# 2. Sync hived to your target block height and let it stabilize
#
# 3. Fetch account data:
#    curl -s -o accounts_dump.json \
#      --data '{"jsonrpc":"2.0", "method":"database_api.list_accounts", \
#               "params": {"start":"", "limit":10000000, "order":"by_name"}, \
#               "id":1}' \
#      "http://localhost:8091"
#
# 4. Clean single quotes (can cause issues):
#    sed -i "s/'//g" accounts_dump.json
#
# 5. Compress for storage:
#    gzip accounts_dump.json
#
# 6. Sync Balance Tracker to the SAME block height as the fixture
#
# 7. Run this test to verify Balance Tracker matches hived
#
# NOTE: The fixture block height must match the Balance Tracker sync point,
#       otherwise balances will differ due to additional processed blocks.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Default configuration
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-haf_admin}"
BTRACKER_SCHEMA="${BTRACKER_SCHEMA:-btracker_app}"

print_help() {
    cat <<EOF
Usage: $0 [OPTIONS]

Runs regression test comparing Balance Tracker values against expected snapshot.

OPTIONS:
    --host=HOSTNAME     PostgreSQL hostname (default: localhost)
    --port=NUMBER       PostgreSQL port (default: 5432)
    --user=USERNAME     PostgreSQL user (default: haf_admin)
    --schema=SCHEMA     Balance Tracker schema name (default: btracker_app)
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
        --schema=*)
            BTRACKER_SCHEMA="${1#*=}"
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

POSTGRES_ACCESS="postgresql://$POSTGRES_USER@$POSTGRES_HOST:$POSTGRES_PORT/haf_block_log"

echo "=============================================="
echo "Balance Tracker Regression Test"
echo "=============================================="
echo "  Host: $POSTGRES_HOST:$POSTGRES_PORT"
echo "  User: $POSTGRES_USER"
echo "  Schema: $BTRACKER_SCHEMA"
echo ""

# -----------------------------------------------------------------------------
# Step 1: Install regression test schema
# -----------------------------------------------------------------------------
echo "Step 1: Installing regression test schema..."
"$SCRIPT_DIR/install_test_schema.sh" \
    --host="$POSTGRES_HOST" \
    --port="$POSTGRES_PORT" \
    --user="$POSTGRES_USER"
echo ""

# -----------------------------------------------------------------------------
# Step 2: Clear previous test data
# -----------------------------------------------------------------------------
echo "Step 2: Clearing previous test data..."
psql "$POSTGRES_ACCESS" -v "ON_ERROR_STOP=on" \
    -c "TRUNCATE btracker_test.expected_account_balances;"
psql "$POSTGRES_ACCESS" -v "ON_ERROR_STOP=on" \
    -c "TRUNCATE btracker_test.differing_accounts;"
echo ""

# -----------------------------------------------------------------------------
# Step 3: Decompress fixture data
# -----------------------------------------------------------------------------
echo "Step 3: Preparing fixture data..."
rm -f "$SCRIPT_DIR/accounts_dump.json"

if [ ! -f "$SCRIPT_DIR/accounts_dump.json.gz" ]; then
    echo "ERROR: Fixture file not found: $SCRIPT_DIR/accounts_dump.json.gz"
    exit 1
fi

# Decompress (handle different gunzip versions)
gunzip -k "$SCRIPT_DIR/accounts_dump.json.gz" 2>/dev/null || \
    gunzip -c "$SCRIPT_DIR/accounts_dump.json.gz" > "$SCRIPT_DIR/accounts_dump.json"
echo "  Decompressed accounts_dump.json"
echo ""

# -----------------------------------------------------------------------------
# Step 4: Install psycopg2 if needed (not in CI)
# -----------------------------------------------------------------------------
if [[ -z "${CI:-}" ]]; then
    echo "Step 4: Installing Python dependencies..."
    pip install psycopg2-binary --quiet
    echo ""
fi

# -----------------------------------------------------------------------------
# Step 5: Load expected data
# -----------------------------------------------------------------------------
echo "Step 5: Loading expected account data..."
python3 "$SCRIPT_DIR/load_expected_balances.py" \
    "$SCRIPT_DIR" \
    --host "$POSTGRES_HOST" \
    --port "$POSTGRES_PORT" \
    --user "$POSTGRES_USER"
echo ""

# -----------------------------------------------------------------------------
# Step 6: Run comparison
# -----------------------------------------------------------------------------
echo "Step 6: Comparing accounts..."

# Set up timestamping for logs
if command -v ts > /dev/null 2>&1; then
    timestamper="ts '%Y-%m-%d %H:%M:%.S'"
elif command -v tai64nlocal > /dev/null 2>&1; then
    timestamper="tai64n | tai64nlocal"
else
    timestamper="cat"
fi

psql "$POSTGRES_ACCESS" -v "ON_ERROR_STOP=on" \
    -c "SET SEARCH_PATH TO ${BTRACKER_SCHEMA};" \
    -c "SELECT btracker_test.compare_accounts();" \
    2>&1 | tee -i >(eval "$timestamper" > "$SCRIPT_DIR/regression_test.log")
echo ""

# -----------------------------------------------------------------------------
# Step 7: Check results
# -----------------------------------------------------------------------------
echo "Step 7: Checking results..."

DIFFERING_ACCOUNTS=$(psql "$POSTGRES_ACCESS" -v "ON_ERROR_STOP=on" -t -A \
    -c "SELECT COUNT(*) FROM btracker_test.differing_accounts;")

if [ "$DIFFERING_ACCOUNTS" -eq 0 ]; then
    echo ""
    echo "=============================================="
    echo "SUCCESS: All account balances match!"
    echo "=============================================="
    exit 0
else
    echo ""
    echo "=============================================="
    echo "FAILURE: $DIFFERING_ACCOUNTS accounts have discrepancies"
    echo "=============================================="
    echo ""
    echo "Differing account IDs:"
    psql "$POSTGRES_ACCESS" -v "ON_ERROR_STOP=on" \
        -c "SELECT * FROM btracker_test.differing_accounts LIMIT 20;"
    echo ""
    echo "Use btracker_test.get_account_comparison(account_id) to debug."
    exit 1
fi

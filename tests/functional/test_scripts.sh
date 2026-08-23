#! /bin/bash


set -euo pipefail

SCRIPTDIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

POSTGRES_HOST="localhost"
script_dir="$SCRIPTDIR/../../scripts"

print_help () {
    echo "Usage: $0 [OPTION[=VALUE]]..."
    echo
    echo "Allows to start a balance tracker test (5m blocks)."
    echo "balance tracker must be stopped on 5m blocks (add flag to ./process_blocks.sh --stop-at-block=5000000)"
    echo "OPTIONS:"
    echo "  --host=VALUE             Allows to specify a PostgreSQL host location (defaults to localhost)"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --host=*)
        POSTGRES_HOST="${1#*=}"
        ;;
    --help)
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


pushd "$script_dir/.."

echo "Test 1. Generate version..."
./scripts/generate_version_sql.sh  "$script_dir/.."
echo "Generate version completed successfully"

popd

pushd "$script_dir"

echo "Test 2. Reinstall app..."
./install_app.sh --postgres-host="$POSTGRES_HOST"
echo "Reinstall completed successfully"

echo "Test 3. impacted_balances SQL-vs-C parity..."
# Asserts btracker_backend.get_impacted_balances (SQL/JSONB) matches the HAF C function
# hive.get_impacted_balances over curated fixtures. ON_ERROR_STOP makes a RAISE in the
# test exit non-zero, failing this job (script runs under `set -e`).
psql "postgresql://haf_admin@$POSTGRES_HOST:5432/haf_block_log" -v ON_ERROR_STOP=on -f "$script_dir/../tests/parity/impacted_balances_parity.sql"
psql "postgresql://haf_admin@$POSTGRES_HOST:5432/haf_block_log" -v ON_ERROR_STOP=on -f "$script_dir/../tests/parity/impacted_balances_batch_parity.sql"
echo "Parity test completed successfully"

echo "Test 4. Uninstall app..."
./uninstall_app.sh --host="$POSTGRES_HOST"
echo "Uninstall app completed successfully"

popd

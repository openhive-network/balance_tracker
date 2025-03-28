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

echo "Test 3. Uninstall app..."
./uninstall_app.sh --host="$POSTGRES_HOST"
echo "Uninstall app completed successfully"

popd

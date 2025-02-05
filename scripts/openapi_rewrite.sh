#!/bin/bash

set -e
set -o pipefail

SCRIPTDIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 || exit 1; pwd -P )"

haf_dir="${SCRIPTDIR}/../haf"
endpoints="${SCRIPTDIR}/../openapi-gen-input/endpoints"

DEPLOY="${SCRIPTDIR}/../"
OUTPUT="$SCRIPTDIR/output"

# Default directories with fixed order if none provided

ENDPOINTS_IN_ORDER=(
"$endpoints/endpoint_schema.sql"
"$endpoints/types/coin_type.sql"
"$endpoints/types/sort_direction.sql"
"$endpoints/account-balances/get_account_balances.sql"
"$endpoints/account-balances/get_balance_history.sql"
)

# Function to install pip3
install_pip() {
    echo "pip3 is not installed. Installing now..."
    # Ensure Python 3 is installed
    if ! command -v python3 &> /dev/null; then
        echo "Python 3 is not installed. Please install Python 3 first."
        exit 1
    fi
    # Try to install pip3
    sudo apt-get update
    sudo apt-get install -y python3-pip
    if ! command -v pip3 &> /dev/null; then
        echo "pip3 installation failed. Please install pip3 manually."
        exit 1
    fi
}

# Check if pip3 is installed
if ! command -v pip3 &> /dev/null; then
    install_pip
fi

# Check if deepmerge is installed
if python3 -c "import deepmerge" &> /dev/null; then
    echo "deepmerge is already installed."
else
    echo "deepmerge is not installed. Installing now..."
    pip3 install deepmerge
    echo "deepmerge has been installed."
fi

# Check if jsonpointer is installed
if python3 -c "import jsonpointer" &> /dev/null; then
    echo "jsonpointer is already installed."
else
    echo "jsonpointer is not installed. Installing now..."
    pip3 install jsonpointer
    echo "jsonpointer has been installed."
fi

echo "Using endpoints sources:"
echo "${ENDPOINTS_IN_ORDER[@]}"

rm -rfv "${OUTPUT}"

pushd "${SCRIPTDIR}"

# run openapi rewrite script
# shellcheck disable=SC2086
python3 "${haf_dir}/scripts/process_openapi.py" "$@" "${OUTPUT}" "${ENDPOINTS_IN_ORDER[@]}"

echo "Rewritten endpoint scripts and rewrite_rules.conf file saved in ${OUTPUT}"
echo "Copying generated sources from ${OUTPUT}/openapi-gen-input/ into expected location by installation scripts: ${DEPLOY}"
cp -vrf "${OUTPUT}"/openapi-gen-input/* "${OUTPUT}"/swagger-doc.json "${DEPLOY}"/

popd

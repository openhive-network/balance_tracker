#!/bin/bash

set -e
set -o pipefail

SCRIPTDIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 || exit 1; pwd -P )"
endpoints_dir="endpoints/"
input_file="rewrite_rules.conf"
temp_output_file=$(mktemp)


cat <<EOF
  Script used to search for all SQL scripts with openapi descriptions...

  Usage: $0 <output_directory> <endpoint_directories>
  
EOF

# Default directories with fixed order if none provided
DEFAULT_OUTPUT="$SCRIPTDIR/output"

DEFAULT_ENDPOINTS="
$endpoints_dir/endpoint_schema.sql

$endpoints_dir/matching-accounts/find_matching_accounts.sql

$endpoints_dir/balance-for-coins/get_balance_for_coin_by_block.sql
$endpoints_dir/balance-for-coins/get_balance_for_coin_by_time.sql

$endpoints_dir/account-balances/get_account_balances.sql
$endpoints_dir/account-balances/get_account_delegations.sql
$endpoints_dir/account-balances/get_account_info_rewards.sql
$endpoints_dir/account-balances/get_account_rewards.sql
$endpoints_dir/account-balances/get_account_savings.sql
$endpoints_dir/account-balances/get_account_withdraws.sql
"

echo "Using default HAF block explorer types and endpoints directories"
echo "$DEFAULT_ENDPOINTS"

# shellcheck disable=SC2086
python3 process_openapi.py $DEFAULT_OUTPUT $DEFAULT_ENDPOINTS

# Function to reverse the lines
reverse_lines() {
    awk '
    BEGIN {
        RS = ""
        FS = "\n"
    }
    {
        for (i = 1; i <= NF; i++) {
            if ($i ~ /^#/) {
                comment = $i
            } else if ($i ~ /^rewrite/) {
                rewrite = $i
            }
        }
        if (NR > 1) {
            print ""
        }
        print comment
        print rewrite
    }' "$input_file" | tac
}

reverse_lines > "$temp_output_file"
mv "$temp_output_file" "$input_file"

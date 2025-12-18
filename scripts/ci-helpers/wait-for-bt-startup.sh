#!/bin/bash

set -e

function print_help () {
cat <<-EOF
Usage: $0 [OPTION[=VALUE]]...

Script that waits for Balance Tracker to process 5'000'000 blocks.
To be used in CI.
OPTIONS:
    --postgress-access=URL    PostgreSQL URL
    --help|-h|-?              Display this help screen and exit
EOF
}

function wait-for-bt-startup() {
    if command -v psql &> /dev/null
    then
        until psql "$POSTGRES_ACCESS" --quiet --tuples-only --command="$COMMAND" | grep 1 &>/dev/null
        do 
            echo "$MESSAGE"
            sleep 20
        done
    else
        echo "Please install psql before running this script."
        exit 1
    fi
}

#shellcheck disable=SC2089
# Check both conditions:
# 1. btracker_app is in sync with HAF
# 2. btracker has actually processed data (has balance records)
# The second check is critical for skip-hived mode where the app starts at HAF head
# and would immediately appear "in sync" without processing any blocks.
COMMAND="SELECT (hive.is_app_in_sync('btracker_app') AND EXISTS(SELECT 1 FROM btracker_app.current_account_balances LIMIT 1))::INT;"
MESSAGE="Waiting for Balance Tracker to finish processing blocks..."

while [ $# -gt 0 ]; do
  case "$1" in
    --postgress-access=*)
        arg="${1#*=}"
        POSTGRES_ACCESS="$arg"
        ;;
    --help|-h|-\?)
        print_help
        exit 0
        ;;
    *)
        echo "ERROR: '$1' is not a valid option/positional argument"
        echo
        print_help
        exit 2
        ;;
  esac
  shift
done

POSTGRES_ACCESS=${POSTGRES_ACCESS:-postgresql://haf_admin@localhost:5432/haf_block_log}

export -f wait-for-bt-startup
export POSTGRES_ACCESS
#shellcheck disable=SC2090
export COMMAND
export MESSAGE

# HAF replay of 5M blocks takes ~50 minutes, plus Balance Tracker sync time
# Total timeout: 90 minutes (60m replay + 30m sync buffer)
timeout -k 1m 90m bash -c wait-for-bt-startup
      
echo "Block processing is finished."
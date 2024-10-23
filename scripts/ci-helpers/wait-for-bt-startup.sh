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
        until psql "$POSTGRES_ACCESS" --quiet --tuples-only --command="$COMMAND" | grep 0 &>/dev/null
        do 
            echo "$MESSAGE"
            sleep 3
        done
    else
        echo "Please install psql before running this script."
        exit 1
    fi
}

#shellcheck disable=SC2089
COMMAND="SELECT CASE WHEN irreversible_block = 5000000 THEN 0 ELSE 1 END FROM hafd.contexts WHERE name = 'btracker_app';"
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

timeout -k 1m 10m bash -c wait-for-bt-startup
      
echo "Block processing is finished."
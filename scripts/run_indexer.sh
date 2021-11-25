#!/bin/bash

set -e
set -o pipefail 

if [[ $# -ge 2 ]] && [[ $@ =~ "start" ]]; then
    ARGS=${@:3}
else
    ARGS=${@:2}
fi;

psql -a -v "ON_ERROR_STOP=1" "${ARGS}" -d haf_block_log -c '\timing' -c "call btracker_app.main('btracker_app', $1);"

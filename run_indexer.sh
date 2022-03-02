#!/bin/bash

set -e
set -o pipefail 

psql -a -v "ON_ERROR_STOP=1" "$@" -d haf_block_log -c '\timing' -c "call btracker_app.main('btracker_app', 5000000);"


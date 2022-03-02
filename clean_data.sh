#!/bin/bash

set -e
set -o pipefail

psql -a -v "ON_ERROR_STOP=1" "$@" -d haf_block_log -c '\timing' -c "do \$\$ BEGIN if hive.app_context_exists('btracker_app') THEN perform hive.app_remove_context('btracker_app'); end if; END \$\$"

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
echo $SCRIPT_DIR
psql -a -v "ON_ERROR_STOP=1" "$@" -d haf_block_log -f $SCRIPT_DIR/db/btracker_app.sql


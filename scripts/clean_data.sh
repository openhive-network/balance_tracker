#!/bin/bash

set -e
set -o pipefail

psql -a -v "ON_ERROR_STOP=1" "$@" -d haf_block_log -c '\timing' -c "do \$\$ BEGIN if hive.app_context_exists('btracker_app') THEN perform hive.app_remove_context('btracker_app'); end if; END \$\$"

psql -a -v "ON_ERROR_STOP=1" "$@" -d haf_block_log -f $PWD/db/btracker_app.sql
#! /bin/bash

# credits: https://gitlab.syncad.com/hive/HAfAH/-/blob/master/scripts/generate_version_sql.bash
# usage: ./generate_version_sql.sh root_project_dir
# example: ./generate_version_sql.sh $PWD
# example: ./generate_version_sql.sh /var/my/sources
# example: ./generate_version_sql.sh /sources/submodules/hafbe /sources/.git/modules/submodules/hafbe

set -euo pipefail

SCRIPT_DIR="$(dirname "${BASH_SOURCE[0]}")"
PATH_TO_SQL_VERSION_FILE="$SCRIPT_DIR/set_version_in_sql.pgsql"
GIT_WORK_TREE=$1
GIT_DIR=${2:-"$1/.git"}


# Use $() instead of legacy backtick notation.
# Rationale:  https://www.shellcheck.net/wiki/SC2006
GIT_HASH=$(git --git-dir="$GIT_DIR" --work-tree="$GIT_WORK_TREE" rev-parse HEAD)

echo "SELECT btracker_app.set_version('$GIT_HASH');" > "$PATH_TO_SQL_VERSION_FILE"
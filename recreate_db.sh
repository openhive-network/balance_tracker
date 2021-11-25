#!/bin/bash

set -e
set -o pipefail 

./scripts/clean_data.sh

re='^[0-9]+$'
if [[ $# -ge 1 ]] && [[ $1 =~ $re ]]; then
    ./scripts/run_indexer.sh $@
else
    ./scripts/run_indexer.sh '5000000'
fi;

if [[ $# -ge 1 ]] && [[ $2 -eq "start" ]]; then
    postgrest webserver.conf
fi;

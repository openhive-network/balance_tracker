#!/bin/bash

set -e
set -o pipefail



# Script reponsible for execution of all actions required to finish configuration of the database holding a HAF database to work correctly with HAfAH.

print_help () {
    echo "Usage: $0 [OPTION[=VALUE]]..."
    echo
    echo "Allows to setup a database already filled by HAF instance, to work with haf_be application."
    echo "OPTIONS:"
    echo "  --c=VALUE            Allows to specify a script command"
    echo "  --host=VALUE         Allows to specify a PostgreSQL host location (defaults to /var/run/postgresql)"
    echo "  --port=NUMBER        Allows to specify a PostgreSQL operating port (defaults to 5432)"
    echo "  --user=VALUE         Allows to specify a PostgreSQL user (defaults to haf_admin)"
    echo "  --help               Display this help screen and exit"
    echo
}

POSTGRES_HOST="/var/run/postgresql"
POSTGRES_PORT=5432
POSTGRES_USER="haf_admin"
COMMAND=""

while [ $# -gt 0 ]; do
  case "$1" in
    --host=*)
        POSTGRES_HOST="${1#*=}"
        ;;
    --port=*)
        POSTGRES_PORT="${1#*=}"
        ;;
    --user=*)
        POSTGRES_USER="${1#*=}"
        ;;
    --c=*)
        COMMAND="${1#*=}"
        ;;
    --help)
        print_help
        exit 0
        ;;
    -*)
        echo "ERROR: '$1' is not a valid option"
        echo
        print_help
        exit 1
        ;;
    *)
        echo "ERROR: '$1' is not a valid argument"
        echo
        print_help
        exit 2
        ;;
    esac
    shift
done

POSTGRES_ACCESS="--host $POSTGRES_HOST --port $POSTGRES_PORT"

clean_data() {
    psql -w $POSTGRES_ACCESS -a -v "ON_ERROR_STOP=1" "$@" -d $DB_NAME -c '\timing' -c "do \$\$ BEGIN if hive.app_context_exists('btracker_app') THEN perform hive.app_remove_context('btracker_app'); end if; END \$\$"
}

create_db() {
    psql -w $POSTGRES_ACCESS -a -v "ON_ERROR_STOP=1" "$@" -d $DB_NAME -f $PWD/db/btracker_app.sql
}

run_indexer() {
    args=("$@")
    re='^[0-9]+$'
    if ! [[ ${args[0]} =~ $re ]]; then
        block_num=$((10**9))
        
        echo 'Running indexer for existing blocks and expecting new blocks...'
        echo 'NOTE: indexes will not be created, balance_tracker server will not be started!'
        echo 'Use ./run.sh create-indexes and ./run.sh start.'
        sleep 3
    else
        block_num=$1
        args=("${@:2}")
    fi
    
    psql -w $POSTGRES_ACCESS -a -v "ON_ERROR_STOP=1" "$args" -d $DB_NAME -c '\timing' -c "call btracker_app.main('btracker_app', $block_num);"
}

create_indexes() {
    psql -w $POSTGRES_ACCESS -a -v "ON_ERROR_STOP=1" "$@" -d $DB_NAME -c '\timing' -c "call btracker_app.create_indexes();"
}

create_api() {
    psql -w $POSTGRES_ACCESS -a -v "ON_ERROR_STOP=1" -d $DB_NAME -f $PWD/api/btracker_api.sql
}

start_webserver() {
    if [ "$1" = "py" ]; then
        ./main.py ${@:2}
    else
        postgrest postgrest.conf
    fi
}

install_dependancies() {
    install_postgrest
    install_jmeter
}

install_postgrest() {
    wget https://github.com/PostgREST/postgrest/releases/download/v8.0.0/postgrest-v8.0.0-linux-x64-static.tar.xz

    POSTGREST=$(find . -name 'postgrest*')
    tar xJf $POSTGREST
    sudo mv 'postgrest' '/usr/local/bin/postgrest'
    rm $POSTGREST
}

install_jmeter() {
    sudo apt-get update -y
    sudo apt-get install openjdk-8-jdk -y

    wget "https://downloads.apache.org//jmeter/binaries/apache-jmeter-${jmeter_v}.zip"

    jmeter_src="apache-jmeter-${jmeter_v}"
    unzip "${jmeter_src}.zip"
    rm "${jmeter_src}.zip"
    sudo mv $jmeter_src "/usr/local/src/${jmeter_src}"

    jmeter="jmeter-${jmeter_v}"
    touch $jmeter
    echo '#!/usr/bin/env bash' >> $jmeter
    echo '' >> $jmeter
    echo "cd '/usr/local/src/apache-jmeter-${jmeter_v}/bin'" >> $jmeter
    echo './jmeter $@' >> $jmeter
    sudo chmod +x $jmeter
    sudo mv $jmeter "/usr/local/bin/${jmeter}"
}

run_tests() {
    ./tests/run_tests.sh $jmeter_v $2
}

recreate_db() {
    clean_data ${@:2}
    create_db ${@:2}
}

restart_all() {
    recreate_db $@
    create_api
}

start_ui() {
    cd gui ; npm start
}

DB_NAME=haf_block_log
admin_role=haf_admin
postgrest_v=9.0.0
jmeter_v=5.4.3

if [ "$COMMAND" = "re-all" ]; then
    restart_all ${@:2}
    echo 'SUCCESS: DB and API recreated'
elif [ "$COMMAND" = "start-processing" ]; then
    run_indexer $@
elif [ "$COMMAND" = "re-all-start" ]; then
    restart_all ${@:2}
    echo 'SUCCESS: DB and API recreated'
    start_webserver
elif [ "$COMMAND" = "re-db" ]; then
    recreate_db ${@:2}
    echo 'SUCCESS: DB recreated'
elif [ "$COMMAND" = "re-db-start" ]; then
    recreate_db ${@:2}
    echo 'SUCCESS: DB recreated'
    start_webserver
elif [ "$COMMAND" = "re-api" ]; then
    create_api
    echo 'SUCCESS: API recreated'
elif [ "$COMMAND" = "re-api-start" ]; then
    create_api
    echo 'SUCCESS: API recreated'
    start_webserver
elif [ "$COMMAND" =  "install-dependancies" ]; then
    install_dependancies
elif [ "$COMMAND" = "install-postgrest" ]; then
    install_postgrest
elif [ "$COMMAND" = "install-jmeter" ]; then
    install_jmeter
elif [ "$COMMAND" = "test" ]; then
    run_tests $jmeter_v
elif [ "$COMMAND" = "test-py" ]; then
    run_tests $jmeter_v "py"
elif [ "$COMMAND" = "start" ]; then
    start_webserver
elif [ "$COMMAND" = "start-py" ]; then
    start_webserver "py" ${@:2}
elif [ "$COMMAND" = "start-ui" ]; then
    start_ui
elif [ "$COMMAND" = "create-indexes" ]; then
    create_indexes ${@:2}
elif [ "$COMMAND" = "continue-processing" ]; then
    run_indexer ${@:2}
else
    echo "job not found"
fi

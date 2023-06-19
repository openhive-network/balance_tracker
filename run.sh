#!/bin/bash

set -e
set -o pipefail

clean_data() {
    echo "Cleaning data..."
    echo "Arguments: $*"
    psql -a -v "ON_ERROR_STOP=1" -d "$@" -c '\timing' -c "do \$\$ BEGIN if hive.app_context_exists('btracker_app') THEN perform hive.app_remove_context('btracker_app'); end if; END \$\$"
}

create_db() {
    echo "Creating database..."
    echo "Arguments: $*"
    psql -a -v "ON_ERROR_STOP=1" "$@" -f "$PWD/db/btracker_app.sql"
}

run_indexer() {
    echo "Running indexer..."
    echo "Arguments: $*"
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
    
    psql -a -v "ON_ERROR_STOP=1" "${args[@]}" -c '\timing' -c "call btracker_app.main('btracker_app', $block_num);"
}

create_indexes() {
    echo "Creating indexes..."
    echo "Arguments: $*"
    psql -a -v "ON_ERROR_STOP=1" "$@" -c '\timing' -c "call btracker_app.create_indexes();"
}

create_api() {
    echo "Creating API..."
    echo "Arguments: $*"
    psql -a -v "ON_ERROR_STOP=1" "$@" -f "$PWD/api/btracker_api.sql"
}

start_webserver() {
    if [ "$1" = "py" ]; then
        echo "Starting Python webserver..."
        echo "Arguments: $*"
        ./main.py "${@:2}"
    else
        echo "Starting PostgREST webserver..."
        echo "Arguments: $*"
        postgrest postgrest.conf
    fi
}

install_dependancies() {
    echo "Installing dependencies..."
    install_postgrest
    install_jmeter
}

install_postgrest() {
    echo "Installing PostgREST..."
    wget https://github.com/PostgREST/postgrest/releases/download/v11.1.0/postgrest-v11.1.0-linux-static-x64.tar.xz -O postgrest.tar.xz

    tar -xJf postgrest.tar.xz
    sudo mv 'postgrest' '/usr/local/bin/postgrest'
    rm postgrest.tar.xz
}

install_jmeter() {
    echo "Installing Jmeter..."
    sudo apt-get update -y
    sudo apt-get install unzip openjdk-8-jdk-headless -y

    wget "https://downloads.apache.org//jmeter/binaries/apache-jmeter-${jmeter_v}.zip" -O jmeter.zip

    unzip "jmeter.zip"
    rm "jmeter.zip"
    sudo mv "apache-jmeter-${jmeter_v}" "/usr/local/src/jmeter-${jmeter_v}"

    jmeter="jmeter-${jmeter_v}"
    {
        echo '#!/usr/bin/env bash'
        echo ''
        echo "cd '/usr/local/src/jmeter-${jmeter_v}/bin'"
        echo './jmeter $@'
    } > $jmeter
    sudo chmod +x $jmeter
    sudo mv $jmeter "/usr/local/bin/${jmeter}"
    sudo ln -sf "/usr/local/bin/${jmeter}" "/usr/local/bin/jmeter"
}

run_tests() {
    echo "Running tests..."
    echo "Arguments: $*"
    ./tests/run_tests.sh $jmeter_v "$2"
}

recreate_db() {
    echo "Recreating database..."
    echo "Arguments: $*"
    clean_data "${@:2}"
    create_db "${@:2}"
    create_indexes "${@:2}"
    run_indexer "$@"
}

restart_all() {
    echo "Restarting everything..."
    echo "Arguments: $*"
    recreate_db "$@"
    create_api "$@"
}

start_ui() {
    echo "Starting GUI..."
    cd gui ; npm start
}

setup_db() {
    echo "Setting up the database..."
    bash "$SCRIPTS_DIR/setup_db.sh" "$@"
}

process_blocks() {
    echo "Processing blocks..."
    echo "Arguments: $*"
    run_indexer "$@"
}

SCRIPTS_DIR=$PWD/scripts

# postgrest_v=9.0.0
jmeter_v=5.4.3

if [ "$1" = "re-all" ]; then
    restart_all "${@:2}"
    echo 'SUCCESS: DB and API recreated'
elif [ "$1" = "re-all-start" ]; then
    restart_all "${@:2}"
    echo 'SUCCESS: DB and API recreated'
    start_webserver
elif [ "$1" = "setup-db" ]; then
    setup_db "${@:2}"
elif [ "$1" = "process-blocks" ]; then
    process_blocks "${@:2}"
elif [ "$1" = "re-db" ]; then
    recreate_db "${@:2}"
    echo 'SUCCESS: DB recreated'
elif [ "$1" = "re-db-start" ]; then
    recreate_db "${@:2}"
    echo 'SUCCESS: DB recreated'
    start_webserver
elif [ "$1" = "re-api" ]; then
    create_api "${@:2}"
    echo 'SUCCESS: API recreated'
elif [ "$1" = "re-api-start" ]; then
    create_api "${@:2}"
    echo 'SUCCESS: API recreated'
    start_webserver
elif [ "$1" =  "install-dependancies" ]; then
    install_dependancies
elif [ "$1" = "install-postgrest" ]; then
    install_postgrest
elif [ "$1" = "install-jmeter" ]; then
    install_jmeter
elif [ "$1" = "test" ]; then
    run_tests $jmeter_v
elif [ "$1" = "test-py" ]; then
    run_tests $jmeter_v "py"
elif [ "$1" = "start" ]; then
    start_webserver
elif [ "$1" = "start-py" ]; then
    start_webserver "py" "${@:2}"
elif [ "$1" = "start-ui" ]; then
    start_ui
elif [ "$1" = "create-indexes" ]; then
    create_indexes "${@:2}"
elif [ "$1" = "continue-processing" ]; then
    run_indexer "${@:2}"
else
    echo "job not found"
fi

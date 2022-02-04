#!/bin/bash

set -e
set -o pipefail

clean_data() {
    psql -a -v "ON_ERROR_STOP=1" "$@" -d haf_block_log -c '\timing' -c "do \$\$ BEGIN if hive.app_context_exists('btracker_app') THEN perform hive.app_remove_context('btracker_app'); end if; END \$\$"
}

create_db() {
    psql -a -v "ON_ERROR_STOP=1" "$@" -d haf_block_log -f $PWD/db/btracker_app.sql
}

run_indexer() {
    psql -a -v "ON_ERROR_STOP=1" "${@:2}" -d haf_block_log -c '\timing' -c "call btracker_app.main('btracker_app', $1);"
}

create_indexes() {
    psql -a -v "ON_ERROR_STOP=1" "$@" -d haf_block_log -c '\timing' -c "call btracker_app.create_indexes();"
}

create_api() {
    psql -a -v "ON_ERROR_STOP=1" -d haf_block_log -f $PWD/api/btracker_api.sql
}

start_webserver() {
    if [ "$1" = "py" ]; then
        ./main.py ${@:2}
    else
        postgrest webserver.conf
    fi
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

    jmeter_v=5.3
    wget "https://downloads.apache.org//jmeter/binaries/apache-jmeter-${jmeter_v}.zip"

    jmeter_src="apache-jmeter-${jmeter_v}"
    unzip "${jmeter_src}.zip"
    rm "${jmeter_src}.zip"
    sudo mv $jmeter_src "/usr/local/src/${jmeter_src}"

    jmeter='jmeter-5.3'
    touch $jmeter
    echo '#!/usr/bin/env bash' >> $jmeter
    echo '' >> $jmeter
    echo 'cd "/usr/local/src/apache-jmeter-5.3/bin"' >> $jmeter
    echo './jmeter $@' >> $jmeter
    sudo chmod +x $jmeter
    sudo mv $jmeter "/usr/local/bin/${jmeter}"
}

run_tests() {
    ./tests/run_tests.sh $1
}

recreate_db() {
    clean_data ${@:2}
    create_db ${@:2}
    run_indexer $@
    create_indexes ${@:2}
}

restart_all() {
    re='^[0-9]+$'
    if ! [[ $1 =~ $re ]]; then
        echo 'ERROR: Second argument must be block number'
        exit 1
    fi

    recreate_db $@
    create_api
}

if [ "$1" = "re-all" ]; then
    restart_all ${@:2}
    echo 'SUCCESS: DB and API recreated'
elif [ "$1" = "re-all-start" ]; then
    restart_all ${@:2}
    echo 'SUCCESS: DB and API recreated'
    start_webserver
elif [ "$1" = "re-db" ]; then
    recreate_db ${@:2}
    echo 'SUCCESS: DB recreated'
elif [ "$1" = "re-db-start" ]; then
    recreate_db ${@:2}
    echo 'SUCCESS: DB recreated'
    start_webserver
elif [ "$1" = "re-api" ]; then
    create_api
    echo 'SUCCESS: API recreated'
elif [ "$1" = "re-api-start" ]; then
    create_api
    echo 'SUCCESS: API recreated'
    start_webserver
elif [ "$1" = "install-postgrest" ]; then
    install_postgrest
elif [ "$1" = "install-jmeter" ]; then
    install_jmeter
elif [ "$1" = "test" ]; then
    run_tests
elif [ "$1" = "test-py" ]; then
    run_tests "py"
elif [ "$1" = "start" ]; then
    start_webserver
elif [ "$1" = "start-py" ]; then
    start_webserver "py" ${@:2}
fi

#!/bin/bash

set -e

function print_help () {
cat <<-EOF
Usage: $0 [OPTION[=VALUE]]...

Script that starts test environment for use with CI test jobs.
To start a local environment use Docker Compose directly. See docker/README.md for details.
OPTIONS:
    --backend-version=VERSION   HAF BE version (default: latest)
    --haf-data-directory=PATH   HAF Data directory path (default: /srv/haf/data)
    --haf-shm-directory=PATH    HAF SHM directory path (default: /srv/haf/shm)
    --haf-registry=REGISTRY     HAF registry to use (default: registry.gitlab.syncad.com/hive/haf/instance)
    --haf-version=VERSION       HAF version to use (default: 9ec94375)
    --hived-uid=UID             UID that hived daemon should be running as (default: $(id -u))
    --help|-h|-?                Display this help screen and exit
EOF
}

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

while [ $# -gt 0 ]; do
  case "$1" in
    --backend-version=*)
        arg="${1#*=}"
        BACKEND_VERSION="$arg"
        ;;
    --haf-data-directory=*)
        arg="${1#*=}"
        HAF_DATA_DIRECTORY="$arg"
        ;;
    --haf-shm-directory=*)
        arg="${1#*=}"
        HAF_SHM_DIRECTORY="$arg"
        ;;
    --haf-registry=*)
        arg="${1#*=}"
        HAF_REGISTRY_PATH="$arg"
        ;;
    --hived-uid=*)
        arg="${1#*=}"
        HIVED_UID="$arg"
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

CI_PROJECT_DIR=${CI_PROJECT_DIR:-$SCRIPTPATH/../..}
COMPOSE_OPTIONS_STRING=${COMPOSE_OPTIONS_STRING:-"--env-file ci.env --file docker-compose.yml --file overrides/dev.yml"}

pushd "${CI_PROJECT_DIR}/docker"

cat <<-EOF | tee ci.env
    BACKEND_VERSION=${BACKEND_VERSION:-latest}
    HAF_COMMAND=--shared-file-size=1G --plugin database_api --replay --stop-at-block=5000000
    HAF_DATA_DIRECTORY=${HAF_DATA_DIRECTORY:-/srv/haf/data}
    HAF_SHM_DIRECTORY=${HAF_SHM_DIRECTORY:-/srv/haf/shm}
    HAF_REGISTRY=${HAF_REGISTRY_PATH:-registry.gitlab.syncad.com/hive/haf/instance}
    HAF_VERSION=${HAF_REGISTRY_TAG:-9ec94375}
    HIVED_UID=${HIVED_UID:-$(id -u)}
    PGHERO_USERNAME=link
    PGHERO_PASSWORD=hyrule
    PGADMIN_DEFAULT_EMAIL=admin@hafblockexplorer.internal
    PGADMIN_DEFAULT_PASSWORD=admin
EOF

echo "Docker Compose options string: $COMPOSE_OPTIONS_STRING"
IFS=" " read -ra COMPOSE_OPTIONS <<< "$COMPOSE_OPTIONS_STRING"

echo "Docker Compose options: ${COMPOSE_OPTIONS[*]}"
docker compose "${COMPOSE_OPTIONS[@]}" config | tee docker-compose-config.yml.log
timeout -s INT -k 1m 15m docker compose "${COMPOSE_OPTIONS[@]}" up --detach --quiet-pull

popd
#! /bin/bash

set -e

print_help () {
    cat <<-EOF
Usage: $0 <source directory> [OPTION[=VALUE]]...

Build any Docker image defined as a target in docker-bake.hcl
OPTIONS:
  --registry=URL        Docker registry to assign the image to (default: 'registry.gitlab.syncad.com/hive/balance_tracker')
  --tag=TAG             Docker tag to be build (defaults set in docker-bake.hcl)
  --target=TARGET       Either 'ci-runner' or 'ci-runner-ci' (default: 'ci-runner')
  --progress=TYPE       Determines how to display build progress (default: 'auto')
  --help|-h|-?          Display this help screen and exit
EOF
}

export CI_REGISTRY_IMAGE=${CI_REGISTRY_IMAGE:-"registry.gitlab.syncad.com/hive/balance_tracker"}
PROGRESS_DISPLAY=${PROGRESS_DISPLAY:-"auto"}
TARGET=${TARGET:-"ci-runner"}

while [ $# -gt 0 ]; do
  case "$1" in
    --registry=*)
        arg="${1#*=}"
        CI_REGISTRY_IMAGE="$arg"
        ;;
    --tag=*)
        arg="${1#*=}"
        BASE_TAG="$arg"
        ;;    
    --progress=*)
        arg="${1#*=}"
        PROGRESS_DISPLAY="$arg"
        ;;
    --target=*)
        arg="${1#*=}"
        TARGET="$arg"
        ;;
    --help|-h|-\?)
        print_help
        exit 0
        ;;
    *)
        if [ -z "$SRCROOTDIR" ];
        then
          SRCROOTDIR="${1}"
        else
          echo "ERROR: '$1' is not a valid option/positional argument"
          echo
          print_help
          exit 2
        fi
        ;;
    esac
    shift
done

if [[ -n $BASE_TAG ]]; then
    export TAG_CI=$BASE_TAG
fi

pushd "$SRCROOTDIR"

docker buildx bake --provenance=false --progress="$PROGRESS_DISPLAY" "$TARGET"

popd
#! /bin/bash

set -e

print_help () {
cat <<-EOF
Usage: $0 <image_tag> <src_dir> <registry_url> [OPTION[=VALUE]]...

A wrapper script for building Balance Tracker Docker image for publishing
OPTIONS:
    --help|-h|-?              Display this help screen and exit
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --help|-h|-?)
      print_help
      exit 0
      ;;
    *)
      if [ -z "$BUILD_IMAGE_TAG" ];
      then
        BUILD_IMAGE_TAG="${1}"
      elif [ -z "$SRCROOTDIR" ];
      then
        SRCROOTDIR="${1}"
      elif [ -z "$REGISTRY" ];
      then
        REGISTRY=${1}
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

_TST_IMGTAG=${BUILD_IMAGE_TAG:?"Missing argument #1 - image tag to be built"}
_TST_SRCDIR=${SRCROOTDIR:?"Missing arg #2 - source directory"}
_TST_REGISTRY=${REGISTRY:?"Missing arg #3 - container registry URL"}

pushd "$SRCROOTDIR"

./scripts/generate_version_sql.sh "$(pwd)"

# On CI build the image using the registry-stored BuildKit cache 
# and push it to registry immediately.
# Locally, build it using local BuildKit cache and load it to the 
# local image store.
if [[ -n ${CI:-} ]]; then
    TARGET="full-ci"
else
    TARGET="full"
fi

scripts/build_docker_image.sh "$SRCROOTDIR" --registry="$REGISTRY" --target="$TARGET" --tag="$BUILD_IMAGE_TAG"

# On CI pull the image form the registry since it's pushed directly to the registry after build
if [[ -n ${CI:-} ]]; then
  docker pull "$REGISTRY:$BUILD_IMAGE_TAG"
fi

docker tag "$REGISTRY:$BUILD_IMAGE_TAG" "$REGISTRY/instance:$BUILD_IMAGE_TAG"
docker tag "$REGISTRY:$BUILD_IMAGE_TAG" "$REGISTRY/minimal-instance:$BUILD_IMAGE_TAG"

popd
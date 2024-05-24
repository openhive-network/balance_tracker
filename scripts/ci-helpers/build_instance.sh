#! /bin/bash

set -e

print_help () {
cat <<-EOF
Usage: $0 <image_tag> <src_dir> <registry_url> [OPTION[=VALUE]]...

A wrapper script for building Balance Tracker Docker image
OPTIONS:
    --progress=TYPE       Determines how to display build progress (default: 'auto')
    --help|-h|-?          Display this help screen and exit
EOF
}

PROGRESS_DISPLAY=${PROGRESS_DISPLAY:-"auto"}

while [ $# -gt 0 ]; do
  case "$1" in
    --progress=*)
      arg="${1#*=}"
      PROGRESS_DISPLAY="$arg"
      ;;
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

# On CI build the image using the registry-stored BuildKit cache 
# and push it to registry immediately.
# Locally, build it using local BuildKit cache and load it to the 
# local image store.
if [[ -n ${CI:-} ]]; then
    TARGET="full-ci"
else
    TARGET="full"
fi

pushd "$SRCROOTDIR"

# All the variables below must be declared and assigned separately
# for 'set -e' to work correctly. See https://www.shellcheck.net/wiki/SC2155
# for an explanation

BUILD_TIME="$(date -uIseconds)"
export BUILD_TIME

GIT_COMMIT_SHA="$(git rev-parse HEAD || true)"
if [ -z "$GIT_COMMIT_SHA" ]; then
  GIT_COMMIT_SHA="[unknown]"
fi
export GIT_COMMIT_SHA

GIT_CURRENT_BRANCH="$(git branch --show-current || true)"
if [ -z "$GIT_CURRENT_BRANCH" ]; then
  GIT_CURRENT_BRANCH="$(git describe --abbrev=0 --all | sed 's/^.*\///' || true)"
  if [ -z "$GIT_CURRENT_BRANCH" ]; then
    GIT_CURRENT_BRANCH="[unknown]"
  fi
fi
export GIT_CURRENT_BRANCH

GIT_LAST_LOG_MESSAGE="$(git log -1 --pretty=%B || true)"
if [ -z "$GIT_LAST_LOG_MESSAGE" ]; then
  GIT_LAST_LOG_MESSAGE="[unknown]"
fi
export GIT_LAST_LOG_MESSAGE

GIT_LAST_COMMITTER="$(git log -1 --pretty="%an <%ae>" || true)"
if [ -z "$GIT_LAST_COMMITTER" ]; then
  GIT_LAST_COMMITTER="[unknown]"
fi
export GIT_LAST_COMMITTER

GIT_LAST_COMMIT_DATE="$(git log -1 --pretty="%aI" || true)"
if [ -z "$GIT_LAST_COMMIT_DATE" ]; then
  GIT_LAST_COMMIT_DATE="[unknown]"
fi
export GIT_LAST_COMMIT_DATE

docker buildx bake --provenance=false --progress="$PROGRESS_DISPLAY" "$TARGET"

popd

# On CI pull the image form the registry since it's pushed directly to the registry after build
if [[ -n ${CI:-} ]]; then
  docker pull "$REGISTRY:$BUILD_IMAGE_TAG"
fi

docker tag "$REGISTRY:$BUILD_IMAGE_TAG" "$REGISTRY/instance:$BUILD_IMAGE_TAG"
docker tag "$REGISTRY:$BUILD_IMAGE_TAG" "$REGISTRY/minimal-instance:$BUILD_IMAGE_TAG"

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
  GIT_CURRENT_BRANCH="$(git describe --abbrev=0 --all --exclude 'pipelines/*' | sed 's/^.*\///' || true)"
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

# Build main image via docker-bake (handles tagging: short SHA + latest on develop + version on tags)
docker buildx bake --provenance=false --progress="$PROGRESS_DISPLAY" "$TARGET"

# Build rewriter image tags
REWRITER_TAGS="--tag $REGISTRY/postgrest-rewriter:$BUILD_IMAGE_TAG"

# Tag with 'latest' on develop branch
if [[ "${CI_COMMIT_BRANCH:-}" == "${CI_DEFAULT_BRANCH:-develop}" ]]; then
  REWRITER_TAGS="$REWRITER_TAGS --tag $REGISTRY/postgrest-rewriter:latest"
fi

# Determine rewriter target and tag with version on protected tags
REWRITER_TARGET="without_tag"
TAG_BUILD_ARGS=""
if [[ -n "${CI_COMMIT_TAG:-}" ]]; then
  REWRITER_TARGET="with_tag"
  TAG_BUILD_ARGS="--build-arg GIT_COMMIT_TAG=$CI_COMMIT_TAG"
  REWRITER_TAGS="$REWRITER_TAGS --tag $REGISTRY/postgrest-rewriter:$CI_COMMIT_TAG"
fi

# Build and push the rewriter image
# shellcheck disable=SC2086
docker buildx build \
    --build-arg BUILD_TIME="$BUILD_TIME" \
    --build-arg GIT_COMMIT_SHA="$GIT_COMMIT_SHA" \
    --build-arg GIT_CURRENT_BRANCH="$GIT_CURRENT_BRANCH" \
    --build-arg GIT_LAST_LOG_MESSAGE="$GIT_LAST_LOG_MESSAGE" \
    --build-arg GIT_LAST_COMMITTER="$GIT_LAST_COMMITTER" \
    --build-arg GIT_LAST_COMMIT_DATE="$GIT_LAST_COMMIT_DATE" \
    --target=$REWRITER_TARGET \
    $TAG_BUILD_ARGS \
    $REWRITER_TAGS \
    --push \
    --file Dockerfile.rewriter .

popd

# On CI pull the image from the registry since it's pushed directly to the registry after build
if [[ -n ${CI:-} ]]; then
  docker pull "$REGISTRY:$BUILD_IMAGE_TAG"
fi

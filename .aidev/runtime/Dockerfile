# The image AIDEV runs this project's checks in. Checks run with the caller's uid
# and, for the slots that reach the shared HAF, on the per-run network its relay is
# on; every tool is in the image, nothing is installed at run time.
#
# It follows hive/common-ci-configuration's haf-app-test-runner (`environment.base`
# in .aidev/project.yaml): AIDEV resolves the tag and builds this file on it with
# --build-arg BASE_IMAGE=<repository>@<digest>, so the tool versions are ccc's
# (docker-bake.hcl) — bash, psql, shellcheck, postgrest, tavern, pytest,
# pytest-xdist and psycopg2 all come from the base, and none is pinned here.
# This file adds only what the base lacks:
#
#   + hive/tests_api (validate_response, the tavern tests' comparator) at
#     TESTS_API_COMMIT, the commit of the tests_api submodule
#   + haf_app_driver.py, the generic HAF block-processing driver that
#     scripts/process_blocks.sh runs in production images, from hive/common-ci-configuration
#     at DRIVER_COMMIT (checksum-pinned)
#
# The default BASE_IMAGE is for a build by hand (.aidev/runtime/build.sh); see .aidev/README.md.
ARG BASE_IMAGE=registry.gitlab.syncad.com/hive/common-ci-configuration/haf-app-test-runner:develop
FROM ${BASE_IMAGE}

ARG TESTS_API_COMMIT=455898cc3462d4e5ee2274756e5e3d44688fe204
ARG DRIVER_COMMIT=3839feb0c30846aa3ae262143f216a3c1a0fa20b

# The base's USER (hived) owns the tools venv, so this installs next to the tools.
RUN pip install --no-cache-dir \
        "https://gitlab.syncad.com/hive/tests_api/-/archive/${TESTS_API_COMMIT}/tests_api-${TESTS_API_COMMIT}.tar.gz"

ADD --chmod=755 \
    --checksum=sha256:55f82b1c0d024a02149ba2d0157d3b7394773631514d5117ebaa3446555dc8d7 \
    https://gitlab.syncad.com/hive/common-ci-configuration/-/raw/${DRIVER_COMMIT}/haf-app-tools/scripts/haf_app_driver.py \
    /usr/local/bin/haf_app_driver.py

ENV HOME=/tmp
ENTRYPOINT []
CMD ["bash"]

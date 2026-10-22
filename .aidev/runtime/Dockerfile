# The image AIDEV runs this project's checks in (`environment.image` in
# .aidev/project.yaml). Checks run with the caller's uid and, for the slots that
# reach the shared HAF, on the per-run network its relay is on; every tool is in
# the image, nothing is installed at run time:
#
#   python:3.14-alpine (hive/tests_api requires Python >= 3.14)
#   + the postgrest binary of postgrest:v12.0.2
#   + bash, psql, shellcheck from Alpine's repository
#   + tavern, pytest and pytest-xdist, and hive/tests_api (validate_response, the tavern
#     tests' comparator) at TESTS_API_COMMIT, the commit of the tests_api submodule
#   + psycopg2 (tests/regression/load_expected_balances.py and the block-processing driver)
#   + haf_app_driver.py, the generic HAF block-processing driver that
#     scripts/process_blocks.sh runs in production images, from hive/common-ci-configuration
#     at DRIVER_COMMIT (checksum-pinned)
#
# Build and pin it with .aidev/runtime/build.sh; see .aidev/README.md.
FROM registry.gitlab.syncad.com/hive/common-ci-configuration/postgrest@sha256:928a9478f8a4b38dab3448846bf40da3f2075ae770c3f94fb9760ff689aa6561 AS postgrest

FROM python:3.14-alpine@sha256:601e99e090cb08c2784a42395487709500bc39930307fe72f3f11a8f3c430762

ARG TESTS_API_COMMIT=455898cc3462d4e5ee2274756e5e3d44688fe204
ARG DRIVER_COMMIT=3839feb0c30846aa3ae262143f216a3c1a0fa20b

RUN apk add --no-cache bash postgresql-client shellcheck

RUN pip install --no-cache-dir --root-user-action=ignore \
        tavern==3.7.0 \
        pytest==9.1.1 \
        pytest-xdist==3.8.0 \
        psycopg2-binary==2.9.13 \
        "https://gitlab.syncad.com/hive/tests_api/-/archive/${TESTS_API_COMMIT}/tests_api-${TESTS_API_COMMIT}.tar.gz"

ADD --chmod=755 \
    --checksum=sha256:55f82b1c0d024a02149ba2d0157d3b7394773631514d5117ebaa3446555dc8d7 \
    https://gitlab.syncad.com/hive/common-ci-configuration/-/raw/${DRIVER_COMMIT}/haf-app-tools/scripts/haf_app_driver.py \
    /usr/local/bin/haf_app_driver.py

COPY --from=postgrest /bin/postgrest /usr/local/bin/postgrest

ENV HOME=/tmp
ENTRYPOINT []
CMD ["bash"]

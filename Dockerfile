# syntax=registry.gitlab.syncad.com/hive/common-ci-configuration/dockerfile:1.5
# Pinned to the c-c-c develop SHA tag that introduces python3 + py3-psycopg2
# + /usr/local/bin/install_with_app_lock.py (the wrapper used by install_app.sh).
# Bump when c-c-c publishes a new semver tag that includes the wrapper.
ARG PSQL_CLIENT_VERSION=3f9ddace955ad0c45a8364fc4ebf880376999262
FROM registry.gitlab.syncad.com/hive/common-ci-configuration/psql:${PSQL_CLIENT_VERSION} AS psql

FROM psql as version-calculcation
ARG API_VERSION="0.0.0-dev"
USER root
# Replace haf_admin (UID 1000 in base image) with hived
RUN deluser haf_admin 2>/dev/null || true && adduser -D -u 1000 -G users -h /home/hived hived
USER hived

COPY --chown=hived:users . /home/hived/src
WORKDIR /home/hived/src
RUN scripts/generate_version_sql.sh $(pwd)
RUN sed -i 's|"version": "[^"]*"|"version": "'"$API_VERSION"'"|' endpoints/endpoint_schema.sql \
    && sed -i 's|^  version: .*|  version: '"$API_VERSION"'|' endpoints/endpoint_schema.sql

FROM psql AS full

ARG BUILD_TIME
ARG GIT_COMMIT_SHA
ARG GIT_CURRENT_BRANCH
ARG GIT_LAST_LOG_MESSAGE
ARG GIT_LAST_COMMITTER
ARG GIT_LAST_COMMIT_DATE
LABEL org.opencontainers.image.created="$BUILD_TIME"
LABEL org.opencontainers.image.url="https://hive.io/"
LABEL org.opencontainers.image.documentation="https://gitlab.syncad.com/hive/balance_tracker"
LABEL org.opencontainers.image.source="https://gitlab.syncad.com/hive/balance_tracker"
#LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.revision="$GIT_COMMIT_SHA"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.ref.name="Balance Tracker"
LABEL org.opencontainers.image.title="Balance Tracker Image"
LABEL org.opencontainers.image.description="Runs Balance Tracker application"
LABEL io.hive.image.branch="$GIT_CURRENT_BRANCH"
LABEL io.hive.image.commit.log_message="$GIT_LAST_LOG_MESSAGE"
LABEL io.hive.image.commit.author="$GIT_LAST_COMMITTER"
LABEL io.hive.image.commit.date="$GIT_LAST_COMMIT_DATE"

USER root

RUN <<EOF
  set -e
  deluser haf_admin 2>/dev/null || true
  adduser -D -u 1000 -G users -h /home/hived hived 2>/dev/null || true
  mkdir /app
  chown hived /app
EOF

USER hived

COPY scripts/install_app.sh /app/scripts/install_app.sh
COPY scripts/uninstall_app.sh /app/scripts/uninstall_app.sh
COPY scripts/process_blocks.sh /app/scripts/process_blocks.sh
COPY db /app/db
COPY backend /app/backend
COPY --from=version-calculcation /home/hived/src/endpoints /app/endpoints
COPY tests/mocks /app/tests/mocks
COPY docker/scripts/block-processing-healthcheck.sh /app/block-processing-healthcheck.sh
COPY docker/scripts/docker_entrypoint.sh /app/docker_entrypoint.sh
COPY --from=version-calculcation --chown=hived:users /home/hived/src/scripts/set_version_in_sql.pgsql /app/scripts/set_version_in_sql.pgsql

ENTRYPOINT ["/app/docker_entrypoint.sh"]
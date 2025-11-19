# syntax=registry.gitlab.syncad.com/hive/common-ci-configuration/dockerfile:1.5
ARG PSQL_CLIENT_VERSION=14-1
FROM registry.gitlab.syncad.com/hive/common-ci-configuration/psql:${PSQL_CLIENT_VERSION} AS psql

# Get Python from official Debian slim image (compatible with glibc-based systems)
FROM python:3.11-slim AS python-base

FROM psql as version-calculcation

COPY --chown=haf_admin:users . /home/haf_admin/src
WORKDIR /home/haf_admin/src
RUN scripts/generate_version_sql.sh $(pwd)

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

# Copy Python from python-base stage
COPY --from=python-base /usr/local /usr/local

RUN <<EOF
  set -e
  mkdir /app
  chown haf_admin /app
  python3 -m pip install --no-cache-dir psycopg2-binary
EOF

USER haf_admin

COPY scripts/install_app.sh /app/scripts/install_app.sh
COPY scripts/uninstall_app.sh /app/scripts/uninstall_app.sh
COPY scripts/process_blocks.sh /app/scripts/process_blocks.sh
COPY scripts/add_mocks_to_db.sh /app/scripts/add_mocks_to_db.sh
COPY process_blocks.py /app/process_blocks.py
COPY db /app/db
COPY backend /app/backend
COPY endpoints /app/endpoints
COPY dump_accounts /app/dump_accounts
COPY mock_data /app/mock_data
COPY balance-tracker.sh /app/balance-tracker.sh
COPY docker/scripts/block-processing-healthcheck.sh /app/block-processing-healthcheck.sh
COPY docker/scripts/docker_entrypoint.sh /app/docker_entrypoint.sh
COPY --from=version-calculcation --chown=haf_admin:users /home/haf_admin/src/scripts/set_version_in_sql.pgsql /app/scripts/set_version_in_sql.pgsql

ENTRYPOINT ["/app/docker_entrypoint.sh"]
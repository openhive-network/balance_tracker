# syntax=docker/dockerfile:1.5
ARG PSQL_CLIENT_VERSION=14-1
FROM registry.gitlab.syncad.com/hive/common-ci-configuration/psql:${PSQL_CLIENT_VERSION} AS psql

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

RUN apk add --no-cache curl \
    && curl -sSL https://packagecloud.io/timescale/timescaledb/gpgkey | apk add --no-cache --allow-untrusted - \
    && echo "https://packagecloud.io/timescale/timescaledb/ubuntu/alpine/main" >> /etc/apk/repositories \
    && apk update \
    && apk add --no-cache timescaledb-2-postgresql-17

RUN <<EOF
  set -e
  mkdir /app
  chown haf_admin /app
EOF

USER haf_admin

COPY scripts/install_app.sh /app/scripts/install_app.sh
COPY scripts/uninstall_app.sh /app/scripts/uninstall_app.sh
COPY scripts/process_blocks.sh /app/scripts/process_blocks.sh
COPY db /app/db
COPY backend /app/backend
COPY endpoints /app/endpoints
COPY dump_accounts /app/dump_accounts
COPY balance-tracker.sh /app/balance-tracker.sh
COPY docker/scripts/block-processing-healthcheck.sh /app/block-processing-healthcheck.sh
COPY docker/scripts/docker-entrypoint.sh /app/docker-entrypoint.sh

ENTRYPOINT ["/app/docker-entrypoint.sh"]
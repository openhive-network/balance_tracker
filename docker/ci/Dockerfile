# syntax=registry.gitlab.syncad.com/hive/common-ci-configuration/dockerfile:1.5
# When updating docker-builder and benchmark-test-runner use the images built from latest develop commit
# from repository registry.gitlab.syncad.com/hive/common-ci-configuration
FROM registry.gitlab.syncad.com/hive/common-ci-configuration/docker-builder:409108966606eba9e8f270c76dfb929cae65dba3

COPY --from=registry.gitlab.syncad.com/hive/common-ci-configuration/benchmark-test-runner:409108966606eba9e8f270c76dfb929cae65dba3 \
  --link /opt/tools/jmeter /opt/tools/jmeter
COPY --from=registry.gitlab.syncad.com/hive/common-ci-configuration/benchmark-test-runner:409108966606eba9e8f270c76dfb929cae65dba3 \
  --link /opt/tools/m2u /opt/tools/m2u

USER root
RUN <<EOF
  # Install system dependencies (need Python 3.12+ for hive-local-tools)
  echo "https://dl-cdn.alpinelinux.org/alpine/v3.20/main" >> /etc/apk/repositories
  echo "https://dl-cdn.alpinelinux.org/alpine/v3.20/community" >> /etc/apk/repositories
  # gcompat provides glibc compatibility for pre-built Python wheels (e.g., hiveio-wax)
  apk add --no-cache bash ca-certificates openjdk11-jre postgresql-client gcc musl-dev python3-dev linux-headers gcompat libstdc++
  apk add --no-cache --repository=https://dl-cdn.alpinelinux.org/alpine/v3.20/main python3~3.12 py3-pip
  apk add --no-cache --repository=https://dl-cdn.alpinelinux.org/alpine/v3.20/community py3-psycopg2

  # Create symlinks in bin directory
  ln -s /opt/tools/jmeter/bin/jmeter.sh /usr/bin/jmeter
  ln -s /opt/tools/m2u/m2u /usr/bin/m2u
EOF

USER hived
CMD ["/bin/bash"]
# syntax=docker/dockerfile:1.5
# When updating docker-builder and benchmark-test-runner use the images built from latest develop commit
# from repository registry.gitlab.syncad.com/hive/common-ci-configuration
FROM registry.gitlab.syncad.com/hive/common-ci-configuration/docker-builder:7ea99b10bbf0f9b1fbb88f52a78ead4c82f15d75

COPY --from=registry.gitlab.syncad.com/hive/common-ci-configuration/benchmark-test-runner:7ea99b10bbf0f9b1fbb88f52a78ead4c82f15d75 \
  --link /opt/tools/jmeter /opt/tools/jmeter
COPY --from=registry.gitlab.syncad.com/hive/common-ci-configuration/benchmark-test-runner:7ea99b10bbf0f9b1fbb88f52a78ead4c82f15d75 \
  --link /opt/tools/m2u /opt/tools/m2u

USER root
RUN <<EOF
  # Install system dependencies
  apk add --no-cache git bash ca-certificates openjdk11-jre postgresql-client python3 py3-pip py3-psycopg2

  # Create symlinks in bin directory
  ln -s /opt/tools/jmeter/bin/jmeter.sh /usr/bin/jmeter
  ln -s /opt/tools/m2u/m2u /usr/bin/m2u
EOF

USER hived
CMD ["/bin/bash"]
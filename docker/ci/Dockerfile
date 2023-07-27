# syntax=docker/dockerfile:1.5
# When updating docker-builder and benchmark-test-runner use the images built from latest develop commit
# from repository registry.gitlab.syncad.com/hive/common-ci-configuration
FROM registry.gitlab.syncad.com/hive/common-ci-configuration/docker-builder:27bf7a315f3cacdf45336501112e6ac414d39006

COPY --from=ghcr.io/forinil/jmeter-alpine:5.6.2 --link /opt/tools/jmeter /opt/tools/jmeter
COPY --from=registry.gitlab.syncad.com/hive/common-ci-configuration/benchmark-test-runner:27bf7a315f3cacdf45336501112e6ac414d39006 \
  --link /opt/tools/m2u /opt/tools/m2u

USER root
RUN <<EOF
  # Install system dependencies
  apk add --no-cache bash ca-certificates openjdk11-jre postgresql-client python3

  # Create symlinks in bin directory
  ln -s /opt/tools/jmeter/bin/jmeter.sh /usr/bin/jmeter
  ln -s /opt/tools/m2u/m2u /usr/bin/m2u
EOF

USER hived
CMD ["/bin/bash"]
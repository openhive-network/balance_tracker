# syntax=docker/dockerfile:1.5
ARG UBUNTU_VERSION=22.04
FROM ubuntu:${UBUNTU_VERSION} AS psql_client

USER root

ENV LANG=en_US.UTF-8

SHELL ["/bin/bash", "-c"]

RUN <<EOF
  set -e
  DEBIAN_FRONTEND=noniteractive apt-get update
  apt-get install -y postgresql-client
  apt-get clean
  rm -rf /var/lib/apt/lists/*
  useradd -ms /bin/bash -g users "haf_admin" && echo "haf_admin ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
EOF

USER haf_admin
WORKDIR /home/haf_admin

ENTRYPOINT [ "/bin/bash", "-c" ]


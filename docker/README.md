# Balance tracker Docker deployment

1. [Quickstart](#quickstart)
1. [Images](#images)
    1. [Overview](#overview)
    1. [Building](#building)
1. [Running Balance Tracker with Docker Compose](#running-balance-tracker-with-docker-compose)
    1. [Profiles](#profiles)
    1. [Configuration](#configuration)
        1. [Environment variables](#environment-variables)
        1. [Configuring HAF](#configuring-haf)
        1. [Configuring containers by using override files](#configuring-containers-by-using-override-files)

## Quickstart

Commands below will start a demo environment consisting of balance tracker and HAF with 5 million blocks.

```bash
curl https://gtg.openhive.network/get/blockchain/block_log.5M -o docker/blockchain/block_log
cd docker
docker compose up -d
```

You can stop the app with `docker compose stop` or `docker compose down` (the latter removes the containers) and remove all application data with `docker compose down -v`.

## Images

### Overview

Balancer Tracker consists of two Docker images: psql client for database setup and block processing, and a PostgREST image for running the API.

Psql client image is a simple Ubuntu-based image containing PostgreSQL client. It is designed to run scripts that are bind-mounted inside it. There is no need to rebuild this image unless to change the version of Ubuntu.

No custom PostgREST image is neeed for the API. The application has been tested to run with both official [postgrest/postgrest](https://hub.docker.com/r/postgrest/postgrest) image as well as Debian-based [bitnami/postgrest](https://hub.docker.com/r/bitnami/postgrest) variant. The latter can be used to build a custom image with healthcheck enabled - see the [dev](docker-compose.dev.yml) [Compose override file]((#configuring-containers-by-using-override-files)) for an example on how to do this on the fly.

### Building

There are several targets defined in the Bakefile

- *default* - alias for *full*
- *psql-client* - builds psql client image
- *full* - builds full image
- *ci-runner* - builds CI runner

There are also some other targets meant to be used by CI only: *ci-runner-ci*, *full-ci*

To build a given target run `docker buildx bake [target-name]`. If no target name is provided the *default* target will be built.

On Linux, you can also run `scripts/build_docker_image.sh [src_dir] --target=[target-name]` to build any of the targets. If no target name is provided the *default* target will be built. Run `scripts/build_docker_image.sh --help` to see all the options.

There's also `scripts/ci-helpers/build_instance.sh`, which can be used to build the *full* target. Running `scripts/ci-helpers/build_instance.sh [image_tag] [src_dir] [registry_url]` is functionally identical to running `scripts/build_docker_image.sh [src_dir] --registry=[registry_url] --target=full --tag=[image_tag]`.

## Running Balance Tracker with Docker Compose

### Profiles

The Composefile contains profiles that add additional containers to the setup:

- *swagger* - adds the Swagger UI running on port 8080
- *db-tools* - adds PgHero running on port 2080 and PgAdmin running on port 1080

You can enable the profiles by adding the profile option to `docker compose` command, eg. `docker compose --profile swagger up -d`. To enable multiple profiles specify the option multiple times (like with `--file` option in [Configuring containers by using override files](#configuring-containers-by-using-override-files) section).

### Configuration

#### Environment variables

The variables below are can be used to configure the Compose files.

| Name                              | Description                                                                                                         | Default value (some of those valuse are overridden in default .env)                 |
|-----------------------------------|---------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| HAF_REGISTRY                      | Registry containing HAF image                                                                                       | hiveio/haf                                                                          |
| HAF_VERSION                       | HAF version to use                                                                                                  | v1.27.4.0                                                                           |
| HAF_COMMAND                       | HAF command to execute                                                                                              | --shared-file-size=1G --plugin database_api --replay --stop-at-block=5000000 |
| BACKEND_REGISTRY                  | Registry containing psql client image                                                                               | registry.gitlab.syncad.com/hive/balance_tracker/psql-client                         |
| BACKEND_VERSION                   | Psql client image version to use                                                                                    | 14                                                                                  |
| POSTGREST_REGISTRY                | Registry containing PostgREST image                                                                                 | postgrest/postgrest                                                                 |
| POSTGREST_VERSION                 | PostgREST version to use                                                                                            | latest                                                                              |
| SWAGGER_REGISTRY                  | Registry containing Swagger UI image (*swagger* profile only)                                                       | swaggerapi/swagger-ui                                                               |
| SWAGGER_VERSION                   | Swagger UI version to use (*swagger* profile only)                                                                  | latest                                                                              |
| PGHERO_REGISTRY                   | Registry containing PgHero image (*db-tools* profile only)                                                          | ankane/pghero                                                                       |
| PGHERO_VERSION                    | PgHero version to use (*db-tools* profile only)                                                                     | latest                                                                              |
| PGADMIN_REGISTRY                  | Registry containing PgAdmin image (*db-tools* profile only)                                                         | dpage/pgadmin4                                                                      |
| PGADMIN_VERSION                   | PgAdmin version to use (*db-tools* profile only)                                                                    | latest                                                                              |
| HAF_DATA_DIRECTORY                | HAF data directory path on host (used by [docker-compose.bind-mounts.yml](docker-compose.bind-mounts.yml))          | none                                                                                |
| HAF_SHM_DIRECTORY                 | HAF shared memory directory path on host (used by [docker-compose.bind-mounts.yml](docker-compose.bind-mounts.yml)) | none                                                                                |
| HIVED_UID                         | UID to be used by HAF service                                                                                       | 0                                                                                   |
| PGHERO_USERNAME                   | PgHero username (*db-tools* profile only)                                                                           | link                                                                                |
| PGHERO_PASSWORD                   | PgHero password (*db-tools* profile only)                                                                           | hyrule                                                                              |
| PGADMIN_DEFAULT_EMAIL             | PgAdmin default email address (*db-tools* profile only)                                                             | [admin@btracker.internal](admin@btracker.internal)                                  |
| PGADMIN_DEFAULT_PASSWORD          | PgAdmin default password (*db-tools* profile only)                                                                  | admin                                                                               |

You can override them by editing the [.env](.env) file or by creating your own env file and instructing Docker Compose to use it instead of the default, eg.

```bash
# Create a .env.local file which overrides registries used to pull HAF and PostgREST images and HAF version as well as HAF command
{
  echo "HAF_REGISTRY=registry.gitlab.syncad.com/hive/haf/instance"
  echo "HAF_VERSION=instance-v1.27.5-rc0"
  echo "POSTGREST_REGISTRY=bitnami/postgrest"
  echo "HIVED_UID=0"
  echo "HAF_COMMAND=--shared-file-size=1G --plugin database_api --replay --stop-at-block=5000000"
} > .env.local

# Start the containers
docker compose --env-file .env.local up -d
```

If you wish to create your own .env file , you may want to use [.env](.env) as a template to avoid errors.

#### Configuring HAF

You can configure HAF by changing HAF_COMMAND value or overriding the entrypoint using Compose override files (more on those below).

#### Configuring containers by using override files

Docker Compose allows for changing the container configuration by using override files.

The easiest way to use the functionality is to create a *docker-compose.override.yml* and start the app as specified in the [Quickstart](#quickstart), eg.

```bash
curl https://gtg.openhive.network/get/blockchain/block_log.5M -o docker/blockchain/block_log
cd docker

# Create an override file that makes the haf-nettwork attachable
cat <<EOF > docker-compose.override.yml
networks:
  haf-network:
    name: haf-network
    attachable: true
EOF

# Optionally verify that the override is recognized by having Docker Compose display the merged configuration
docker compose config

docker compose up -d
```

There is one example override files provide: [dev.yml](overrides/dev.yml).

The file makes several changes to the configuration described in [docker-compose.yml](docker-compose.yml):

- adds variable `PGCTLTIMEOUT` set to 600 to HAF's environment
- changes value of variable `PG_ACCESS` in HAF's environment to `host    all    all    all    trust\n`
- exposes port 5432 on HAF's container
- changes the location of named volumes used by HAF container on from the host's default to the directories specified by the environment variables mentioned in [Environment variables](#environment-variables) section above. This makes the named volumes behave like bind mounts - which means that command `docker compose down -v` will not remove HAF's data. It needs to be done that manually.

The second one provides various overrides that can be useful for development, like static IP for the HAF container, healthcheck for the PostGREST container (not enabled by default, since it requires building a custom PostgREST image) or making the *haf-network* attachable.

If you want to use that file, you need to provide Docker Compose a list of YAML files to use, eg.

```bash
docker compose --file docker-compose.yml --file overrides/dev.yml up --detach
```

And, of course, you can combine all of these ways of configuring the containers, eg.

```bash
curl https://gtg.openhive.network/get/blockchain/block_log.5M -o docker/blockchain/block_log
cd docker

# Create a .env.local file which overrides registries used to pull HAF and PostgREST images
# and configures the bind mount directories as well as overides HAF command
{
  echo "HAF_REGISTRY=registry.gitlab.syncad.com/hive/haf/instance"
  echo "HAF_VERSION=instance-v1.27.5-rc0"
  echo "POSTGREST_REGISTRY=bitnami/postgrest"
  echo "HIVED_UID=$(id -u)" # Your user id
  echo "HAF_DATA_DIRECTORY=/srv/haf/data"
  echo "HAF_SHM_DIRECTORY=/srv/haf/shm"
  echo "HAF_COMMAND=--shared-file-size=1G --plugin database_api --replay --stop-at-block=5000000"
} > .env.local

# Create an override file that makes the haf-nettwork attachable
cat <<EOF > docker-compose.override.yml
networks:
  haf-network:
    name: haf-network
    attachable: true
EOF

# Verify you configuration
docker compose --project-name balance-tracker \
  --env-file .env.local \
  --file docker-compose.yml \
  --file docker-compose.override.yml \
  --file overrides/dev.yml \
  config

# Create the HAF data directories
mkdir -p /srv/haf/data
mkdir /srv/haf/shm

# Start the application with custom project name
docker compose --project-name balance-tracker \
  --env-file .env.local \
  --file docker-compose.yml \
  --file docker-compose.override.yml \
  --file overrides/dev.yml \
  up -d
```

Note that - as shown in the example above - if you want to use `docker-compose.override.yml` in addition to other override files, you need to specify it explicitly.

# Balance tracker Docker deployment

1. [Quickstart](#quickstart)
1. [Profiles](#profiles)
1. [Configuration](#configuration)
    1. [Environment variables](#environment-variables)
    1. [Configuring HAF](#configuring-haf)
    1. [Configuring containers by using override files](#configuring-containers-by-using-override-files)

## Quickstart

Commands below will start a demo environment consisting of balance tracker and HAF with 5 million blocks.

```bash
docker buildx bake # optional, if you want to build balance_tracker images, rather than pull them from the registry 
curl https://gtg.openhive.network/get/blockchain/block_log.5M -o docker/blockchain/block_log
cd docker
docker compose up -d
```

You can stop the app with `docker compose stop` or `docker compose down` (the latter removes the containers) and remove all the application data with `docker compose down -v`.

## Building images

There are several targets defined in the Bakefile

- *default* - groups together targets *frontend* and *backend*
- *base* - groups together targets *frontend-base* and *backend-base*
- *backend-base* - builds backend base image
- *backend* - builds backend image
- *frontend-base* - builds frontend image
- *frontend* - builds frontend image

To build a given target run `docker buildx bake [target-name]`. If no target name is provided the *default* target will be built.

## Profiles

The Composefile contains profiles that add additional containers to the setup:

- *swagger* - adds the Swagger UI running on port 8080
- *db-tools* - adds PgHero running on port 2080 and PgAdmin running on port 1080

You can enable the profiles by adding the profile option to `docker compose` command, eg. `docker compose --profile swagger up -d`. To enable multiple profiles specify the option multiple times (like with `--file` option in [Configuring containers by using override files](#configuring-containers-by-using-override-files) section).

## Configuration

### Environment variables

The variables below are can be used to configure the Compose files.

| Name                     | Description                                                                                                         | Default value                                           |
|--------------------------|---------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------|
| HAF_REGISTRY             | Registry containing HAF image                                                                                       | hiveio/haf                                              |
| HAF_VERSION              | HAF version to use                                                                                                  | v1.27.4.0                                               |
| BACKEND_REGISTRY         | Registry containing backend image                                                                                   | registry.gitlab.syncad.com/hive/balance_tracker/backend |
| BACKEND_VERSION          | Backend version to use                                                                                              | latest                                                  |
| POSTGREST_REGISTRY       | Registry containing PostgREST image                                                                                 | postgrest/postgrest                                     |
| POSTGREST_VERSION        | PostgREST version to use                                                                                            | latest                                                  |
| SWAGGER_REGISTRY         | Registry containing Swagger UI image (*swagger* profile only)                                                       | swaggerapi/swagger-ui                                   |
| SWAGGER_VERSION          | Swagger UI version to use (*swagger* profile only)                                                                  | latest                                                  |
| PGHERO_REGISTRY          | Registry containing PgHero image (*db-tools* profile only)                                                          | ankane/pghero                                           |
| PGHERO_VERSION           | PgHero version to use (*db-tools* profile only)                                                                     | latest                                                  |
| PGADMIN_REGISTRY         | Registry containing PgAdmin image (*db-tools* profile only)                                                         | dpage/pgadmin4                                          |
| PGADMIN_VERSION          | PgAdmin version to use (*db-tools* profile only)                                                                    | latest                                                  |
| HAF_DATA_DIRECTORY       | HAF data directory path on host (used by [docker-compose.bind-mounts.yml](docker-compose.bind-mounts.yml))          | none                                                    |
| HAF_SHM_DIRECTORY        | HAF shared memory directory path on host (used by [docker-compose.bind-mounts.yml](docker-compose.bind-mounts.yml)) | none                                                    |
| HIVED_UID                | UID to be used by HAF service                                                                                       | 0                                                       |
| PGHERO_USERNAME          | PgHero username (*db-tools* profile only)                                                                           | link                                                    |
| PGHERO_PASSWORD          | PgHero password (*db-tools* profile only)                                                                           | hyrule                                                  |
| PGADMIN_DEFAULT_EMAIL    | PgAdmin default email address (*db-tools* profile only)                                                             | [admin@btracker.internal](admin@btracker.internal)      |
| PGADMIN_DEFAULT_PASSWORD | PgAdmin default password (*db-tools* profile only)                                                                  | admin                                                   |

You can override them by editing the [.env](.env) file or by creating your own env file and instructing Docker Compose to use it instead of the default, eg.

```bash
# Create a .env.local file which overrides registries used to pull HAF and PostgREST images and HAF version
{
  echo "HAF_REGISTRY=registry.gitlab.syncad.com/hive/haf/instance"
  echo "HAF_VERSION=instance-v1.27.4.0"
  echo "POSTGREST_REGISTRY=bitnami/postgrest"
  echo "HIVED_UID=0"
} > .env.local

# Start the containers
docker compose --env-file .env.local up -d
```

### Configuring HAF

You can configure HAF by editing its [configuration file](haf/config_5M.ini) and by overriding the entrypoint using Compose override files (more on those below).

### Configuring containers by using override files

Docker Compose allows for changing the container configuration by using override files.

The easiest way to use the functionality is to create a *docker-compose.override.yml* and start the app as specified in the [Quickstart](#quickstart), eg.

```bash
curl https://gtg.openhive.network/get/blockchain/block_log.5M -o docker/blockchain/block_log
cd docker

# Create an override file that makes the haf-nettwork attachable
cat <<EOF > docker-compose.override.yml
networks:
  haf-network:
    attachable: true
EOF

# Optionally verify that the override is recognized by having Docker Compose display the merged configuration
docker compose config

docker compose up -d
```

There are two examples of override files provided:

- [docker-compose.bind-mounts.yml](docker-compose.bind-mounts.yml)
- [docker-compose.dev.yml](docker-compose.dev.yml)

The first one changes the location of named volumes used by HAF container on from the host's default to the directories specified by the environment variables mentioned in [Environment variables](#environment-variables) section above. This makes the named volumes behave like bind mounts - which means that command `docker compose down -v` will not remove HAF's data. You need to do that manually.

The second one provides various overrides that can be useful for development, like static IP for the HAF container, healthcheck for the PostGREST container (not enabled by default, since it requires building a custom PostgREST image) or making the *haf-network* attachable.

If you want to use one (or both) of those files, you need to provide Docker Compose a list of YAML files to use, eg.

```bash
docker compose --file docker-compose.yml --file docker-compose.dev.yml up -d
```

And, of course, you can combine all of these ways of configuring the containers, eg.

```bash
curl https://gtg.openhive.network/get/blockchain/block_log.5M -o docker/blockchain/block_log
cd docker

# Create a .env.local file which overrides registries used to pull HAF and PostgREST images
# and configures the bind mount directories
{
  echo "HAF_REGISTRY=registry.gitlab.syncad.com/hive/haf/instance"
  echo "HAF_VERSION=instance-v1.27.4.0"
  echo "POSTGREST_REGISTRY=bitnami/postgrest"
  echo "HIVED_UID=$(id -u)" # Your user id
  echo "HAF_DATA_DIRECTORY=/srv/haf/data"
  echo "HAF_SHM_DIRECTORY=/srv/haf/shm"
} > .env.local

# Create an override file that makes the haf-nettwork attachable
cat <<EOF > docker-compose.override.yml
networks:
  haf-network:
    attachable: true
EOF

# Verify you configuration
docker compose \
  --env-file .env.local \
  --file docker-compose.yml \
  --file docker-compose.override.yml \
  --file docker-compose.bind-mounts.yml \
  config

# Create the HAF data directories
mkdir -p /srv/haf/data
mkdir /srv/haf/shm

# Start the application
docker compose \
  --env-file .env.local \
  --file docker-compose.yml \
  --file docker-compose.override.yml \
  --file docker-compose.bind-mounts.yml \
  up -d
```

Note that - as shown in the example above - if you want to use `docker-compose.override.yml` in addition to other override files, you need to specify it explicitly.

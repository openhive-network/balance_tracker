# Balance tracker Docker deployment

```bash
docker compose up -d
docker compose down -v

docker compose --file docker-compose.yml --file docker-compose.dev.yml config
docker compose --file docker-compose.yml --file docker-compose.dev.yml up -d
docker compose --file docker-compose.yml --file docker-compose.dev.yml down -v

docker compose --env-file .env.local \
    --file docker-compose.yml \
    --file docker-compose.bind-mounts.yml \
    config
docker compose --env-file .env.local \
    --file docker-compose.yml \
    --file docker-compose.bind-mounts.yml \
    up -d
docker compose --env-file .env.local \
    --file docker-compose.yml \
    --file docker-compose.bind-mounts.yml \
    down -v

docker compose --env-file .env.local \
    --file docker-compose.yml \
    --file docker-compose.dev.yml \
    --file docker-compose.bind-mounts.yml \
    config
docker compose --env-file .env.local \
    --file docker-compose.yml \
    --file docker-compose.dev.yml \
    --file docker-compose.bind-mounts.yml \
    up -d
docker compose --env-file .env.local \
    --file docker-compose.yml \
    --file docker-compose.dev.yml \
    --file docker-compose.bind-mounts.yml \
    down -v
```

## docker-compose script for launching balance-tracker

This docker-compose script is not stand-alone.  It is designed to work with the docker-compose scripts
in the HAF distribution, documented [here](https://gitlab.syncad.com/hive/haf/-/blob/develop/example_deployment/README.md).

## Launch example
To launch the balance-tracker app, add the .yaml file in this directory to your docker-compose command line, like:
```SH
docker compose --env-file .env.dev -f haf_base.yaml -f backend.yaml -f balance-tracker-app.yaml up -d
docker compose --env-file .env.dev -f haf_base.yaml -f backend.yaml -f balance-tracker-app.yaml down
```

You will need to copy/link this .yaml file into HAF's example_deployment directory, or specify a full or relative 
path when running docker-compose

#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== [Start] Airflow Webserver ==="

docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile webserver \
  up -d --build --force-recreate --remove-orphans

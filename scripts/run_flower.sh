#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== [1/3] Stop & Remove Docker Containers (flower) ==="
docker-compose -f ../docker-compose-prod.yaml --env-file ../.env.prod --profile monitoring down --remove-orphans || true

docker ps -a -q --filter "name=flower" | xargs -r sudo docker stop || true
docker ps -a -q --filter "name=flower" | xargs -r sudo docker rm || true

echo "=== [2/3] Build Image (Cache Check) ==="
docker-compose -f ../docker-compose-prod.yaml --env-file ../.env.prod --profile monitoring build

echo "=== [3/3] Start Airflow Flower ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile monitoring \
  up -d --force-recreate --always-recreate-deps --no-build

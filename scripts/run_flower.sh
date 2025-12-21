#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== [1/3] Cleaning up existing containers and orphans ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  down --remove-orphans

echo "=== [2/3] Pruning dangling images and containers ==="
docker container prune -f

echo "=== [3/3] Starting Airflow Webserver (Rebuild) ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile monitoring \
  up -d --build --force-recreate

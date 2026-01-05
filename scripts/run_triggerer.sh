#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== [Cleanup] Removing previous 'airflow-init' container ==="

docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  rm -f -s -v airflow-init

echo "=== Ensuring Airflow Triggerer is up and running ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile triggerer \
  up -d --remove-orphans --build

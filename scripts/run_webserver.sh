#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== [Cleanup] Removing previous 'airflow-init' container ==="

docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  rm -f -s -v airflow-init

echo "=== Ensuring Airflow Webserver is up and running ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile webserver \
  up -d --remove-orphans --build

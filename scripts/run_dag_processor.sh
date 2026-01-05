#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== [Cleanup] Removing previous 'airflow-init' container ==="

docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  rm -f -s -v airflow-init

echo "=== Ensuring Airflow DAG Processor is up and running ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile dag-processor \
  up -d --remove-orphans --build

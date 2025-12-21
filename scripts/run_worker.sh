#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== [Start] Airflow Celery Worker ==="

docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile worker \
  up -d --build --force-recreate --remove-orphans

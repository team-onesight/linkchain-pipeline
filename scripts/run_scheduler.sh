#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== [Start] Airflow Scheduler Group ==="

docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile scheduler \
  up -d --build --force-recreate --remove-orphans

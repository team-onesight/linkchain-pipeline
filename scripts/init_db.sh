#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "=== [Init] Airflow DB Migration & User Creation ==="

docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile init \
  run --rm airflow-init

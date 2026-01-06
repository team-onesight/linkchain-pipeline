#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== [1/3] Stop & Remove Docker Containers (dag-processor, init) ==="
docker ps -a -q --filter "name=airflow-dag-processor" --filter "name=airflow-init" | xargs -r sudo docker stop || true
docker ps -a -q --filter "name=airflow-dag-processor" --filter "name=airflow-init" | xargs -r sudo docker rm || true

echo "=== [2/3] Build Image (Cache Check) ==="
docker-compose -f ../docker-compose-prod.yaml --env-file ../.env.prod --profile dag-processor build

echo "=== [3/3] Start Airflow DAG Processor ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile dag-processor \
  up -d --remove-orphans --no-build

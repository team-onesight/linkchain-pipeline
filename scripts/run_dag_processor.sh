#!/bin/bash
set -e
cd "$(dirname "$0")"

SERVICE_NAME="airflow-dag-processor"

echo "=== [1/3] Stop & Remove Specific Container ($SERVICE_NAME) ==="
docker-compose -f ../docker-compose-prod.yaml --env-file ../.env.prod stop $SERVICE_NAME || true
docker-compose -f ../docker-compose-prod.yaml --env-file ../.env.prod rm -f $SERVICE_NAME || true

docker ps -a -q --filter "name=$SERVICE_NAME" --filter "name=airflow-init" | xargs -r sudo docker rm -f || true

echo "=== [2/3] Build Image (Cache Check) ==="
docker-compose -f ../docker-compose-prod.yaml --env-file ../.env.prod build $SERVICE_NAME

echo "=== [3/3] Start Airflow DAG Processor ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile dag-processor \
  up -d --no-deps --force-recreate --always-recreate-deps --no-build $SERVICE_NAME

#!/bin/bash
set -e
cd "$(dirname "$0")"

SERVICE_NAME="airflow-init"

echo "=== [1/3] Stop & Remove Old Init Containers ==="
docker-compose -f ../docker-compose-prod.yaml --env-file ../.env.prod stop $SERVICE_NAME || true
docker-compose -f ../docker-compose-prod.yaml --env-file ../.env.prod rm -f $SERVICE_NAME || true

docker ps -a -q --filter "name=$SERVICE_NAME" | xargs -r sudo docker rm -f || true

echo "=== [2/3] Rebuild Image from Scratch (No Cache) ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile init build --no-cache $SERVICE_NAME

echo "=== [3/3] Run Airflow Init (Migration & User Creation) ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile init \
  run --rm $SERVICE_NAME

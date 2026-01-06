#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== [1/3] Cleanup Old Init Containers ==="
docker ps -a -q --filter "name=airflow-init" | xargs -r sudo docker stop || true
docker ps -a -q --filter "name=airflow-init" | xargs -r sudo docker rm || true

echo "=== [2/3] Rebuild Image from Scratch (No Cache) ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  build --no-cache airflow-init

echo "=== [3/3] Run Airflow Init (Migration & User Creation) ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile init \
  run --rm airflow-init

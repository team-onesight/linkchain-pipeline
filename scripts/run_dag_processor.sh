#!/bin/bash
set -e
cd "$(dirname "$0")"


echo "=== Stop Docker Container (dag-processor, init) ==="
docker ps -a -q --filter "name=airflow-dag-processor" \
--filter "name=airflow-init" | xargs -r sudo docker stop || true
echo "=== Remove Docker Container (dag-processor, init) ==="
docker ps -a -q --filter "name=airflow-dag-processor" \
--filter "name=airflow-init" | xargs -r sudo docker rm || true

echo "=== Ensuring Airflow DAG Processor is up and running ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile dag-processor \
  up -d --remove-orphans --build

#!/bin/bash
set -e
cd "$(dirname "$0")"


echo "=== Stop Docker Container (triggerer, init) ==="
docker ps -a -q --filter "name=airflow-triggerer" \
--filter "name=airflow-init" | xargs -r sudo docker stop || true
echo "=== Remove Docker Container (triggerer, init) ==="
docker ps -a -q --filter "name=airflow-triggerer" \
--filter "name=airflow-init" | xargs -r sudo docker rm || true

echo "=== Ensuring Airflow Triggerer is up and running ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile triggerer \
  up -d --remove-orphans --build

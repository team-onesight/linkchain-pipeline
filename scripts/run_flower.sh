#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== Stop Docker Container (flower) ==="
docker ps -a -q --filter "name=flower" | xargs -r sudo docker stop || true
echo "=== Remove Docker Container (flower) ==="
docker ps -a -q --filter "name=flower" | xargs -r sudo docker rm || true

echo "=== Ensuring Airflow Flower is up and running ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile monitoring \
  up -d --remove-orphans --build

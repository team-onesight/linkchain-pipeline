#!/bin/bash
set -e
cd "$(dirname "$0")"


echo "=== Stop Docker Container (scheduler, init) ==="
docker ps -a -q --filter "name=airflow-scheduler" \
--filter "name=airflow-init" | xargs -r sudo docker stop || true
echo "=== Remove Docker Container (scheduler, init) ==="
docker ps -a -q --filter "name=airflow-scheduler" \
--filter "name=airflow-init" | xargs -r sudo docker rm || true

echo "=== Ensuring Airflow Scheduler is up and running ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile scheduler \
  up -d --remove-orphans --build

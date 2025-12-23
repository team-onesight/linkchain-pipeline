#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== Stop Docker Container (apiserver, init) ==="
docker ps -a -q --filter "name=airflow-apiserver" \
--filter "name=airflow-init" | xargs -r sudo docker stop || true
echo "=== Remove Docker Container (apiserver, init) ==="
docker ps -a -q --filter "name=airflow-apiserver" \
--filter "name=airflow-init" | xargs -r sudo docker rm || true

echo "=== Ensuring Airflow Webserver is up and running ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile webserver \
  up -d --remove-orphans --build

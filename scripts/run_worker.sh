#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== Stop Docker Container (worker, init) ==="
docker ps -a -q --filter "name=airflow-worker" --filter "name=airflow-init" | xargs -r sudo docker stop || true
echo "=== Remove Docker Container (worker, init) ==="
docker ps -a -q --filter "name=airflow-worker" --filter "name=airflow-init" | xargs -r sudo docker rm || true


export WORKER_PORT=${1:-8793}
# shellcheck disable=SC2155
export SERVER_IP=$(hostname -I | awk '{print $1}')
export AIRFLOW__CORE__HOSTNAME=$SERVER_IP
echo "=== Starting Airflow Worker with Fixed IP: $SERVER_IP (port: $WORKER_PORT) ==="

docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile worker \
  up -d --force-recreate --build

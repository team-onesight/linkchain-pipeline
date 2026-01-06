#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== [1/3] Stop & Remove Old Containers ==="
docker ps -a -q --filter "name=airflow-worker" --filter "name=airflow-init" | xargs -r sudo docker stop || true
docker ps -a -q --filter "name=airflow-worker" --filter "name=airflow-init" | xargs -r sudo docker rm || true


export WORKER_PORT=${1:-8793}
# shellcheck disable=SC2155
export SERVER_IP=$(hostname -I | awk '{print $1}')
export AIRFLOW__CORE__HOSTNAME=$SERVER_IP

echo "=== [2/3] Build Image (Cache Check) ==="
docker-compose -f ../docker-compose-prod.yaml --env-file ../.env.prod --profile worker build

echo "=== [3/3] Start Worker with Fixed IP: $SERVER_IP ==="
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile worker \
  up -d --force-recreate --no-build

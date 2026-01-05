#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== [Cleanup] Removing previous 'airflow-init' container ==="

docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  rm -f -s -v airflow-init

export WORKER_PORT=${1:-8793}
# shellcheck disable=SC2155
export SERVER_IP=$(hostname -I | awk '{print $1}')
export AIRFLOW__CORE__HOSTNAME=$SERVER_IP

echo "============================================"
echo "   [STEP 2] BUILD & DEPLOY START            "
echo "   Target IP: $SERVER_IP                    "
echo "   Port     : $WORKER_PORT                  "
echo "============================================"

echo ">>> Running Docker Compose..."
docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile worker \
  up -d --force-recreate --build

echo "============================================"
echo "   [STEP 2] DEPLOYMENT SUCCESSFUL!          "
echo "============================================"

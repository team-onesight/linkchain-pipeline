#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== [Start] Flower (Monitoring) ==="

docker-compose \
  -f ../docker-compose-prod.yaml \
  --env-file ../.env.prod \
  --profile monitoring \
  up -d --build --force-recreate --remove-orphans

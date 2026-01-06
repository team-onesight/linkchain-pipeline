#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== [1/3] Clean up Local Environment ==="
docker-compose -f ../../docker-compose-local.yaml --env-file ../../.env.local down --remove-orphans || true

echo "=== [2/3] Build Local Image (Cache Enabled) ==="
docker-compose -f ../../docker-compose-local.yaml --env-file ../../.env.local build

echo "=== [3/3] Start Local Airflow (Foreground or Background) ==="
docker-compose \
  -f ../../docker-compose-local.yaml \
  --env-file ../../.env.local \
  up -d --force-recreate --always-recreate-deps

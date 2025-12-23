#!/bin/bash
set -e

docker-compose \
  -f ../../docker-compose-local.yaml \
  --env-file ../../.env.local \
  up -d --build --force-recreate --remove-orphans --build

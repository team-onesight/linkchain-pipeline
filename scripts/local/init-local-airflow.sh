#!/bin/bash
set -e

docker-compose \
  -f ../../docker-compose-local.yaml \
  --env-file ../../.env.local \
  up -d --force-recreate --remove-orphans --build

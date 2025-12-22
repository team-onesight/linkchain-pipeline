#!/bin/bash
set -e
cd "$(dirname "$0")"


echo "=== Stop All Airflow Docker Containers ==="
docker ps -a -q --filter "name=linkchain-pipeline_airflow" \
--filter "name=airflow-init" | xargs -r sudo docker stop || true
echo "=== Stop All Airflow Docker Containers ==="
docker ps -a -q --filter "name=linkchain-pipeline_airflow" \
--filter "name=airflow-init" | xargs -r sudo docker rm || true

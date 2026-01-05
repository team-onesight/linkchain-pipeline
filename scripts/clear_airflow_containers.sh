#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "============================================"
echo "   [STEP 1] DOCKER CLEANUP & RESET START    "
echo "============================================"

echo ">>> Stop & Remove Existing Containers..."
docker ps -a -q --filter "name=linkchain" --filter "name=airflow" | xargs -r sudo docker stop || true
docker ps -a -q --filter "name=linkchain" --filter "name=airflow" | xargs -r sudo docker rm || true

echo ">>> Pruning Docker System (Freeing up Disk Space)..."
sudo docker system prune -a -f
sudo docker builder prune -a -f

echo "============================================"
echo "   [STEP 1] CLEANUP COMPLETED! CHECK DISK   "
echo "============================================"

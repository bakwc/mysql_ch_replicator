#!/bin/bash
docker compose -f docker-compose-tests.yaml up --force-recreate --no-deps --wait -d
CONTAINER_ID=$(docker ps | grep -E "(mysql_ch_replicator_src-replicator|mysql_ch_replicator-replicator)" | awk '{print $1}')
docker exec -w /app/ -i $CONTAINER_ID python3 -m pytest -x -v -s tests/
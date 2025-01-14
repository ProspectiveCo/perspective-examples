#!/bin/bash

# Description: Run ClickHouse in Docker container.

DB_USER=admin
DB_PASS=admin1234

# Pull the latest ClickHouse image.
echo "Pulling the latest ClickHouse image..."
docker pull clickhouse/clickhouse-server

# Check if a container with the name 'prsp-clickhouse' exists and remove it if found.
if [ "$(docker ps -a -q -f name=prsp-clickhouse)" ]; then
    echo "Removing the previous ClickHouse container..."
    docker rm -vf prsp-clickhouse
fi

# Start a new Clickhouse docker container.
#   - port 8123: HTTP interface.
#   - port 9000: Native interface.
#   - ulimit nofile: Increase the maximum number of open files.
#   - --rm: Remove the container when it stops.
echo "Starting ClickHouse container..."
docker run -d --rm \
    --name prsp-clickhouse \
    --ulimit nofile=262144:262144 \
    -p 8123:8123 \
    -p 9000:9000 \
    -e CLICKHOUSE_DB=test \
    -e CLICKHOUSE_USER=${DB_USER} \
    -e CLICKHOUSE_PASSWORD=${DB_PASS} \
    -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
    clickhouse/clickhouse-server

# Check the container status.
echo -e -n "\nWaiting for the ClickHouse container to come up.."
while true; do
    response=$(echo 'SELECT version()' | curl -s -u ${DB_USER}:${DB_PASS} 'http://localhost:8123/' --data-binary @-)
    if [[ $response =~ ^[0-9]+\.[0-9]+ ]]; then
        echo " OK"
        break
    else
        echo -n "."
        sleep 1
    fi
done

echo -e "\nClickHouse is ready!"
echo "USERNAME: ${DB_USER} PASSWORD: ${DB_PASS}"
echo "HTTP interface: http://localhost:8123"
echo "Native interface: http://localhost:9000"

#!/bin/bash

# LATEST INFLUXDB TESTED VERSION: 2.7.10

# Configuration
IMAGE_TAG="influxdb:2"
CONTAINER_NAME="influxdb_container"

# InfluxDB data dirs
DATA_DIR="./container-data/data"
CONFIG_DIR="./container-data/config"
SCRIPTS_DIR="./container-data/scripts"

# InfluxDB credentials and initial setup
INIT_TOKEN="sudo-banana-404-not-found"
INFLUX_USER="admin"
INFLUX_PASS="sudo-banana-404"
ORG_NAME="perspective"
BUCKET_NAME="stocks"
RETENTION="1w"


# Check if directories exist, if not create them
if [[ ! -d "$DATA_DIR" || ! -d "$CONFIG_DIR" || ! -d "$SCRIPTS_DIR" ]]; then
    echo "One or more directories are missing. Creating necessary directories..."
    if [[ ! -d "$DATA_DIR" ]]; then
        mkdir -p "$DATA_DIR"
    fi
    if [[ ! -d "$CONFIG_DIR" ]]; then
        mkdir -p "$CONFIG_DIR"
    fi
    if [[ ! -d "$SCRIPTS_DIR" ]]; then
        mkdir -p "$SCRIPTS_DIR"
    fi
fi


# Stop and remove any existing container with the same name
if docker ps -a --filter "name=$CONTAINER_NAME" | grep -q "$CONTAINER_NAME"; then
    echo "Stopping and removing existing container..."
    docker stop "$CONTAINER_NAME"
    docker rm -f "$CONTAINER_NAME"
fi

echo "Checking if Docker image $IMAGE_TAG is pulled..."
if [[ "$(docker images -q $IMAGE_TAG 2> /dev/null)" == "" ]]; then
    echo "Docker image $IMAGE_TAG not found. Pulling the image..."
    docker pull $IMAGE_TAG
else
    echo "Docker image $IMAGE_TAG already exists. Skipping pull."
fi

# Run the InfluxDB container with the required setup
echo "Starting the InfluxDB container..."
docker run -d \
    --name "$CONTAINER_NAME" \
    -p 8086:8086 \
    -v "$DATA_DIR":/var/lib/influxdb2 \
    -v "$CONFIG_DIR":/etc/influxdb2 \
    -v "$SCRIPTS_DIR":/docker-entrypoint-initdb.d \
    "$IMAGE_TAG" \
    influxd --reporting-disabled

# Wait for the container to be ready
echo "Waiting for the container to be ready..."
until curl -s http://localhost:8086/health | grep -q '"status":"pass"'; do
    echo "Waiting for InfluxDB to become healthy..."
    sleep 2
done

# Initialize InfluxDB with the given settings
echo "Setting up InfluxDB..."
docker exec "$CONTAINER_NAME" influx setup --skip-verify --force \
    -u "$INFLUX_USER" \
    -p "$INFLUX_PASS" \
    -o "$ORG_NAME" \
    -b "$BUCKET_NAME" \
    -r "$RETENTION" \
    -t "$INIT_TOKEN"

echo "InfluxDB setup completed successfully."
echo "Admin user: $INFLUX_USER"
echo "Admin password: $INFLUX_PASS"
echo "Organization: $ORG_NAME"
echo "Bucket: $BUCKET_NAME"
echo "Retention: $RETENTION"
echo "Admin token: $INIT_TOKEN"

#!/bin/bash

# LATEST INFLUXDB TESTED VERSION: 2.7.10

# Configuration
IMAGE_TAG="influxdb:2"
CONTAINER_NAME="influxdb_container"
# InfluxDB data dirs
INFLUXDB_DATA_DIR="./volumes/influxdb"
INFLUXDB_CONFIG_DIR="./volumes/config"
DATA_DIR="./volumes/data"
# InfluxDB credentials and initial setup
INIT_TOKEN="sudo-banana-404-not-found"
INFLUX_USER="admin"
INFLUX_PASS="sudo-banana-404"
ORG_NAME="perspective"
BUCKET_NAME="trades"
SAMPLE_BUCKET_NAME="trades_sample"
RETENTION="0"
# initial data file
LARGE_DATA_FILE="/data/trades-md.tar.gz"
SAMPLE_DATA_FILE="/data/trades-sm.csv"


# Check if the InfluxDB container is already running
if docker ps --filter "name=$CONTAINER_NAME" | grep -q "$CONTAINER_NAME"; then
    echo "The InfluxDB container is already running. Exiting."
    echo "Stopping and removing existing container..."
    docker rm -vf "$CONTAINER_NAME"
fi

# Stop and remove any existing container with the same name
if docker ps -a --filter "name=$CONTAINER_NAME" | grep -q "$CONTAINER_NAME"; then
    echo "Removing existing container..."
    docker rm -vf "$CONTAINER_NAME"
fi

echo "Checking if Docker image $IMAGE_TAG is pulled..."
if [[ "$(docker images -q $IMAGE_TAG 2> /dev/null)" == "" ]]; then
    echo "Docker image $IMAGE_TAG not found. Pulling the image..."
    docker pull $IMAGE_TAG
else
    echo "Docker image $IMAGE_TAG already exists. Skipping pull."
fi

# Check if directories exist, if not create them
if [[ ! -d "$INFLUXDB_DATA_DIR" ]]; then
    echo "Creating data dir: $INFLUXDB_DATA_DIR"
    mkdir -p "$INFLUXDB_DATA_DIR"
else
    echo "Erasing data dir content: $INFLUXDB_DATA_DIR"
    rm -rf "$INFLUXDB_DATA_DIR/*"
fi
if [[ ! -d "$INFLUXDB_CONFIG_DIR" ]]; then
    echo "Creating config dir: $INFLUXDB_CONFIG_DIR"
    mkdir -p "$INFLUXDB_DATA_DIR"
else
    echo "Erasing config dir content: $INFLUXDB_CONFIG_DIR"
    rm -rf "$INFLUXDB_CONFIG_DIR/*"
fi

# Run the InfluxDB container with the required setup
echo "Starting the InfluxDB container..."
docker run -d \
    --name "$CONTAINER_NAME" \
    -p 8086:8086 \
    -v "$INFLUXDB_DATA_DIR":/var/lib/influxdb2 \
    -v "$INFLUXDB_CONFIG_DIR":/etc/influxdb2 \
    -v "$DATA_DIR":/data \
    -e DOCKER_INFLUXDB_INIT_MODE=setup \
    -e DOCKER_INFLUXDB_INIT_USERNAME="$INFLUX_USER" \
    -e DOCKER_INFLUXDB_INIT_PASSWORD="$INFLUX_PASS" \
    -e DOCKER_INFLUXDB_INIT_ORG="$ORG_NAME" \
    -e DOCKER_INFLUXDB_INIT_BUCKET="$BUCKET_NAME" \
    -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN="$INIT_TOKEN" \
    "$IMAGE_TAG"

# Wait for the container to be ready
echo "Waiting for the container to be ready..."
until curl -s http://localhost:8086/health | grep -q '"status":"pass"'; do
    echo "Waiting for InfluxDB to become healthy..."
    sleep 2
done

# Load initial sample data
echo "Loading sample data..."
# creating sample bucket
docker exec "$CONTAINER_NAME" influx bucket create \
    --org $ORG_NAME \
    --token "$INIT_TOKEN" \
    --name "$SAMPLE_BUCKET_NAME" \
    --retention $RETENTION
echo "New bucket created: $SAMPLE_BUCKET_NAME"

docker exec "$CONTAINER_NAME" influx write \
    --bucket $SAMPLE_BUCKET_NAME \
    --org $ORG_NAME \
    --token "$INIT_TOKEN" \
    --file $SAMPLE_DATA_FILE \
    --format csv \
    --skipRowOnError
echo "Loaded sample dataset"

# load large (gzip) historical dataset
echo "Loading large historical dataset. This will take a few minutes..."
docker exec "$CONTAINER_NAME" influx write \
    --bucket $BUCKET_NAME \
    --org $ORG_NAME \
    --token "$INIT_TOKEN" \
    --file $LARGE_DATA_FILE \
    --format csv \
    --compression gzip \
    --header "#constant measurement,trades" \
    --header "#datatype dateTime:number,tag,tag,double,double,double,double,unsignedLong,double,double,double,string" \
    --skipRowOnError
echo "Loaded historical dataset"

# output config params
echo -e "\nInfluxDB setup completed successfully.\n"
echo "CONNECTION INFO:"
echo "======================================="
echo "Admin user: $INFLUX_USER"
echo "Admin password: $INFLUX_PASS"
echo "Organization: $ORG_NAME"
echo "Buckets: $BUCKET_NAME, $SAMPLE_BUCKET_NAME"
echo "Retention: $RETENTION"
echo "Admin token: $INIT_TOKEN"
echo "Web UI: http://localhost:8006/"

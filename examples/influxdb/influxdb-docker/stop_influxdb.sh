#!/bin/bash

# LATEST INFLUXDB TESTED VERSION: 2.7.10

# Configuration
IMAGE_TAG="influxdb:2"
CONTAINER_NAME="influxdb_container"
# InfluxDB data dirs
INFLUXDB_DATA_DIR="./volumes/influxdb"
INFLUXDB_CONFIG_DIR="./volumes/config"
DATA_DIR="./volumes/data"


# Check if the InfluxDB container is already running
if docker ps --filter "name=$CONTAINER_NAME" | grep -q "$CONTAINER_NAME"; then
    echo "The InfluxDB container is already running. Exiting..."
    echo "Stopping and removing existing container..."
    docker rm -vf "$CONTAINER_NAME"
fi

# Stop and remove any existing container with the same name
if docker ps -a --filter "name=$CONTAINER_NAME" | grep -q "$CONTAINER_NAME"; then
    echo "Removing existing container..."
    docker rm -vf "$CONTAINER_NAME"
fi

# Check if directories exist, if not create them
if [[ -d "$INFLUXDB_DATA_DIR" ]]; then
    echo "Erasing influxdb dir content: $INFLUXDB_DATA_DIR"
    rm -rf "$INFLUXDB_DATA_DIR"
fi
if [[ -d "$INFLUXDB_CONFIG_DIR" ]]; then
    echo "Erasing config dir content: $INFLUXDB_CONFIG_DIR"
    rm -rf "$INFLUXDB_CONFIG_DIR"
fi

echo "Done."

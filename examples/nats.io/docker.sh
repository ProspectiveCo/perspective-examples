#!/bin/bash

CONTAINER_NAME="prsp-nats"
IMAGE_NAME="nats:latest"


# check if the docker daemon is running
if ! command -v docker &> /dev/null; then
    echo "WARNING: docker daemon is not running. Please start the docker daemon."
    exit 1
fi

# Check if the container is already running, delete it if so
if [ "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
    echo "Container $CONTAINER_NAME is already running. Stopping and removing it..."
    docker rm -vf $CONTAINER_NAME
fi

# pull the latest nats image
docker pull $IMAGE_NAME

# run the nats container
docker run -it --rm \
    --name $CONTAINER_NAME \
    -v "$PWD"/conf:/container \
    -p 8080:8080 \
    -p 4222:4222 \
    -p 8222:8222\
    $IMAGE_NAME \
    -c /container/nats.conf

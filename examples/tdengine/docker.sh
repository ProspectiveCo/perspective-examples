#  ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
#  ┃ ██████ ██████ ██████       █      █      █      █      █ █▄  ▀███ █       ┃
#  ┃ ▄▄▄▄▄█ █▄▄▄▄▄ ▄▄▄▄▄█  ▀▀▀▀▀█▀▀▀▀▀ █ ▀▀▀▀▀█ ████████▌▐███ ███▄  ▀█ █ ▀▀▀▀▀ ┃
#  ┃ █▀▀▀▀▀ █▀▀▀▀▀ █▀██▀▀ ▄▄▄▄▄ █ ▄▄▄▄▄█ ▄▄▄▄▄█ ████████▌▐███ █████▄   █ ▄▄▄▄▄ ┃
#  ┃ █      ██████ █  ▀█▄       █ ██████      █      ███▌▐███ ███████▄ █       ┃
#  ┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
#  ┃ Copyright (c) 2017, the Perspective Authors.                              ┃
#  ┃ ╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌ ┃
#  ┃ This file is part of the Perspective library, distributed under the terms ┃
#  ┃ of the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). ┃
#  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

# This script starts a new tdengine docker container, 
# populates the database with some test data, and 
# checks the status of the database.

TAOS_IMAGE="tdengine/tdengine"
CONTAINER_NAME="prsp-tdengine"
TAOS_USER="root"
TAOS_PASSWORD="taosdata"


# check if docker is installed
if ! command -v docker &> /dev/null; then
    echo "WARNING: docker is not installed. Please install docker first."
    exit 1
fi

# remove any existing tdengine docker container
if [ "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
    echo "stopping existing tdengine docker container..."
    docker stop $CONTAINER_NAME
    docker rm -vf $CONTAINER_NAME
fi

# pull the latest tdengine docker image
echo "pulling the latest tdengine docker image..."
docker pull $TAOS_IMAGE

# start a new tdengine docker container
echo "starting a new tdengine docker container..."
docker run -d --name $CONTAINER_NAME \
    -e TAOS_USER="$TAOS_USER" \
    -e TAOS_PASSWORD="$TAOS_PASSWORD" \
    -p 6030:6030 \
    -p 6041:6041 \
    -p 6043-6060:6043-6060 \
    -p 6043-6060:6043-6060/udp \
    $TAOS_IMAGE


# check the tdengine database status
echo -n "waiting for tdengine database to initiate..."
while true; do
    status=$(docker exec $CONTAINER_NAME taos --check)
    if [[ $status == *"service ok"* ]]; then
        echo -e "\ntdengine database is ready!"
        break
    fi
    sleep 1
    echo -n "."
done

# populate the database with some data
echo "populating the database with test data..."
docker exec -it $CONTAINER_NAME taosBenchmark -y

# done
echo "done!"

# This script runs an InfluxDB container with the following configuration:
docker run -d --rm \
    --name "influxdb_perspective" \
    -p 8086:8086 \
    -e DOCKER_INFLUXDB_INIT_MODE=setup \
    -e DOCKER_INFLUXDB_INIT_USERNAME="admin" \
    -e DOCKER_INFLUXDB_INIT_PASSWORD="sudo-banana-404" \
    -e DOCKER_INFLUXDB_INIT_ORG="perspective" \
    -e DOCKER_INFLUXDB_INIT_BUCKET="smartgrid" \
    -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN="sudo-banana-404" \
    "influxdb:2"
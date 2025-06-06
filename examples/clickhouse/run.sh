#!/bin/bash

echo "CLICKHOUSE <> PERSPECTIVE INTEGRATION"
echo "-------------------------------------"
echo ""
echo "Actions:"
echo "  1. Run ClickHouse in Docker container."
echo "  2. Start a new python virtualenv and install the required packages."
echo "  3. Run ``producer.py`` (in background) to create a table in Clickhouse and insert data into it."
echo "  4. Run ``server.py`` to read data from Clickhouse and display it in Perspective."
echo "  5. Embed a Perspective Viewer on a HTML page to display an interactive dashboard."
echo -e "\nProceeding..."

# Run ClickHouse in Docker container.
echo "Pulling the latest ClickHouse docker image and starting the container..."
./docker.sh

# Start a new python virtualenv and install the required packages.
echo "Checking if virtualenv 'venv' already exists..."
if [ -d "venv" ]; then
    echo "Virtualenv found. Activating it..."
    source venv/bin/activate
    pip install -U -r requirements.txt
else
    echo "Creating a new python virtualenv and installing the required packages..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -U pip
    pip install -r requirements.txt
fi

# Run producer.py (in background) to create a table in Clickhouse and insert data into it.
python producer.py > /dev/null &
PRODUCER_PID=$!
echo "Producer is running in the background with PID=$PRODUCER_PID"

# Run server.py to read data from Clickhouse and display it in Perspective.
python server.py > /dev/null &
PERSPECTIVE_PID=$!
echo "Perspective server is running in the background with PID=$PERSPECTIVE_PID"

# Embed a Perspective Viewer on a HTML page to display an interactive dashboard.
echo "Attempting to open ``index.html``... alternatively, you can open it manually."
xdg-open index.html > /dev/null 2>&1 || open index.html > /dev/null 2>&1 || true

# check if producer and perspective server pids are running
sleep 2     # wait for the producer and perspective server to start
if ! ps -p $PRODUCER_PID > /dev/null || ! ps -p $PERSPECTIVE_PID > /dev/null; then
    echo "Producer or Perspective server has stopped. Exiting..."
    kill $PERSPECTIVE_PID $PRODUCER_PID
    deactivate
    exit 1
fi

# Wait for the user to press Ctrl+C to stop the Perspective server and the producer.
echo -e "\nPress Ctrl+C to stop the Perspective server and the producer."
trap "kill $PRODUCER_PID $PERSPECTIVE_PID; docker rm -vf prsp-clickhouse;" INT
wait
echo "Stopped the Perspective server and the producer."
echo "Exiting..."
deactivate
exit 0

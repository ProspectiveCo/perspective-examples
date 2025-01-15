#!/bin/bash

echo "CLICKHOUSE <> PERSPECTIVE INTEGRATION"
echo "-------------------------------------"
echo ""
echo "Actions:"
echo "  1. Run ClickHouse in Docker container."
echo "  2. Start a new python virtualenv and install the required packages."
echo "  3. Run producer.py (in background) to create a table in Clickhouse and insert data into it."
echo -e"\nProceeding..."

# Run ClickHouse in Docker container.
echo "Pulling the latest ClickHouse docker image and starting the container..."
./docker.sh

# Start a new python virtualenv and install the required packages.
echo "Creating a new python virtualenv and installing the required packages..."
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements.txt

# Run producer.py (in background) to create a table in Clickhouse and insert data into it.
python producer.py

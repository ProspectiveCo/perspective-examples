# Perspective <> Clickhouse Integration

This page demonstrates how to integrate Perspective with Clickhouse to visualize _fast-moving data streams_. By following this example, you will learn how to:

1. Set up a connection between Perspective and Clickhouse.
2. Embed an interactive `<perspective-viewer>` in your web apps enabling real-time _data visualization_ and _analysis_.

## Overview

[Clickhouse](https://clickhouse.com/docs) is an open-source columnar database management system (DBMS) designed for online analytical processing (OLAP) of queries. It is known for its high performance, scalability, and efficiency in handling large volumes of data. 

Main Technical Advantages:
- _Columnar Storage:_ Optimized for reading and writing large datasets, making it ideal for analytical queries.
- _Compression:_ Efficient data compression techniques reduce storage costs and improve query performance.
- _Distributed Processing:_ Supports distributed query execution across multiple nodes, enhancing scalability and fault tolerance.
- _Real-time Data Ingestion:_ Capable of ingesting millions of rows per second, making it suitable for real-time analytics.

<br/>

[Perspective](https://perspective.finos.org/) is an open-source data visualization library designed for real-time, fast-moving, and large data volumes. It provides a highly efficient and flexible way to visualize and analyze data streams in web applications.

Technical Advantages:
- _Real-time Visualization:_ Optimized for handling and rendering large datasets with minimal latency, making it ideal for dynamic and interactive data visualizations.
- _WebAssembly and Arrow:_ Utilizes WebAssembly and Apache Arrow to achieve unparalleled performance in data processing and rendering.
- _Multi-language Support:_ Offers support for multiple backends, including Python, Node.js, and Rust, allowing seamless integration into various development environments.

<br/>

**Primary Use-cases:**

Together, _Clickhouse_ and _Perspective_ are widely used in industries such as finance, telecommunications, and e-commerce for applications that require real-time analytics and reporting. It excels in scenarios involving fast-moving or time-series data, such as:

- **Monitoring and Observability:** Real-time monitoring of system metrics and logs.
- **Financial Analytics:** High-frequency trading data analysis and risk management.
- **User Behavior Analytics:** Tracking and analyzing user interactions on websites and applications.
- **Real-time Ad and Impression Analytics:** Analyzing ad performance and user impressions in real-time to optimize marketing strategies.

<br/>

## Demo Architecture & Components

![Architecture](imgs/perspective_clickhouse_demo_architecture.jpg)

This demo includes the following components:

- `docker.sh`: Starts a Clickhouse Docker container.
- `producer.py`: Generates a random stream of data and inserts it into Clickhouse every _250ms_.
- `perspective_server.py`: Reads the data stream from Clickhouse and sets up a _Perspective Server_. Multiple Perspective viewers (HTML clients) can then connect and provide interactive dashboards to users.
- `prsp-viewer.html`: Demonstrates how to embed an interactive `<perspective-viewer>` custom component in a web application.

<br/>

## Getting Started

#### 1. Start a Clickhouse Docker Container

Start by pulling and running a Clickhouse docker container to host our demo data.

```bash
./docker.sh
```

This script performs the following actions:
- Pulls the latest Clickhouse Docker image.
- Removes any existing Clickhouse container named `prsp-clickhouse`.
- Starts a new Clickhouse container with the necessary configurations.

#### 2. Set Up Python Virtual Environment

Next, create a new Python 3 virtual environment to manage the dependencies for this demo.

```bash
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

This script performs the following actions:
- Creates a new virtual environment named `venv`.
- Activates the virtual environment.
- Installs the required Python packages listed in `requirements.txt`.

The `clickhouse-connect` package is a Python client for Clickhouse, enabling efficient interaction with the Clickhouse database from your Python applications. It provides functionalities for executing queries, managing databases, and handling data ingestion.

The `perspective-python` package is the Python binding for Perspective, allowing you to create and manage Perspective tables, views, and servers. It facilitates the integration of real-time data visualization AND real-time publishing of data to Perspective Viewer clients.

#### 3. Run the Producer Script

The `producer.py` script is responsible for creating a Clickhouse table, generating random market data, and inserting it into the Clickhouse Docker container at regular intervals.

To run the producer script, execute the following command:

```bash
python producer.py
```

This script performs the following actions:
- Connects to the Clickhouse database running in the Docker container.
- Creates a table named `stock_values` to store the market data.
- Generates random market data, including timestamps, ticker symbols, client names, and stock prices.
- Inserts the generated data into the Clickhouse table every second.

This continuous data generation simulates a real-time data stream, which will be used by the Perspective server for visualization.
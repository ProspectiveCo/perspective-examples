# Perspective <> Clickhouse Integration

This page demonstrates how to integrate Perspective with Clickhouse to visualize _fast-moving data streams_. By following this example, you will learn how to:

1. set up a connection between Perspective and Clickhouse.
1. Embed an interactive `<perspective-viewer>` in your web apps enabling real-time _data visualization_ and _analysis_.

### Overview

#### Clickhouse

Clickhouse is an open-source columnar database management system (DBMS) designed for online analytical processing (OLAP) of queries. It is known for its high performance, scalability, and efficiency in handling large volumes of data. 

**Main Technical Advantages**
- _Columnar Storage:_ Optimized for reading and writing large datasets, making it ideal for analytical queries.
- _Compression:_ Efficient data compression techniques reduce storage costs and improve query performance.
- _Distributed Processing:_ Supports distributed query execution across multiple nodes, enhancing scalability and fault tolerance.
- _Real-time Data Ingestion:_ Capable of ingesting millions of rows per second, making it suitable for real-time analytics.

#### Primary Uses
Clickhouse is widely used in industries such as finance, telecommunications, and e-commerce for applications that require real-time analytics and reporting. It excels in scenarios involving fast-moving or time-series data, such as:
- **Monitoring and Observability**: Real-time monitoring of system metrics and logs.
- **Financial Analytics**: High-frequency trading data analysis and risk management.
- **User Behavior Analytics**: Tracking and analyzing user interactions on websites and applications.

By leveraging Clickhouse, organizations can gain insights from their data with minimal latency, enabling timely decision-making and improved operational efficiency.

### Demo Architecture & Components

As a while, this demo contains the following components:

- `docker.sh`: Which starts a Clickhouse docker container.
- `producer.py`: A script that generates a random stream of data and inserts it to Clickhouse every 500ms.
- `perspective_server.py`: A script which reads the data stream from Clickhouse and stands up a _Perspective Server_. Multiple perspective viewers (HTML clients) can then connect and provide interactive dashboards to users.


## Getting Started

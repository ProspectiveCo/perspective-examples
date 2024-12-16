# Visualizing InfluxDB Time-Series Data with Prospective

**Subtitle**: Bring powerful, real-time visualization to your InfluxDB data with the Prospective <> InfluxDB integration.

InfluxDB has long been a powerhouse for managing time-series data in domains like IoT, finance, and performance monitoring. But when it comes to visualizing this data in real time, users often face challenges. This where we come in! The company behind the open-source data visualization tool [Perspective](https://perspective.finos.org/). 

In this blog, we’ll introduce Prospective’s integration with InfluxDB, walking through a sample project to help you get started and explore its advanced visualization features.

## Why Prospective for InfluxDB?

### Built for Real-Time Data Streams

Prospective’s core product, Perspective, has been an open-source tool for over eight years, popular in fintech for its capabilities in real-time data visualization. Originally created to meet the needs of high-frequency trading applications, it offers data managers and data scientists a powerful way to visualize fast-moving data streams. 

Our new Prospective <> InfluxDB integration provides a Tableau-like interface, allowing InfluxDB users to perform analytics directly within their dashboards.

### Key Benefits of Prospective for InfluxDB Data

- **Real-Time Data Stream Visualization**: View InfluxDB data as it updates, making it ideal for applications that rely on fresh insights from time-series data.
- **Client-Side Interactive Interface**: All visualizations and user interactions happen on the client side. No extra back-end resources are needed for filters, group-bys, or other aggregations.
- **Local-First Design**: Users experience low-latency interactions as all data transformations occur directly on the client.
- **Ease of Integration**: Prospective provides `<perspective-viewer>`, a custom HTML element that developers can integrate easily into front-end applications, as well as a Python widget for Jupyter Notebooks.

---

## Getting Started: InfluxDB Integration Sample Project

In this example project, you’ll learn to connect InfluxDB to Prospective and explore real-time visualization options. The sample project is hosted on GitHub with a ready-to-go InfluxDB Docker container, data files, and scripts.

### Project Features

- **Direct integration** with InfluxDB data streams using Prospective’s connectors.
- **Continuous data pull** from InfluxDB with customizable intervals and Flux query settings.
- **Tableau-like interface** to explore and visualize time-series data in real time.
- Supports a range of data sizes to highlight performance with small to medium datasets.

---

## Setting Up Your Environment

### Requirements

- **Docker**: Ensure Docker is installed and running on your machine.
- **Trial License for Prospective**: Contact [hello@prospective.co](mailto:hello@prospective.co) with the subject “influxdb trial login” to request access.

### Step 1: Clone the Repository

Start by cloning the GitHub repository to access the sample project files:

```bash
git clone <https://github.com/ProspectiveCo/perspective-examples.git>
```

### Step 2: Start the InfluxDB Docker Container

Navigate to the directory and run the provided setup script:

```bash
cd examples/influxdb/influxdb-docker
./start_influxdb.sh
```

This script will:

- Pull the InfluxDB Docker image.
- Start a container configured with credentials and an initial admin token.
- Load a medium-sized dataset (`trades-md.tar.gz`) with 54,000 rows.

### Step 3: Access InfluxDB

Once the container is running, open the InfluxDB dashboard at [http://localhost:8086](http://localhost:8086/) and use these credentials:

- **Username**: `admin`
- **Password**: `sudo-banana-404`

From here, you can explore the data directly using InfluxDB’s query interface or access it via Prospective.

### Step 4: Stop the InfluxDB Container

When finished, stop and remove the container by running:

```bash
./stop_influxdb.sh

```

---

## Visualizing the Dataset with Prospective

To get started with Prospective:

1. **Log in to Prospective** at https://prospective.co/ with your trial license.
2. Go to the “SOURCES” tab and select **InfluxDB** as your data source.
3. Enter the following connection details:
    - **Endpoint URL**: `localhost:8080`
    - **Token**: `sudo-banana-404-not-found`
    - **Organization**: `perspective`

You should see the sample data imported into Prospective. Now, you can create dashboards by selecting chart types, adding group-by fields, and experimenting with different aggregations to gain insights into the dataset.

---

## Exploring the Interface

Prospective’s interactive Tableau-like interface allows you to:

- **Choose chart types**: Visualize your data with options like bar charts, line graphs, and heatmaps.
- **Group and filter**: Filter and group data to reveal trends, anomalies, and patterns.
- **Adjust aggregation functions**: Choose from a variety of aggregation functions to analyze data at different levels.

---

## Why Prospective?

Prospective’s integration with InfluxDB empowers data teams with a powerful, client-side visualization tool for real-time analytics:

- **Local-First Design**: All user interactions (filters, group-bys) are processed client-side, reducing reliance on additional server resources.
- **Real-Time Streaming**: Visualize data streams as they update, perfect for applications like finance, IoT, and monitoring.
- **Ease of Integration**: Prospective offers `<perspective-viewer>`, a custom HTML element that embeds directly into web applications, as well as a Jupyter Notebook widget for Python-based analysis.
- **Secure, Fast Analysis**: The local-first architecture supports secure analysis without requiring data transfer back to the server for each interaction.

---

With Prospective, InfluxDB users can enjoy a solution that brings powerful, client-side visual analytics to their real-time data streams. Additionally, Prospective Jupyter notebooks widget offers data engineers and scientists a cutting-edge tool for visual analytics in real-time.

---
# Visualizing Fast-Moving Kafka Data Streams with Perspective

Modern applications are increasingly dependent on real-time data for analytics, monitoring, and decision-making. Apache Kafka, a distributed streaming platform, has become a cornerstone for handling fast-moving data across industries. Combining Kafka with Perspective, a cutting-edge data visualization library, enables seamless, real-time insights from high-throughput data streams. In this post, we demonstrate how to visualize Kafka data streams using Perspective, leveraging its advanced features like WebAssembly, Apache Arrow, and efficient delta updates.

## Introduction to Kafka and Its Use in Real-Time Data

Kafka is designed to handle fast, continuous streams of data from producers to consumers. It is widely used for:
- *Event Streaming*: Tracking user activity, system logs, or IoT sensor data.
- *Data Pipelines*: Moving data between systems with low latency.
- *Real-Time Processing*: Enabling real-time analytics and monitoring.

Kafka’s ability to manage high-throughput data pipelines makes it an ideal match for real-time data visualization needs.

## Why Perspective for Kafka Data Visualization?

Perspective is a versatile data visualization library built to handle large and streaming datasets with ease. Its features include:
- *WebAssembly Performance*: High-speed data processing directly in the browser.
- *Apache Arrow Data Layer*: Efficient memory usage and seamless server-client communication.
- *Delta Updates*: Only the changes are sent over WebSockets, reducing bandwidth usage.
- *Tornado Integration*: A fast I/O layer to manage WebSocket communication efficiently.

These features ensure smooth, real-time updates in the browser, ideal for visualizing fast moving data in Kafka topics.

### Step-by-Step Instructions

This demo sets up a data pipeline to:
1.	Generate random stock market data using a Kafka producer.
2.	Consume this data using a Perspective Tornado server.
3.	Stream the data to a browser-based Perspective Viewer for live visualization.

#### 1. Set Up the Environment

Start by cloning the repository and installing dependencies:

```bash
git clone https://github.com/ProspectiveCo/perspective-examples.git

cd perspective-examples
python3 -m venv venv
source venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Launch Kafka

Spin up a Kafka instance using Docker:

```bash
cd examples/kafka
./kafka_container.sh
```

This script runs a Kafka docker container with a single broker, making it ready to process data. Please make sure you have docker pre-installed.

#### 3. Run the Kafka Producer

The `producer.py` script generates random stock data and writes it to the Kafka topic (ie: "stock_values") at regular intervals. 

To start the producer, start a new terminal and run:

```bash
cd perspective-examples
source venv/bin/activate

cd examples/kafka
python producer.py
```

You can kill the producer at any point with CTRL + C.

#### 4. Start the Perspective Server and Kafka consumer

The `perspective_server.py` script reads data from the Kafka topic and updates a Perspective Table. 

Start the server in a new terminal:

```bash
cd perspective-examples
source venv/bin/activate

cd examples/kafka
python perspective_server.py
```

You can kill the server at any point with CTRL + C.

#### 5 Loading the Perspective Dashboard

There are two ways that you can use the Perspective dashboard:

1. Using our [Prospective](https://prospective.co) (commercial) product. Prospective offers built-in data adapters that can connect to the Perspective server and many other data sources out-of-the-box. More instructions below.
2. Embedding the `<perspective-viewer>` (open source) custom html tag in your webapp.

#### 5.1 Leveraging the Prospective Platform

For users seeking a simplified and scalable experience, the [Prospective.co](https://prospective.co) platform offers a commercial version of Perspective. The platform provides:
- *Ease of Setup:* Connect to data sources with minimal configuration.
- *Advanced Visualization:* Build dashboards with out-of-the-box data adapters and drag-and-drop interfaces.
- *Built-in Jupyter Notebooks:* Ability to build custom data pipelines directly with Jupyter notebooks.
- *Sharing Feature:* Once dashboards are defined by the creator they can easily be shared (via custom URL link) with other team members.

To obtain a trail license of Prospective, please send email `hello@prospective.co` with a subject line: _"kafka blog"_

To use Prospective with this setup:
1.	Start the Prospective dashboard.
2.	Select “Perspective” as the data SOURCES (top right).
3.	Enter the WebSocket URL (http://localhost:8080/websocket) to stream data directly.

This simplifies the process, especially for teams and organizations that want to focus on insights rather than infrastructure.

#### 5.2 Alternative: Setting Up the Perspective Viewer for Embedding Analytics

Perspective uses a custom HTML element, `<perspective-viewer>`, making it easy to embed interactive analytics in web applications. 

Open `prsp-viewer.html` in your browser. This file contains the `<perspective-viewer>` HTML element, which connects to the Perspective server and displays the live data.


Key Points:
1.	Custom HTML Element: `<perspective-viewer>` allows for seamless integration of interactive visualizations into any web application.
2.	Dynamic Updates: The viewer fetches and updates data from the Perspective server using WebSockets.
3.	Apache Arrow: Data is transmitted efficiently using Apache Arrow, ensuring minimal latency and optimal performance.

This HTML structure enables a fully interactive dashboard that dynamically updates as new data flows in.


## How It Works

Producer Script
- Data Generation: The script creates random stock data, including timestamps, tickers, and prices.
- Kafka Producer: It sends this data to the stock_values topic in JSON format.

Key Code Snippet:

```python
def send_to_kafka(producer, topic):
    records = gen_data()
    for record in records:
        producer.produce(topic, json.dumps(record, cls=CustomJSONEncoder))
    producer.flush()
```

How the Perspective Server Works

The Perspective server acts as a bridge between Kafka and the Viewer, performing the following tasks:
1.	Kafka Consumer: The server reads messages from the Kafka topic (stock_values) at regular intervals.
2.	Perspective Table: Messages are inserted into a Perspective Table, which is designed for efficient data updates.
3.	WebSocket Communication: The Tornado server streams delta updates to the connected viewers via WebSockets.

Explained:

Periodic Data Refresh: Tornado’s PeriodicCallback is used to poll Kafka and update the Perspective table at 250ms intervals. `tornado.ioloop.PeriodicCallback()` method is used to setup the period data refreshes. The perspective table `table.update()` accepts a JSON array to update new rows received from the Kafka topic.

```python
def perspective_thread(perspective_server):
    table = client.table(schema, limit=2500, name="stock_values")
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])

    def updater():
        table.update(read_kafka(consumer, timeout=.1))

    tornado.ioloop.PeriodicCallback(callback=updater, callback_time=250).start()
```

Perspective Tornado Handler: This handles WebSocket connections and streams updates to the viewer.

```python
def make_app(perspective_server):
    return tornado.web.Application([
        (
            r"/websocket",
            perspective.handlers.tornado.PerspectiveTornadoHandler,
            {"perspective_server": perspective_server},
        ),
    ])
```

## Conclusion

By combining Kafka and Perspective, you can build powerful real-time data pipelines and visualize streaming data effortlessly. This setup is ideal for applications in finance, IoT, gaming, and beyond.

Try it out and let us know your thoughts! For more information, explore the Perspective [documentation](https://perspective.finos.org/).

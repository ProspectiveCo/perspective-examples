# TDengine <> Perspective Integration

This document explains how to configure Perspective with a TDengine data source. 

![TDengine - Perspective Architecture](imgs/tdengine_prsp_architecture.jpg)

<br/>

The architecture is as follow:

1. Instantiate a TDengine docker container and initialize it with data (using TAOS benchmark dataset).
2. Install TDengine python client libs (taospy).
3. Start a perspective-python server (`perspective_server.py`) which reads data periodically from TDengine and publishes it out to Perspective real-time Table via a Tornado Websocket.
4. Embed a Perspective viewer in HTML and connect to the backend Server.
5. Visualize and interact with TDengine data in real-time

<br/>

## Getting Started

### 1. Install TDengine client

Run the `install.sh` script to download and install the TDengine client libraries locally. This is necessary for the TDengine Python SDK (taospy) to function.

For more information on installing TDengine's client, please refer to [install client library](https://docs.tdengine.com/tdengine-reference/client-libraries/#install-client-driver) docs.

```sh
./install.sh
```

### 2. Check the client installation

After the install script runs, please verify if the everything is setup correctly.

You should see a symlink for `libtaos.so` in:

```sh
ls -l tdengine-client/driver/
```

Output:

```txt
total 68488
lrwxrwxrwx 1 warthog warthog       18 Jan  7 16:08 libtaos.so -> libtaos.so.3.3.5.0
-rwxr-xr-x 1 warthog warthog 59186032 Dec 31 03:42 libtaos.so.3.3.5.0
-rwxr-xr-x 1 warthog warthog 10937480 Dec 31 03:42 libtaosws.so
-rw-r--r-- 1 warthog warthog        8 Dec 31 03:42 vercomp.txt
```

Check if the client lib folder is correctly added to `$LD_LIBRARY_PATH`:

```sh
echo $LD_LIBRARY_PATH
```

`LD_LIBRARY_PATH` should have been added to your bash profile file. Please check to ensure that it is set properly.

If you don't see this line at the end of your `~/.bashrc` or `~/.bash_profile`, please add it:

```sh
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:***YOUR PATH***/tdengine-client/driver"
```

### 3. Start a TDengine Docker container

Run the `docker.sh` script to start a TDengine container. This script will also wait for the database to initialize.

```sh
./docker.sh
```

If you with to pre-populate the TDengine container with benchmark data. Run the script with the following flag:

```sh
./docker --benchmark
```

For complete information on running TDengine docker engine, please refer to [Get Started with TDengine Using Docker](https://docs.tdengine.com/get-started/deploy-in-docker/) docs.

### 4. Activate your virtualenv

`install.sh` already sets up a virtual environment for you and installs the TDengine `taospy` client. If you need to activate it manually, use the following commands:

```sh
source venv/bin/activate

pip install --upgrade pip
pip install --upgrade -r requirements.txt
```

### 4. Run the producer

Run the `producer.py` script to periodically insert data into TDengine. This script simulates real-time data ingestion by generating random data points and inserting them into the TDengine database.

```sh
python producer.py
```

### 5. Run Perspective Server

Run the `perspective_server.py` script to start a Perspective server (on a new terminal). This server will pull data from TDengine and stream it into a Tornado WebSocket.

```sh
python perspective_server.py
```

**NOTE:** Don't forget to activate your virtual environment before running the script.

<br/><br/>

## Explained

### `docker.sh`

The `docker.sh` script starts a TDengine Docker container. It waits for the database to initialize before returning. You can run this script with the following flags:

- `--benchmark`: Pre-populates the TDengine container with benchmark data.
- `--no-pull`: Skips pulling the TDengine Docker image.

```sh
./docker.sh --benchmark --no-pull
```

<br/>

### `producer.py`

The `producer.py` script connects to the TDengine database and inserts data at regular intervals. 

Here's how it works:

1. **Connecting to TDengine:**

```python
import taosws

TAOS_HOST = "localhost"
TAOS_PORT = 6041
TAOS_USER = "root"
TAOS_PASSWORD = "taosdata"

conn = taosws.connect(host=TAOS_HOST, port=TAOS_PORT, user=TAOS_USER, password=TAOS_PASSWORD)
```

2. **Creating a table:**

```python
create_table = """
CREATE TABLE IF NOT EXISTS stocks_values (
    timestamp TIMESTAMP,
    ticker NCHAR(10),
    client NCHAR(10),
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume INT UNSIGNED,
    date TIMESTAMP
)
"""
conn.execute(create_table)
```

3. **Inserting data:**

The `gen_data()` method generates a series of random stock trades on every call:

```python
import random
from datetime import datetime, date, timezone as tz

def gen_data():
    modifier = random.random() * random.randint(1, 50)
    return [{
        "timestamp": datetime.now(tz=tz.utc),
        "ticker": random.choice(["AAPL.N", "AMZN.N", "QQQ.N", "NVDA.N", "TSLA.N", "FB.N", "MSFT.N", "TLT.N", "XIV.N", "YY.N", "CSCO.N", "GOOGL.N", "PCLN.N"]),
        "client": random.choice(["Homer", "Marge", "Bart", "Lisa", "Maggie", "Moe", "Lenny", "Carl", "Krusty"]),
        "open": random.uniform(0, 75) + random.randint(0, 9) * modifier,
        "high": random.uniform(0, 105) + random.randint(1, 3) * modifier,
        "low": random.uniform(0, 85) + random.randint(1, 3) * modifier,
        "close": random.uniform(0, 90) + random.randint(1, 3) * modifier,
        "volume": random.randint(10_000, 100_000),
        "date": date.today(),
    } for _ in range(250)]
```

The `insert_data()` method uses prepared statements and batch inserts to enhance performance. By generating a batch of records at a time and using a prepared SQL statement, the method minimizes the overhead associated with multiple individual insert operations. This approach ensures efficient data insertion into the TDengine database.

```python
def insert_data(conn):
    records = gen_data()
    sql = "INSERT INTO stocks_values VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    stmt = conn.statement()
    stmt.prepare(sql)
    timestamps = [int(record['timestamp'].timestamp() * 1000) for record in records]
    tickers = [record['ticker'] for record in records]
    clients = [record['client'] for record in records]
    opens = [record['open'] for record in records]
    highs = [record['high'] for record in records]
    lows = [record['low'] for record in records]
    closes = [record['close'] for record in records]
    volumes = [record['volume'] for record in records]
    dates = [int(datetime.combine(record['date'], datetime.min.time()).timestamp() * 1000) for record in records]
    stmt.bind_param([
        taosws.millis_timestamps_to_column(timestamps),
        taosws.nchar_to_column(tickers),
        taosws.nchar_to_column(clients),
        taosws.floats_to_column(opens),
        taosws.floats_to_column(highs),
        taosws.floats_to_column(lows),
        taosws.floats_to_column(closes),
        taosws.ints_to_column(volumes),
        taosws.millis_timestamps_to_column(dates),
    ])
    stmt.add_batch()
    stmt.execute()


while True:
    insert_data(conn)
    time.sleep(0.25)
```

### `perspective_server.py`

The `perspective_server.py` script starts a Perspective server that reads data from TDengine and streams it to a Perspective Table via a Tornado WebSocket.

Here's how it works:

1. **Connecting to TDengine:**

```python
import taosws

TAOS_HOST = "localhost"
TAOS_PORT = 6041
TAOS_USER = "root"
TAOS_PASSWORD = "taosdata"

conn = taosws.connect(host=TAOS_HOST, port=TAOS_PORT, user=TAOS_USER, password=TAOS_PASSWORD)
```

2. **Reading data from TDengine:**

The `read_tdengine()` function queries the TDengine database and retrieves the latest stock data:

```python
def read_tdengine(conn):
    sql = """
        SELECT `timestamp`, ticker, client, open, high, low, close, volume, date
        FROM stocks_values
        WHERE `timestamp` >= NOW() - 1s
        ORDER BY `timestamp` DESC
        LIMIT 1000
    """
    res = conn.query(sql)
    data = [
        {
            "timestamp": convert_ts(row[0]),
            "ticker": row[1],
            "client": row[2],
            "open": row[3],
            "high": row[4],
            "low": row[5],
            "close": row[6],
            "volume": row[7],
            "date": convert_ts(row[8]),
        }
        for row in res
    ]
    return data
```

3. **Updating Perspective Table:**

The `perspective_thread()` function creates a Perspective table and updates it with new data from TDengine every 250 milliseconds:

```python
def perspective_thread(perspective_server, tdengine_conn):
    client = perspective_server.new_local_client()
    schema = {
        "timestamp": datetime,
        "ticker": str,
        "client": str,
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": int,
        "date": datetime,
    }
    table = client.table(schema, limit=1000, name="stock_values")
    
    def updater():
        data = read_tdengine(tdengine_conn)
        table.update(data)
    
    callback = tornado.ioloop.PeriodicCallback(callback=updater, callback_time=250)
    callback.start()
```

4. **Starting Tornado WebSocket Server:**

The `make_app()` function sets up a Tornado application with a WebSocket handler to serve the Perspective table:

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

5. **Running the server:**

The main block initializes the Perspective server, TDengine connection, and starts the Tornado IOLoop:

```python
if __name__ == "__main__":
    perspective_server = perspective.Server()
    tdengine_conn = create_tdengine_connection()
    app = make_app(perspective_server)
    app.listen(8080, address='0.0.0.0')
    
    loop = tornado.ioloop.IOLoop.current()
    loop.call_later(0, perspective_thread, perspective_server, tdengine_conn)
    loop.start()
```

## Helpful Resources

- **TDengine client library examples including python and node.js:** Download [TDengine's client library](https://docs.tdengine.com/tdengine-reference/client-libraries/#install-client-driver) tar file and unpack it. Look inside the examples directory for a comprehensive list of examples.

- [TDengine Docker Container with Data](https://docs.tdengine.com/get-started/deploy-in-docker/)
- [TDengine SQL Reference](https://docs.tdengine.com/basic-features/data-querying/)
- [Inserting data into TDengine](https://docs.tdengine.com/basic-features/data-ingestion/)

Next steps:
- [Streaming data from TDengine](https://docs.tdengine.com/advanced-features/stream-processing/)

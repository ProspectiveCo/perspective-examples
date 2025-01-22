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

import logging
import tornado.websocket
import tornado.web
import tornado.ioloop
from datetime import date, datetime
import perspective
import perspective.handlers.tornado
import json
import clickhouse_connect
from clickhouse_connect.driver.client import Client as ClickhouseClient


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('main')


# -----------------------------------------------------------------------------
# Clickhouse connection parameters
# -----------------------------------------------------------------------------
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "admin"
CLICKHOUSE_PASSWORD = "admin1234"
CLICKHOUSE_DATABASE = "test"

# -----------------------------------------------------------------------------
# Demo Parameters
# -----------------------------------------------------------------------------
TABLE_NAME = "stock_values"
INTERVAL = 1  # seconds


class CustomJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that serializes datetime and date objects
    """
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)


json.JSONEncoder.default = CustomJSONEncoder().default



def read_data_from_clickhouse(client: ClickhouseClient) -> list[dict]:
    """
    Read data from Clickhouse table and convert the results into a dict oriented list.
    """
    # Read the latest data from Clickhouse
    # NOTE:
    #   - If you like to read everything, remove the WHERE clause with the interval
    #   - Other interval options are: MINUTE, HOUR, DAY, WEEK, MONTH, YEAR
    sql = f"""
        SELECT 
            timestamp,
            ticker,
            client,
            open,
            high,
            low,
            close,
            volume,
            date
        FROM {CLICKHOUSE_DATABASE}.{TABLE_NAME}
        WHERE timestamp >= now() - INTERVAL 1 SECOND
        ORDER BY timestamp, ticker DESC
        """
    df = client.query_df(sql)
    logger.debug(f"Read {len(df)} rows from Clickhouse")
    return df.to_dict(orient='records')


def perspective_thread(perspective_server, clickhouse_client):
    """
    Create a new Perspective table and update it with new data every 50ms
    """
    # create a new Perspective table
    client = perspective_server.new_local_client()
    schema = {
        "timestamp": "datetime",
        "ticker": "string",
        "client": "string",
        "open": "float",
        "high": "float",
        "low": "float",
        "close": "float",
        "volume": "float",
        "date": "date",
    }
    # define the table schema
    table = client.table(
        schema,
        limit=1000,         # maximum number of rows in the table
        name=TABLE_NAME,    # table name. Use this with perspective-viewer on the client side
    )
    logger.info(f"Created new Perspective table={TABLE_NAME}")

    # update with new data every 50ms
    def updater():
        data = read_data_from_clickhouse(clickhouse_client)
        table.update(data)
        logger.debug(f"Updated Perspective table with {len(data)} rows")

    logger.info(f"Starting tornado ioloop update loop @ interval={INTERVAL:.3f}s")
    # start the periodic callback to update the table data
    callback = tornado.ioloop.PeriodicCallback(callback=updater, callback_time=(INTERVAL * 1000))
    callback.start()


def make_app(perspective_server):
    """
    Create a new Tornado application with a websocket handler that
    serves a Perspective table. PerspectiveTornadoHandler handles
    the websocket connection and streams the Perspective table changes 
    to the client.
    """
    return tornado.web.Application([
        (
            r"/websocket",                                              # websocket endpoint. Use this URL to configure the websocket client OR Prospective Server adapter
            perspective.handlers.tornado.PerspectiveTornadoHandler,     # PerspectiveTornadoHandler handles perspective table updates <-> websocket client
            {"perspective_server": perspective_server},                 # pass the perspective server to the handler
        ),
    ])


if __name__ == "__main__":
    # create a clickhouse client
    clickhouse_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
        connect_timeout=3,
        query_retries=1,
        query_limit=1_000_000,
        compress=True,
    )
    
    # test out the clickhouse client
    results = clickhouse_client.query('SELECT version()')
    version = results.result_rows[0][0]
    logger.info(f"Connected to Clickhouse version={version}")

    # create a new Perspective server
    perspective_server = perspective.Server()
    
    # setup and start the Tornado app
    app = make_app(perspective_server)
    app.listen(8080)
    
    logger.info("Listening on http://localhost:8080")
    try:
        # start the io loop
        loop = tornado.ioloop.IOLoop.current()
        loop.call_later(0, perspective_thread, perspective_server, clickhouse_client)
        loop.start()
    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt detected. Shutting down tornado server...")
        loop.stop()
        loop.close()
        logging.info("Shut down")

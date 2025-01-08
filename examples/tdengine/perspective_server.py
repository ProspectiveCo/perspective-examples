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
import taosws


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('main')


# =============================================================================
# TDengine connection parameters
# =============================================================================
TAOS_HOST = "localhost"                 # TDengine server host
TAOS_PORT = 6041                        # TDengine server port
TAOS_USER = "root"                      # TDengine username
TAOS_PASSWORD = "taosdata"              # TDengine password

# =============================================================================
# Perspective server parameters
# =============================================================================
PERSPECTIVE_TABLE_NAME = "meters"       # name of the Perspective table
PERSPECTIVE_REFRESH_RATE = 1000         # refresh rate in milliseconds


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


def create_tdengine_connection(
        host: str = TAOS_HOST,
        port: int = TAOS_PORT,
        user: str = TAOS_USER,
        password: str = TAOS_PASSWORD,
        ) -> taosws.Connection:
    conn = None
    try:
        conn = taosws.connect(
            user=user,
            password=password,
            host=host,
            port=port,
        )
        logger.info(f"Connected to tdengine successfully: {host}:{port}");
    except Exception as err:
        logger.error(f"Failed to connect to tdengine: {host}:{port} -- ErrMessage:{err}")
        raise err
    return conn


def read_tdengine(
        conn: taosws.Connection, 
        ) -> list:
    
    # convert the timestamp string to datetime object
    def convert_ts(ts) -> datetime:
        for fmt in ('%Y-%m-%d %H:%M:%S.%f %z', '%Y-%m-%d %H:%M:%S %z'):
            try:
                return datetime.strptime(ts, fmt)
            except ValueError:
                continue
        raise ValueError(f"Time data '{ts}' does not match any format")

    try:
        # query the database
        sql = """SELECT ts, current, voltage, phase, groupid, location FROM test.meters LIMIT 10000"""
        logger.debug(f"Executing query: {sql}")
        res = conn.query(sql)
        data = [
            {
            "ts": convert_ts(row[0]),
            "current": row[1],
            "voltage": row[2],
            "phase": row[3],
            "groupid": row[4],
            "location": row[5],
            }
            for row in res
        ]
        return data
    except Exception as err:
        logger.error(f"Failed to query tdengine: {err}")
        raise err



def perspective_thread(perspective_server: perspective.Server, tdengine_conn: taosws.Connection):
    """
    Create a new Perspective table and update it with new data every 50ms
    """
    # create a new Perspective table
    client = perspective_server.new_local_client()
    schema = {
        "ts": "datetime",
        "current": "float",
        "voltage": "integer",
        "phase": "float",
        "groupid": "integer",
        "location": "string",
    }
    # define the table schema
    table = client.table(
        schema,
        limit=10000,                    # maximum number of rows in the table
        name=PERSPECTIVE_TABLE_NAME,    # table name. Use this with perspective-viewer on the client side
    )
    logger.info("Created new Perspective table")

    # update with new data every 50ms
    def updater():
        data = read_tdengine(tdengine_conn)
        table.update(data)
        logger.debug(f"Updated Perspective table: {len(data)} rows")

    logger.info("Starting tornado ioloop update loop every 1 sec")
    # start the periodic callback to update the table data
    callback = tornado.ioloop.PeriodicCallback(callback=updater, callback_time=1000)
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
    logger.info("TDEngine <-> Perspective Demo")

    # create a new Perspective server
    logger.info("Creating new Perspective server")
    perspective_server = perspective.Server()
    # create the tdengine connection
    logger.info("Creating new TDEngine connection")
    tdengine_conn = create_tdengine_connection()

    # setup and start the Tornado app
    logger.info("Creating Tornado server")
    app = make_app(perspective_server)
    app.listen(8080, address='0.0.0.0')
    logger.info("Listening on http://localhost:8080")

    try:
        # start the io loop
        logger.info("Starting ioloop to update Perspective table data via tornado websocket...")
        loop = tornado.ioloop.IOLoop.current()
        loop.call_later(0, perspective_thread, perspective_server, tdengine_conn)
        loop.start()
    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt detected. Shutting down tornado server...")
        loop.stop()
        loop.close()
        logging.info("Shut down")

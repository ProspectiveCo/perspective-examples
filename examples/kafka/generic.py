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

import random
import logging
import tornado.websocket
import tornado.web
import tornado.ioloop
from datetime import date, datetime
import perspective
import perspective.handlers.tornado
import json


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('main')


SECURITIES = [
    "AAPL.N",
    "AMZN.N",
    "QQQ.N",
    "NVDA.N",
    "TSLA.N",
    "FB.N",
    "MSFT.N",
    "TLT.N",
    "XIV.N",
    "YY.N",
    "CSCO.N",
    "GOOGL.N",
    "PCLN.N",
]

CLIENTS = ["Homer", "Marge", "Bart", "Lisa", "Maggie", "Moe", "Lenny", "Carl", "Krusty"]


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


def data_source():
    """
    Generate random data for the Perspective table
    """
    modifier = random.random() * random.randint(1, 50)
    return [{
        "name": random.choice(SECURITIES),
        "client": random.choice(CLIENTS),
        "open": random.uniform(0, 75) + random.randint(0, 9) * modifier,
        "high": random.uniform(0, 105) + random.randint(1, 3) * modifier,
        "low": random.uniform(0, 85) + random.randint(1, 3) * modifier,
        "close": random.uniform(0, 90) + random.randint(1, 3) * modifier,
        "lastUpdate": datetime.now(),
        "date": date.today(),
    } for _ in range(5)]


def perspective_thread(perspective_server):
    """
    Create a new Perspective table and update it with new data every 50ms
    """
    # create a new Perspective table
    client = perspective_server.new_local_client()
    # define the table schema
    table = client.table(
        {
            "name": "string",
            "client": "string",
            "open": "float",
            "high": "float",
            "low": "float",
            "close": "float",
            "lastUpdate": "datetime",
            "date": "date",
        },
        limit=2500,                 # maximum number of rows in the table
        name="rand_stocks",         # table name. Use this with perspective-viewer on the client side
    )
    logger.info("Created new Perspective table")

    # update with new data every 50ms
    def updater():
        table.update(data_source())

    logger.info("Starting tornado ioloop update loop every 50ms")
    # start the periodic callback to update the table data
    callback = tornado.ioloop.PeriodicCallback(callback=updater, callback_time=50)
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
    # create a new Perspective server
    perspective_server = perspective.Server()
    # setup and start the Tornado app
    app = make_app(perspective_server)
    app.listen(8080)
    logger.info("Listening on http://localhost:8080")
    try:
        # start the io loop
        loop = tornado.ioloop.IOLoop.current()
        loop.call_later(0, perspective_thread, perspective_server)
        loop.start()
    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt detected. Shutting down tornado server...")
        loop.stop()
        loop.close()
        logging.info("Shut down")

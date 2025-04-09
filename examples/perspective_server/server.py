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
import tornado.web
import tornado.ioloop
from datetime import date, datetime
import perspective
import perspective.handlers.tornado
import json


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('main')

# --- Demo constants ---
NUMBER_OF_ROWS = 100        # number of rows to generate per interval
INTERVAL = 250              # milliseconds

# --- data generation ---
PERSPECTIVE_TABLE_NAME = "stock_values"  # name of the perspective table
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
    "NFLX.N",
    "BABA.N",
    "INTC.N",
    "V.N",
    "JPM.N",
    "WMT.N",
    "DIS.N",
    "PYPL.N",
    "ADBE.N",
    "CMCSA.N",
    "PEP.N",
    "KO.N",
    "NKE.N",
    "MRK.N",
    "PFE.N",
    "T.N",
    "VZ.N",
    "ORCL.N",
    "IBM.N",
    "CRM.N",
    "BA.N",
    "GE.N",
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


def generate_data(nrows: int = NUMBER_OF_ROWS) -> list:
    """
    Generate random data for the Perspective table
    """
    modifier = random.random() * random.randint(1, 50)
    return [{
        "ticker": random.choice(SECURITIES),
        "client": random.choice(CLIENTS),
        "open": random.uniform(0, 75) + random.randint(0, 9) * modifier,
        "high": random.uniform(0, 105) + random.randint(1, 3) * modifier,
        "low": random.uniform(0, 85) + random.randint(1, 3) * modifier,
        "close": random.uniform(0, 90) + random.randint(1, 3) * modifier,
        "lastUpdate": datetime.now(),
        "date": date.today(),
    } for _ in range(nrows)]


def create_perspective_table(perspective_server):
    """
    Create a new Perspective table.
    """
    # create a new Perspective table
    client = perspective_server.new_local_client()
    # define the table schema
    table = client.table(
        {
            "ticker": "string",
            "client": "string",
            "open": "float",
            "high": "float",
            "low": "float",
            "close": "float",
            "lastUpdate": "datetime",
            "date": "date",
        },
        limit=2500,                     # maximum number of rows in the table
        name=PERSPECTIVE_TABLE_NAME,    # table name. Use this with perspective-viewer on the client side
        format="json",                  # table format. possible values: "json", "arrow", "ndjson"
    )
    logger.info("Created new Perspective table")
    return table


def update_perspective_table(perspective_table):
    """
    Update the Perspective table with new data 
    """
    # generate new data and update the table
    data = generate_data()
    perspective_table.update(data)


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

    # create the perspective table
    logger.info("Creating Perspective table")
    table = create_perspective_table(perspective_server)

    # start the websocket server
    try:
        # strat a new perspective table update loop
        logger.info("Starting perspective table update loop")
        perspective_loop = tornado.ioloop.PeriodicCallback(
            callback=lambda: update_perspective_table(table),
            callback_time=INTERVAL,
        )
        perspective_loop.start()
        # start the io loop
        loop = tornado.ioloop.IOLoop.current()
        loop.start()
    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt detected. Shutting down tornado server...")
        # stop the update loop and io loop
        perspective_loop.stop()
        loop.stop()
        loop.close()
        logging.info("Shut down")

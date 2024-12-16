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
from confluent_kafka import Consumer, KafkaException


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('main')


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "stock_values"
KAFKA_GROUP_ID = "stock_values_consumer"


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



def read_kafka(consumer: Consumer, timeout: float = 0.250):
    """
    Read latest records from a Kafka topic and return them as an array of dict objects.
    """
    messages = []
    try:
        while True:
            msg = consumer.poll(timeout)
            if msg is None or (msg.error() and msg.error().code() == KafkaException._PARTITION_EOF):
                break
            messages.append(json.loads(msg.value().decode('utf-8')))
    except KafkaException as e:
        logger.warning(f"KafkaException: {e}")
    except Exception as e:
        logger.warning(f"Exception: {e}")
    # print(messages)
    return messages


def perspective_thread(perspective_server):
    """
    Create a new Perspective table and update it with new data every 50ms
    """
    # create a new Perspective table
    client = perspective_server.new_local_client()
    schema = {
        "timestamp": "datetime",
        "ticker": "string",
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
        limit=2500,                 # maximum number of rows in the table
        name="stock_values",        # table name. Use this with perspective-viewer on the client side
    )
    logger.info("Created new Perspective table")

    # create a kafka consumer
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',  # Start reading from the beginning of the topic
        'security.protocol': 'PLAINTEXT',
    }
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])

    # update with new data every 50ms
    def updater():
        table.update(read_kafka(consumer, timeout=.1))

    logger.info("Starting tornado ioloop update loop every 50ms")
    # start the periodic callback to update the table data
    callback = tornado.ioloop.PeriodicCallback(callback=updater, callback_time=250)
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

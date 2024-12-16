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

"""
This is an example of how to use the StockStreamDataGenerator class to generate stock data and send it to a Kafka topic.

The StockStreamDataGenerator class is a subclass of StreamGenerator, which fetches recent stock values from the Alpha Vantage API
and replays it.
"""


import sys
from datetime import datetime, timedelta
from time import sleep
try:
    from utils.logger import logger
except ImportError:
    sys.path.append('../../../')
finally:
    from utils.logger import logger

from utils.config_loader import config
from generators.stocks.stock_values import StockValuesStreamGenerator
from writers.kafka_writer import KafkaWriter


TICKERS = config['stocks_generator']['socials_tickers']
START_TIME = datetime.now().replace(microsecond=0) - timedelta(seconds=0)
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'stock_values'


def main():
    logger.info('-' * 80)
    logger.info("STOCKS -> KAFKA GENERATOR")

    # create a new stocks generator
    logger.info("Creating new StockValuesStreamGenerator...")
    generator = StockValuesStreamGenerator(
        tickers=TICKERS,
        interval=0.250,
        start_time=START_TIME,
        ticker_interval='INTRADAY',
        intraday_interval='60min',
    )

    # add a Kafka writer to the generator
    logger.info("Adding KafkaWriter to the generator...")
    kafka_writer = KafkaWriter(
        topic=KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
    )
    generator.add_subscriber(kafka_writer)

    # start the generator
    logger.info("Starting the generator thread...")
    generator.start()
    try:
        while generator.is_running():
            sleep(1)
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt: Stopping generator...")
        generator.stop()
        kafka_writer.close()
        logger.info("Generator stopped")


if __name__ == "__main__":
    main()

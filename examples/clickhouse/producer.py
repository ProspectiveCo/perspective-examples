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
from datetime import date, datetime
from time import sleep
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
CLICKHOUSE_TABLE = "stock_values"


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


# json.JSONEncoder.default = CustomJSONEncoder().default


def gen_data():
    """
    Generate random data for the Perspective table
    """
    modifier = random.random() * random.randint(1, 50)
    return [{
        "timestamp": datetime.now(),
        "ticker": random.choice(SECURITIES),
        "client": random.choice(CLIENTS),
        "open": random.uniform(0, 75) + random.randint(0, 9) * modifier,
        "high": random.uniform(0, 105) + random.randint(1, 3) * modifier,
        "low": random.uniform(0, 85) + random.randint(1, 3) * modifier,
        "close": random.uniform(0, 90) + random.randint(1, 3) * modifier,
        "volume": random.randint(10_000, 100_000),
        "date": date.today(),
    } for _ in range(5)]


def clickhouse_create_table(
        client: ClickhouseClient, 
        table_name: str = "stock_values"
        ) -> None:
    """
    Create a Clickhouse table to store the data. Drop the table if it already exists.
    """
    pass


def send_to_kafka(producer, topic):
    """
    Send data to Kafka topic
    """
    records = gen_data()
    for record in records:
        producer.produce(topic, json.dumps(record, cls=CustomJSONEncoder))
    producer.flush()
    logger.debug(f"write - Wrote {len(records)} records to topic {topic}")


def main():
    # create a clickhouse client
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )
    # test out the client
    results = client.query('SELECT version()')
    # print(dir(results))
    print(results.column_names)

    for result in results.result_rows:
        print(result)

    # clickhouse_create_table(CLICKHOUSE_TABLE)

    # interval = 0.250
    # progress_counter = 0
    # logger.info(f"Sending data to Kafka topic every {interval:.3f}s...")
    # try:
    #     while True:
    #         send_to_kafka(producer, CLICKHOUSE_TABLE)
    #         progress_counter += 1
    #         print('.', end='' if progress_counter % 80 else '\n', flush=True)
    #         sleep(interval)
    # except KeyboardInterrupt:
    #     logger.info("Shutting down...")


if __name__ == "__main__":
    main()
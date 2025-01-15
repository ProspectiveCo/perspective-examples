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

INTERVAL = 1  # seconds


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
    Generate random data
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


def create_database(
        client: ClickhouseClient, 
        database_name: str = CLICKHOUSE_DATABASE
        ) -> None:
    """
    Create a Clickhouse database. Drop the database if it already exists.
    """
    # create the database
    sql = f"CREATE DATABASE IF NOT EXISTS {database_name}"
    client.command(sql)
    logger.info(f"Clickhouse - Created database {database_name}")


def create_table(
        client: ClickhouseClient, 
        table_name: str = CLICKHOUSE_TABLE
        ) -> None:
    """
    Create a Clickhouse table to store the data. Drop the table if it already exists.
    """
    # drop the table if it already exists
    sql = f"DROP TABLE IF EXISTS {table_name}"
    try:
        client.command(sql)
        logger.info(f"Clikchouse - Dropped table {table_name}")
    except Exception as e:
        pass # table does not exist
    # create the table
    sql = f"""
    CREATE TABLE {table_name} (
        timestamp DateTime,
        ticker String,
        client String,
        open Float32,
        high Float32,
        low Float32,
        close Float32,
        volume UInt32,
        date Date
    ) ENGINE MergeTree()
    ORDER BY (timestamp, ticker)
    """
    client.command(sql)
    logger.info(f"Clickhouse - Created table {table_name}")


def insert_data(
        client: ClickhouseClient, 
        table_name: str = CLICKHOUSE_TABLE
        ) -> None:
    """
    Insert data into the Clickhouse table
    """
    records = gen_data()
    # incoming data is a list of dictionaries -- convert to list of list of values
    records = [list(record.values()) for record in records]
    # insert the data
    result = client.insert(
        table=table_name, 
        data=records, 
        database=CLICKHOUSE_DATABASE, 
        column_names=['timestamp', 'ticker', 'client', 'open', 'high', 'low', 'close', 'volume', 'date'],
        )
    logger.debug(f"Clickhouse - Wrote rows={result.written_rows}, bytes={result.written_bytes}, table={table_name}, database={CLICKHOUSE_DATABASE}")


def main():
    """
    Create a Clickhouse client, create the database and table, and insert data into the table
    """
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
    version = results.result_rows[0][0]
    logger.info(f"Clickhouse - Connected to Clickhouse version={version}")
    
    # create the database and table
    create_database(client, database_name=CLICKHOUSE_DATABASE)
    create_table(client, table_name=CLICKHOUSE_TABLE)

    progress_counter = 0
    logger.info(f"Inserting data to Clickhouse @ interval={INTERVAL:.3f}s...")
    try:
        while True:
            insert_data(client, table_name=CLICKHOUSE_TABLE)
            progress_counter += 1
            print('.', end='' if progress_counter % 80 else '\n', flush=True)
            sleep(INTERVAL)
    except KeyboardInterrupt:
        logger.info(f"Shutting down...")


if __name__ == "__main__":
    main()
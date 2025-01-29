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
from datetime import timezone as tz
from time import sleep
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

TAOS_DATABASE = "stocks"                # TDengine database name
TAOS_TABLENAME = "stocks"               # TDengine table name

# =============================================================================
# Data generation parameters
# =============================================================================
INTERVAL = 1                        # seconds. insert data every INTERVAL seconds
NUM_ROWS_PER_INTERVAL = 250         # number of rows to insert every INTERVAL seconds
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
        "timestamp": datetime.now(tz=tz.utc),
        "ticker": random.choice(SECURITIES),
        "client": random.choice(CLIENTS),
        "open": random.uniform(0, 75) + random.randint(0, 9) * modifier,
        "high": random.uniform(0, 105) + random.randint(1, 3) * modifier,
        "low": random.uniform(0, 85) + random.randint(1, 3) * modifier,
        "close": random.uniform(0, 90) + random.randint(1, 3) * modifier,
        "volume": random.randint(10_000, 100_000),
        "date": date.today(),
    } for _ in range(NUM_ROWS_PER_INTERVAL)]


def create_database(
        conn, 
        database_name: str = TAOS_DATABASE
        ) -> None:
    """
    Create a TDengine database. Drop the database if it already exists.
    """
    
    # create the database
    conn.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    conn.execute(f"USE {database_name}")
    logger.info(f"TDengine - Created database {database_name}")


def create_table(
        conn, 
        table_name: str = TAOS_TABLENAME
        ) -> None:
    """
    Create a TDengine table to store the data. Drop the table if it already exists.
    """
    # drop the table if it already exists
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    # create the table
    sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            `timestamp` TIMESTAMP,
            `ticker` NCHAR(10),
            `client` NCHAR(10),
            `open` FLOAT,
            `high` FLOAT,
            `low` FLOAT,
            `close` FLOAT,
            `volume` INT UNSIGNED,
            `date` TIMESTAMP
        )
    """
    conn.execute(sql)
    logger.info(f"TDengine - Created table {table_name}")


def insert_data(
        conn, 
        table_name: str = TAOS_TABLENAME
        ) -> None:
    """
    Insert data into the TDengine table
    """
    records = gen_data()
    
    # prepare a parameterized SQL statement
    sql = f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    stmt = conn.statement()
    stmt.prepare(sql)
    
    # prepare the columns into their respective lists
    timestamps = [int(record['timestamp'].timestamp() * 1000) for record in records]
    tickers = [record['ticker'] for record in records]
    clients = [record['client'] for record in records]
    opens = [record['open'] for record in records]
    highs = [record['high'] for record in records]
    lows = [record['low'] for record in records]
    closes = [record['close'] for record in records]
    volumes = [record['volume'] for record in records]
    dates = [int(datetime.combine(record['date'], datetime.min.time()).timestamp() * 1000) for record in records]

    # bind the parameters and execute the statement
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
        ]
    )
    # send the batch for insert
    stmt.add_batch()
    stmt.execute()
    logger.debug(f"TDengine - Wrote {len(records)} rows to table {table_name}")



def main():
    """
    Create a TDengine client, create the database and table, and insert data into the table
    """
    # create a tdengine websocket client
    conn = taosws.connect(host=TAOS_HOST, port=TAOS_PORT, user=TAOS_USER, password=TAOS_PASSWORD)

    # create the database and table
    create_database(conn, database_name=TAOS_DATABASE)
    create_table(conn, table_name=TAOS_TABLENAME)
    
    progress_counter = 0
    logger.info(f"Inserting data to TDengine @ interval={INTERVAL:.3f}s...")
    try:
        while True:
            insert_data(conn, table_name=TAOS_TABLENAME)
            progress_counter += 1
            print('.', end='' if progress_counter % 80 else '\n', flush=True)
            sleep(INTERVAL)
    except KeyboardInterrupt:
        logger.info(f"Shutting down...")


if __name__ == "__main__":
    main()
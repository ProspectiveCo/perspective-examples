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
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic



logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('main')


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "stock_values"


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


def create_kafka_producer(
        bootstrap_servers,
        topic: str,
        client_id: str = None,
        security_protocol: str = 'PLAINTEXT',
        sasl_mechanism: str = None,
        sasl_username: str = None,
        sasl_password: str = None,
        ):
    """
    Create a Kafka producer
    """
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': client_id,
        'security.protocol': security_protocol,
    }

    if sasl_mechanism and sasl_username and sasl_password:
        conf.update({
            'sasl.mechanism': sasl_mechanism,
            'sasl.username': sasl_username,
            'sasl.password': sasl_password,
        })
    # create a producer
    producer = Producer(conf)
    logger.info("init - Kafka producer initialized")
    return producer


def create_kafka_topic_if_not_exists(topic: str):
    """
    Create a Kafka topic if it does not exist
    """
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    topic_metadata = admin_client.list_topics(timeout=10)
    if topic not in topic_metadata.topics:
        new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        logger.info(f"init - Created new topic {topic}")


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
    # create kafka topic and producer
    create_kafka_topic_if_not_exists(KAFKA_TOPIC)
    producer = create_kafka_producer(KAFKA_BROKER, KAFKA_TOPIC)

    interval = 0.250
    progress_counter = 0
    logger.info(f"Sending data to Kafka topic every {interval:.3f}s...")
    try:
        while True:
            send_to_kafka(producer, KAFKA_TOPIC)
            progress_counter += 1
            print('.', end='' if progress_counter % 80 else '\n', flush=True)
            sleep(interval)
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == "__main__":
    main()
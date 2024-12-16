from confluent_kafka import Producer
import pandas as pd
import json
from .base import DataWriter
from confluent_kafka.admin import AdminClient, NewTopic
from utils.logger import logger


class KafkaWriter(DataWriter):
    def __init__(self, 
                 topic: str,
                 bootstrap_servers: str,
                 client_id: str = None,
                 security_protocol: str = 'PLAINTEXT',
                 sasl_mechanism: str = None,
                 sasl_username: str = None,
                 sasl_password: str = None,
                 **kwargs
                 ) -> None:
        """
        Initialize a KafkaWriter instance.

        Args:
            topic (str): The Kafka topic to which messages will be sent.
            bootstrap_servers (str): A comma-separated list of Kafka bootstrap servers.
            client_id (str, optional): An optional identifier for the Kafka client. Defaults to None.
            security_protocol (str, optional): Protocol used to communicate with brokers. Defaults to 'PLAINTEXT'.
            sasl_mechanism (str, optional): SASL mechanism to use for authentication. Defaults to None.
            sasl_username (str, optional): Username for SASL authentication. Defaults to None.
            sasl_password (str, optional): Password for SASL authentication. Defaults to None.
            **kwargs: Additional keyword arguments to pass to the superclass initializer.

        Returns:
            None
        """
        super().__init__(**kwargs)
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password

        # setup the Kafka producer
        # Kafka producer configuration
        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': self.client_id,
            'security.protocol': self.security_protocol,
        }
        if self.sasl_mechanism and self.sasl_username and self.sasl_password:
            self.producer_config.update({
                'sasl.mechanism': self.sasl_mechanism,
                'sasl.username': self.sasl_username,
                'sasl.password': self.sasl_password,
            })
        self.producer = Producer(self.producer_config)
        logger.info("KafkaWriter::init - Kafka producer initialized")

        # Check if the topic exists and create it if it does not
        admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        topic_metadata = admin_client.list_topics(timeout=10)
        if self.topic not in topic_metadata.topics:
            new_topic = NewTopic(self.topic, num_partitions=1, replication_factor=1)
            admin_client.create_topics([new_topic])
            logger.info(f"KafkaWriter::init - Created new topic {self.topic}")

    def write(self, data: pd.DataFrame) -> None:
        # Convert DataFrame to JSON records
        records = data.to_json(orient='records', date_format='iso')
        records = json.loads(records)

        # Produce messages to Kafka
        for record in records:
            self.producer.produce(self.topic, json.dumps(record).encode('utf-8'))
            self.producer.poll(0)

        # Ensure all messages are delivered
        self.producer.flush()
        logger.debug(f"KafkaWriter::write - Wrote {len(records)} records to topic {self.topic}")

    def close(self) -> None:
        self.producer.flush()

    @staticmethod
    def required_parameters() -> dict[str, str]:
        return {
            "topic": "str",
            "bootstrap_servers": "str",
        }

    @staticmethod
    def from_config(config: dict) -> 'DataWriter':
        return KafkaWriter(**config)
    

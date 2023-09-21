import time
from os import getenv

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from src.logger import LogManager

log = LogManager().get_logger(name=__name__)

DELAY = 2  # Delay in sec between attemts to connect or do something


class KafkaClient:
    __slots__ = ("properties",)

    def __init__(self) -> None:
        self.properties = dict(
            bootstrap_servers=f'{getenv("KAFKA_HOST")}:{getenv("KAFKA_PORT")}',
        )

    def get_producer(self) -> KafkaProducer:
        log.debug("Connecting to Kafka cluster in producer mode")

        for i in range(1, 10 + 1):
            try:
                producer = KafkaProducer(**self.properties)

                assert producer.bootstrap_connected(), "Not connected!"
                return producer

            except NoBrokersAvailable as err:
                if i == 10:
                    raise err
                else:
                    log.warning(f"{err}. Retrying...")
                    time.sleep(DELAY)

                    continue

    def get_consumer(self) -> KafkaConsumer:
        log.debug("Connecting to Kafka cluster in consumer mode")

        for i in range(1, 10 + 1):
            try:
                consumer = KafkaConsumer(
                    **self.properties, auto_offset_reset="earliest"
                )

                assert consumer.bootstrap_connected(), "Not connected!"
                return consumer

            except NoBrokersAvailable as err:
                if i == 10:
                    raise err
                else:
                    log.warning(f"{err}. Retrying...")
                    time.sleep(DELAY)

                    continue

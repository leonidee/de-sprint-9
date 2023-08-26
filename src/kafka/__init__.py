from os import getenv
import time
import kafka
from src.logger import LogManager

log = LogManager().get_logger(name=__name__)


class KafkaClient:
    __slots__ = ("properties",)

    def __init__(self) -> None:
        self.properties = dict(
            bootstrap_servers=getenv("YC_KAFKA_BOOTSTRAP_SERVERS"),
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=getenv("YC_KAFKA_USERNAME"),
            sasl_plain_password=getenv("YC_KAFKA_PASSWORD"),
            ssl_cafile=getenv("CERTIFICATE_PATH"),
        )

    def get_producer(self) -> kafka.KafkaProducer:
        log.debug("Connecting to Kafka cluster in producer mode")

        for i in range(1, 10+1):
            try:
                return kafka.KafkaProducer(**self.properties)
            except kafka.errors.NoBrokersAvailable as err:

                if i == 10:
                    raise err
                else:
                    log.warning(f"{err}. Retrying...")
                    time.sleep(10)

                    continue

    def get_consumer(self) -> kafka.KafkaConsumer:
        log.debug("Connecting to Kafka cluster in consumer mode")

        for i in range(1, 10+1):
            try:
                return kafka.KafkaConsumer(
                    **self.properties,
                    auto_offset_reset="earliest",  # earliest / latest
                )
            except kafka.errors.NoBrokersAvailable as err:

                if i == 10:
                    raise err
                else:
                    log.warning(f"{err}. Retrying...")
                    time.sleep(10)

                    continue


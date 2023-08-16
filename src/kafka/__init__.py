from os import getenv

import kafka
from src.logger import LogManager

log = LogManager().get_logger(name=__name__)


class KafkaClient:

    def __init__(self) -> None:
        ...

    def get_producer(self) -> kafka.KafkaProducer:
        log.debug("Connecting to Kafka cluster producer mode")

        return kafka.KafkaProducer(
            bootstrap_servers=getenv("KAFKA_BOOTSTRAP_SERVERS"),
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=getenv("KAFKA_USERNAME"),
            sasl_plain_password=getenv("KAFKA_PASSWORD"),
            ssl_cafile=getenv("CERTIFICATE_PATH"),
        )

    def get_consumer(self) -> kafka.KafkaConsumer:
        log.debug("Connecting to Kafka cluster in consumer mode")

        return kafka.KafkaConsumer(
            bootstrap_servers=getenv("KAFKA_BOOTSTRAP_SERVERS"),
            auto_offset_reset="earliest",  # earliest / latest
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=getenv("KAFKA_USERNAME"),
            sasl_plain_password=getenv("KAFKA_PASSWORD"),
            ssl_cafile=getenv("CERTIFICATE_PATH"),
        )

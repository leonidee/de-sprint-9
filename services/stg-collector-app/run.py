import sys

import kafka

sys.path.append("/app")
from src.kafka import KafkaClient
from src.logger import LogManager
from src.redis import RedisClient

log = LogManager().get_logger(__name__)


def main() -> ...:
    consumer = KafkaClient().get_consumer()

    consumer.subscribe("order-service_orders")

    poll = consumer.poll(timeout_ms=1000)

    for tp, msgs in poll.items():
        for msg in msgs:
            print(msg.value)


if __name__ == "__main__":
    main()
import sys

import json

sys.path.append("/app")
from src.kafka import KafkaClient

def main() -> ...:
    producer = KafkaClient().get_producer()

    with open('/app/tests/kafka/producer/kafka-input-msg.json', 'r') as f:
        value = json.load(f)

    producer.send(
            topic="order-service_orders",
            value=json.dumps(value).encode("utf-8")
        )


if __name__ == "__main__":
    main()
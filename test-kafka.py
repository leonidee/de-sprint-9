import json
import os
import time

import dotenv
import kafka

dotenv.load_dotenv()


def main() -> ...:
    consumer = kafka.KafkaConsumer(  # https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        auto_offset_reset="earliest",  # earliest / latest
        enable_auto_commit=True,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username="producer_consumer",
        sasl_plain_password="4kkDxgdnwuagovH7JuQy",
        ssl_cafile="./CA.pem",
    )

    consumer.subscribe("order-service_orders")
    # Message exampole:
    # ConsumerRecord(
    #     topic="order-service_orders",
    #     partition=0,
    #     offset=153,
    #     timestamp=1691820435717,
    #     timestamp_type=0,
    #     key=None,
    #     value='{"object_id": 2301366, "object_type": "order", "sent_dttm": "2023-08-12 06:07:15", "payload": {"restaurant": {"id": "626a81cfefa404208fe9abae"}, "date": "2023-08-05 06:29:05", "user": {"id": "626a81ce9a8cd1920641e2a9"}, "order_items": [{"id": "6276e8cd0cf48b4cded00871", "name": "\\u041a\\u0420\\u0423\\u0410\\u0421\\u0421\\u0410\\u041d \\u0421 \\u0412\\u0415\\u0422\\u0427\\u0418\\u041d\\u041e\\u0419", "price": 120, "quantity": 5}], "bonus_payment": 0, "cost": 600, "payment": 600, "bonus_grant": 0, "statuses": [{"status": "CLOSED", "dttm": "2023-08-05 06:29:05"}, {"status": "DELIVERING", "dttm": "2023-08-05 06:12:37"}, {"status": "COOKING", "dttm": "2023-08-05 05:29:19"}, {"status": "OPEN", "dttm": "2023-08-05 04:31:26"}], "final_status": "CLOSED", "update_ts": "2023-08-05 06:29:05"}}',
    #     headers=[],
    #     checksum=None,
    #     serialized_key_size=-1,
    #     serialized_value_size=776,
    #     serialized_header_size=-1,
    # )

    timeout = 1_000
    msgs = consumer.poll(timeout_ms=timeout)

    print(type(msgs))
    print(msgs)

    # for msg in msgs.items():
    #     # print(msg.value)
    #     for i in msg:
    #         #     print("")
    #         print(i.value)

    # while True:
    #     try:
    #         msg = consumer.poll(timeout_ms=timeout)

    #         if not msg:
    #             print("no messages")
    #             time.sleep(3)
    #             continue

    #     except Exception:
    #         raise Exception(msg.error())

    #     val = msg.value.decode()
    #     print(val)

    consumer.close()


if __name__ == "__main__":
    main()

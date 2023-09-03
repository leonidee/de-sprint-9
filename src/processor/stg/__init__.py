from __future__ import annotations

import dataclasses
import json
import sys
from datetime import datetime
from json.decoder import JSONDecodeError

from src.logger import LogManager
from src.processor.common import MessageProcessor, Payload, STGAppOutputMessage

log = LogManager().get_logger(__name__)


@dataclasses.dataclass(slots=True, frozen=True)
class OrderEvent:
    object_id: int
    object_type: str
    sent_dttm: datetime
    payload: dict


class STGMessageProcessor(MessageProcessor):
    __slots__ = (
        "consumer",
        "producer",
        "pg",
        "redis",
        "config",
        "environ",
    )

    def __init__(self) -> None:
        super().__init__()

        self.config = self.config["stg-collector-app"]

    def run(self) -> ...:
        log.info("Running stg layer message processor")

        self.consumer.subscribe(self.config["topic-in"])
        log.info(f"Subscribed to {self.config['topic-in']}")

        log.info(f"Will send output messages to -> {self.config['topic-out']} topic")

        log.info("Processing messages...")
        for message in self.consumer:
            log.info(
                f"Processing -> Offset: {message.offset} Partition: {message.partition} Timestamp: {message.timestamp}"
            )
            try:
                value: dict = json.loads(message.value)
            except JSONDecodeError:
                log.warning(f"Unable to decode {message.offset} offset. Skipping")
                continue

            if all(
                key in value
                for key in ("object_id", "object_type", "sent_dttm", "payload")
            ):  # Workaround for unexpected format messages
                if (
                    value["object_type"] == "order"
                ):  # In this step we need only 'order' messages
                    self._insert_order_event_row(message=self._get_order_event(value))

                    self.producer.send(
                        topic=self.config["topic-out"],
                        value=json.dumps(
                            dataclasses.asdict(self._get_output_message(value))
                        ).encode("utf-8"),
                    )
                else:
                    continue
            else:
                continue

    def _insert_order_event_row(self, message: OrderEvent) -> ...:
        cur = self.pg.cursor()

        try:
            log.debug(f"Inserting order event row: {message}")

            cur.execute(
                f""" 
                INSERT INTO
                        {self.config["target-tables"]["order-events"]} (object_id, object_type, sent_dttm, payload)
                    VALUES
                        ('{message.object_id}', '{message.object_type}', '{message.sent_dttm}', '{message.payload}')
                    ON CONFLICT (object_id) DO UPDATE
                        SET
                            object_type = excluded.object_type,
                            sent_dttm  = excluded.sent_dttm,
                            payload    = excluded.payload;

                """
            )
            self.pg.commit()

        except Exception as err:
            log.exception(err)
            self.pg.rollback()

            cur.close()
            self.pg.close()

            sys.exit(1)

    def _get_order_event(self, value: dict) -> OrderEvent:
        object_id = int(value["object_id"])
        object_type = str(value["object_type"])
        payload = value["payload"]

        order_event = OrderEvent(
            object_id=object_id,
            object_type=object_type,
            sent_dttm=value["sent_dttm"],
            payload=json.dumps(payload),
        )
        log.debug(f"Got order event: {order_event}")

        return order_event

    def _get_output_message(self, value: dict) -> STGAppOutputMessage:
        def get_category_name(restaurant_id: str, product_id: str) -> str | None:
            menu = self.redis.get(key=restaurant_id)["menu"]

            for product in menu:
                if product["_id"] == product_id:
                    return product["category"]

        object_id = int(value["object_id"])
        object_type = str(value["object_type"])
        payload = value["payload"]

        products = []

        for product in payload["order_items"]:
            d = dict(
                id=product["id"],
                name=product["name"],
                price=product["price"],
                quantity=product["quantity"],
                category=get_category_name(payload["restaurant"]["id"], product["id"]),
            )

            products.append(d)

        message = STGAppOutputMessage(
            object_id=object_id,
            object_type=object_type,
            payload=Payload(
                id=object_id,
                date=payload["date"],
                cost=payload["cost"],
                payment=payload["payment"],
                status=payload["final_status"],
                restaurant=dict(
                    id=payload["restaurant"]["id"],
                    name=self.redis.get(payload["restaurant"]["id"])["name"],
                ),
                user=dict(
                    id=payload["user"]["id"],
                    name=self.redis.get(payload["user"]["id"])["name"],
                ),
                products=products,
            ),
        )

        log.debug(f"Got output message: {message}")

        return message

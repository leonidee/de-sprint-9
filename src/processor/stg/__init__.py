from __future__ import annotations

import itertools
import json
import time
from datetime import datetime
from json.decoder import JSONDecodeError

import psycopg
from pydantic import BaseModel, ValidationError, field_validator

from src.logger import LogManager
from src.processor.common import MessageProcessor, Payload, STGAppOutputMessage

log = LogManager().get_logger(__name__)


class Value(BaseModel):
    object_id: int
    object_type: str
    send_dttm: datetime
    payload: dict

    @field_validator("object_type")
    @classmethod
    def validate_object_type(cls, value) -> ...:
        if value != "order":
            raise ValueError("Only order as object_type are allowed")


class STGMessageProcessor(MessageProcessor):
    __slots__ = (
        "consumer",
        "producer",
        "pg",
        "redis",
        "config",
        "environ",
        "batch_size",
        "delay",
        "timeout",
    )

    def __init__(self) -> None:
        super().__init__()

        self.config = self.config["stg-collector-app"]

    def run_processor(self) -> ...:
        log.info("Running stg layer message processor")

        self.consumer.subscribe(self.config["topic-in"])
        log.info(f"Subscribed to {self.config['topic-in']}")

        log.info(f"Will send output messages to {self.config['topic-out']} topic")

        counter = itertools.count(1)

        while True:
            start = datetime.now()
            current_batch = next(counter)
            log.info(f"Processing {current_batch} batch")

            pack = self.consumer.poll(
                timeout_ms=self.timeout, max_records=self.batch_size
            )

            cur = self.pg.cursor()

            for _, messages in pack.items():
                for message in messages:
                    log.debug(
                        f"Processing -> Offset: {message.offset} Partition: {message.partition} Timestamp: {message.timestamp}"
                    )
                    try:
                        value: dict = json.loads(message.value)
                    except JSONDecodeError:
                        log.warning(f"Unable to decode {message.offset} offset")
                        continue

                    try:
                        value = Value(**value)
                    except ValidationError as err:
                        if "missing" or "value_error" in err.errors()[0]["type"]:
                            log.warning(err)
                        else:
                            log.error(err)
                        continue

                    try:
                        self._insert_order_event_row(value, cur)
                    except Exception as err:
                        log.error(err)
                        self.pg.rollback()
                        cur.close()
                    else:
                        self.pg.commit()

            # self.producer.send(
            #     topic=self.config["topic-out"],
            #     value=json.dumps(
            #         dataclasses.asdict(self._get_output_message(value))
            #     ).encode("utf-8"),
            # )
            cur.close()

            log.info(f"{current_batch} batch processed in {datetime.now() - start}")

            log.info("Waiting for new batch")
            time.sleep(self.delay)

    def _insert_order_event_row(self, value: Value, cur: psycopg.Cursor) -> ...:
        log.debug(f"{value=}")

        cur.execute(
            f""" 
            INSERT INTO
                    {self.config["target-tables"]["order-events"]} (object_id, object_type, sent_dttm, payload)
                VALUES
                    ('{value.object_id}', '{value.object_type}', '{value.sent_dttm}', '{json.dumps(value.payload).encode("utf-8")}')
                ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_type = excluded.object_type,
                        sent_dttm  = excluded.sent_dttm,
                        payload    = excluded.payload;

            """
        )

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

        log.debug(f"{message=}")

        return message


class Payload(BaseModel):
    id: int
    date: datetime
    cost: float
    payment: float
    status: str
    restaurant: dict
    user: dict
    products: list


class STGAppMessage(BaseModel):
    object_id: int
    object_type: str
    payload: Payload

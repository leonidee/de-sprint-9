from __future__ import annotations

import dataclasses
import json
import sys
import uuid
from datetime import datetime
from typing import Any

import yaml

from src.kafka import KafkaClient
from src.logger import LogManager
from src.postgre import PGClient
from src.processor.common import MessageProcessor
from src.redis import RedisClient

log = LogManager().get_logger(__name__)


# dst_msg = {
#     "object_id": str(builder.h_order().h_order_pk),
#     "sent_dttm": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
#     "object_type": "order_report",
#     "payload": {
#         "id": str(builder.h_order().h_order_pk),
#         "order_dt": builder.h_order().order_dt.strftime("%Y-%m-%d %H:%M:%S"),
#         "status": builder.s_order_status().status,
#         "restaurant": {
#             "id": str(builder.h_restaurant().h_restaurant_pk),
#             "name": builder.s_restaurant_names().name,
#         },
#         "user": {
#             "id": str(builder.h_user().h_user_pk),
#             "username": builder.s_user_names().username,
#         },
#         "products": self._format_products(builder),
#     },
# }


@dataclasses.dataclass(slots=True, frozen=True)
class Payload:
    id: int
    date: datetime
    cost: float
    payment: float
    status: str
    restaurant: dict
    user: dict
    products: list


# @dataclasses.dataclass(slots=True, frozen=True)
# class HUser:
#     h_user_pk: uuid.UUID
#     user_id: str
#     load_dt: datetime
#     load_src: str


# class Builder:
#     def __init__(self, payload: Payload) -> None:
#         self.payload = payload

#     def get_uuid(self, obj: Any) -> uuid.UUID:
#         return uuid.uuid5(
#             namespace=uuid.UUID("7f288a2e-0ad0-4039-8e59-6c9838d87307"),
#             name=str(obj),
#         )

#     def get_h_users(self) -> HUser:
#         return HUser(
#             h_user_pk=self.get_uuid(obj=self.payload.user["id"]),
#             user_id=self.payload.user["id"],
#             load_dt=datetime.now(),
#             load_src="kafka",
#         )


class DDSMessageProcessor(MessageProcessor):
    __slots__ = (
        "consumer",
        "producer",
        "pg",
        "redis",
        "config",
    )

    def __init__(self) -> None:
        super().__init__()

        self.config = self.config["dds-collector-app"]

    def get_uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(
            namespace=uuid.UUID("7f288a2e-0ad0-4039-8e59-6c9838d87307"),
            name=str(obj),
        )

    def insert_hubs(self, payload: Payload) -> ...:
        conn = self.pg.connect()

        try:
            cur = conn.cursor()

            cur.execute(
                f""" 
            INSERT INTO
                dds.h_user (h_user_pk, user_id, load_dt, load_src)
            VALUES
                ('{self.get_uuid(payload.user['id'])}', '{payload.user['id']}', '{datetime.now()}', 'kafka')
            ON CONFLICT (h_user_pk) DO UPDATE
                SET
                    user_id = excluded.user_id,
                    load_dt  = excluded.load_dt,
                    load_src    = excluded.load_src;
            """
            )

            cur.execute(
                f""" 
            INSERT INTO
                dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
            VALUES
                ('{self.get_uuid(payload.restaurant['id'])}', '{payload.restaurant['id']}', '{datetime.now()}', 'kafka')
            ON CONFLICT (h_restaurant_pk) DO UPDATE
                SET
                    restaurant_id = excluded.restaurant_id,
                    load_dt  = excluded.load_dt,
                    load_src    = excluded.load_src;
            """
            )
            for product in payload.products:
                cur.execute(
                    f""" 
                INSERT INTO
                    dds.h_product (h_product_pk, product_id, load_dt, load_src)
                VALUES
                    ('{self.get_uuid(product['id'])}', '{product['id']}', '{datetime.now()}', 'kafka')
                ON CONFLICT (h_product_pk) DO UPDATE
                    SET
                        product_id = excluded.product_id,
                        load_dt  = excluded.load_dt,
                        load_src    = excluded.load_src;
                """
                )

            cur.execute(
                f""" 
            INSERT INTO
                dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
            VALUES
                ('{self.get_uuid(payload.id)}', '{payload.id}', '{payload.date}', '{datetime.now()}', 'kafka')
            ON CONFLICT (h_order_pk) DO UPDATE
                SET
                    order_id = excluded.order_id,
                    order_dt = excluded.order_dt,
                    load_dt  = excluded.load_dt,
                    load_src    = excluded.load_src;
            """
            )

            conn.commit()

        except Exception as err:
            log.exception(err)
            conn.rollback()

            sys.exit(1)

        finally:
            cur.close()
            conn.close()

    def run(self) -> ...:
        log.info("Running stg layer message processor")

        self.consumer.subscribe(self.config["dds-collector-app"]["topic-in"])

        log.info(f'Subscribed to {self.config["dds-collector-app"]["topic-in"]}')

        log.info("Processing messages...")

        for message in self.consumer:
            value: dict = json.loads(message.value)

            if all(
                key in value for key in ("object_id", "object_type", "payload")
            ):  # Workaround for unexpected format messages
                if (
                    value["object_type"] == "order"
                ):  # In this step we need only 'order' messages
                    payload = value["payload"]

                    payload = Payload(
                        id=payload["id"],
                        date=payload["date"],
                        cost=payload["cost"],
                        payment=payload["payment"],
                        status=payload["status"],
                        restaurant=dict(
                            id=payload["restaurant"]["id"],
                            name=payload["restaurant"]["name"],
                        ),
                        user=dict(
                            id=payload["user"]["id"],
                            name=payload["user"]["name"],
                        ),
                        products=payload["products"],
                    )

                    self.insert_hubs(payload)
                else:
                    continue
            else:
                continue

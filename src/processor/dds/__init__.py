from __future__ import annotations

import dataclasses
import json
import sys
import uuid
from datetime import datetime
from typing import Any, List

import yaml

from src.kafka import KafkaClient
from src.logger import LogManager
from src.postgre import PGClient
from src.processor.common import MessageProcessor, Payload
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

    def run(self) -> ...:
        log.info("Running dds layer message processor")

        self.consumer.subscribe(self.config["topic-in"])

        log.info(f'Subscribed to {self.config["topic-in"]}')

        log.info("Processing messages...")

        for message in self.consumer:
            log.info(
                f"Processing -> Offset: {message.offset} Partition: {message.partition} Timestamp: {message.timestamp}"
            )

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
                    builder = Builder(payload)

                    self.insert_hubs(builder)
                    self.insert_links(builder)

                else:
                    continue
            else:
                continue

    def insert_hubs(self, builder: Builder) -> ...:
        cur = self.pg.cursor()

        try:
            h_user = builder.get_h_user()

            cur.execute(
                f""" 
            INSERT INTO
                dds.h_user (h_user_pk, user_id, load_dt, load_src)
            VALUES
                ('{h_user.h_user_pk}', '{h_user.user_id}', '{h_user.load_dt}', '{h_user.load_src}')
            ON CONFLICT (h_user_pk) DO UPDATE
                SET
                    user_id   =  excluded.user_id,
                    load_dt   =  excluded.load_dt,
                    load_src  =  excluded.load_src;
            """
            )
            h_restaurant = builder.get_h_restaurant()

            cur.execute(
                f"""
            INSERT INTO
                dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
            VALUES
                ('{h_restaurant.h_restaurant_pk}', '{h_restaurant.restaurant_id}', '{h_restaurant.load_dt}', '{h_restaurant.load_src}')
            ON CONFLICT (h_restaurant_pk) DO UPDATE
                SET
                    restaurant_id = excluded.restaurant_id,
                    load_dt  = excluded.load_dt,
                    load_src    = excluded.load_src;
            """
            )

            for product in builder.get_h_product():
                cur.execute(
                    f"""
                INSERT INTO
                    dds.h_product (h_product_pk, product_id, load_dt, load_src)
                VALUES
                    ('{product.h_product_pk}', '{product.product_id}', '{product.load_dt}', '{product.load_src}')
                ON CONFLICT (h_product_pk) DO UPDATE
                    SET
                        product_id = excluded.product_id,
                        load_dt  = excluded.load_dt,
                        load_src    = excluded.load_src;
                """
                )

            for category in builder.get_h_category():
                cur.execute(
                    f"""
                INSERT INTO
                    dds.h_category (h_category_pk, category_name, load_dt, load_src)
                VALUES
                    ('{category.h_category_pk}', '{category.category_name}', '{category.load_dt}', '{category.load_src}')
                ON CONFLICT (h_category_pk) DO UPDATE
                    SET
                        category_name = excluded.category_name,
                        load_dt  = excluded.load_dt,
                        load_src    = excluded.load_src;
                """
                )

            h_order = builder.get_h_order()

            cur.execute(
                f"""
            INSERT INTO
                dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
            VALUES
                ('{h_order.h_order_pk}', '{h_order.order_id}', '{h_order.order_dt}', '{h_order.load_dt}', '{h_order.load_src}')
            ON CONFLICT (h_order_pk) DO UPDATE
                SET
                    order_id = excluded.order_id,
                    order_dt = excluded.order_dt,
                    load_dt  = excluded.load_dt,
                    load_src    = excluded.load_src;
            """
            )

            self.pg.commit()
            cur.close()

        except Exception as err:
            log.exception(err)

            self.pg.rollback()
            self.pg.close()

            sys.exit(1)

    def insert_links(self, builder: Builder) -> ...:
        cur = self.pg.cursor()

        try:
            h_user = builder.get_h_user()
            h_order = builder.get_h_order()

            l_order_user = builder.get_l_order_user(h_order=h_order, h_user=h_user)

            cur.execute(
                f"""
            INSERT INTO
                dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
            VALUES
                ('{l_order_user.hk_order_user_pk}', '{l_order_user.h_order_pk}', '{l_order_user.h_user_pk}', '{l_order_user.load_dt}', '{l_order_user.load_src}')
            ON CONFLICT (hk_order_user_pk) DO UPDATE
                SET
                    h_order_pk = excluded.h_order_pk,
                    h_user_pk  = excluded.h_user_pk,
                    load_dt    = excluded.load_dt,
                    load_src   = excluded.load_src;
            """
            )

            for product in builder.get_h_product():
                l_order_product = builder.get_l_order_product(
                    h_order=h_order, h_product=product
                )
                cur.execute(
                    f"""
                INSERT INTO
                    dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                VALUES
                    ('{l_order_product.hk_order_product_pk}', '{l_order_product.h_order_pk}', '{l_order_product.h_product_pk}', '{l_order_product.load_dt}', '{l_order_product.load_src}')
                ON CONFLICT (hk_order_product_pk) DO UPDATE
                    SET
                        h_order_pk   = excluded.h_order_pk,
                        h_product_pk = excluded.h_product_pk,
                        load_dt      = excluded.load_dt,
                        load_src     = excluded.load_src;
                """
                )

            self.pg.commit()
            cur.close()

        except Exception as err:
            log.exception(err)

            self.pg.rollback()
            self.pg.close()

            sys.exit(1)


class Builder:
    __slots__ = ("payload",)

    def __init__(self, payload: Payload) -> None:
        self.payload = payload

    def get_uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(
            namespace=uuid.UUID("7f288a2e-0ad0-4039-8e59-6c9838d87307"),
            name=str(obj),
        )

    def format_products(self, products: list[HubProduct]) -> list[dict[str, Any]]:
        return [dataclasses.asdict(product) for product in products]

    def get_h_order(self) -> HubOrder:
        return HubOrder(
            h_order_pk=self.get_uuid(obj=self.payload.id),
            order_id=self.payload.id,
            order_dt=datetime.strptime(self.payload.date, r"%Y-%m-%d %H:%M:%S"),
            load_dt=datetime.now(),
            load_src="test",
        )

    def get_h_user(self) -> HubUser:
        return HubUser(
            h_user_pk=self.get_uuid(obj=self.payload.user["id"]),
            user_id=self.payload.user["id"],
            load_dt=datetime.now(),
            load_src="test",
        )

    def get_h_restaurant(self) -> HubRestaurant:
        return HubRestaurant(
            h_restaurant_pk=self.get_uuid(obj=self.payload.restaurant["id"]),
            restaurant_id=self.payload.restaurant["id"],
            load_dt=datetime.now(),
            load_src="test",
        )

    def get_h_category(self) -> list[HubCategory]:
        return [
            HubCategory(
                h_category_pk=self.get_uuid(product["category"]),
                category_name=product["category"],
                load_dt=datetime.now(),
                load_src="test",
            )
            for product in self.payload.products
        ]

    def get_h_product(self) -> list[HubProduct]:
        return [
            HubProduct(
                h_product_pk=self.get_uuid(product["id"]),
                product_id=product["id"],
                load_dt=datetime.now(),
                load_src="test",
            )
            for product in self.payload.products
        ]

    def get_l_order_user(self, h_order: HubOrder, h_user: HubUser) -> LinkOrderUser:
        return LinkOrderUser(
            hk_order_user_pk=self.get_uuid(
                obj=str(h_user.h_user_pk) + str(h_order.h_order_pk)
            ),
            h_order_pk=h_order.h_order_pk,
            h_user_pk=h_user.h_user_pk,
            load_dt=datetime.now(),
            load_src="test",
        )

    def get_l_order_product(
        self, h_order: HubOrder, h_product: HubProduct
    ) -> LinkOrderProduct:
        return LinkOrderProduct(
            hk_order_product_pk=self.get_uuid(
                obj=str(h_order.h_order_pk) + str(h_product.h_product_pk)
            ),
            h_order_pk=h_order.h_order_pk,
            h_product_pk=h_product.h_product_pk,
            load_dt=datetime.now(),
            load_src="test",
        )

    def get_l_product_category(
        self, h_product: HubProduct, h_category: HubCategory
    ) -> LinkProductCategory:
        return LinkProductCategory(
            hk_product_category_pk=self.get_uuid(
                obj=h_product.h_product_pk + h_category.h_category_pk
            ),
            h_product_pk=h_product.h_product_pk,
            h_category_pk=h_category.h_category_pk,
            load_dt=datetime.now(),
            load_src="test",
        )

    def get_l_product_restaurant(
        self, h_product: HubProduct, h_restaurant: HubRestaurant
    ) -> LinkProductRestaurant:
        return LinkProductRestaurant(
            hk_product_restaurant_pk=self.get_uuid(
                obj=h_product.h_product_pk + h_restaurant.h_restaurant_pk
            ),
            h_product_pk=h_product.h_product_pk,
            h_restaurant_pk=h_restaurant.h_restaurant_pk,
            load_dt=datetime.now(),
            load_src="test",
        )


@dataclasses.dataclass(slots=True, frozen=True)
class HubUser:
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str


@dataclasses.dataclass(slots=True, frozen=True)
class HubRestaurant:
    h_restaurant_pk: uuid.UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str


@dataclasses.dataclass(slots=True, frozen=True)
class HubOrder:
    h_order_pk: uuid.UUID
    order_id: int
    order_dt: datetime
    load_dt: datetime
    load_src: str


@dataclasses.dataclass(slots=True, frozen=True)
class HubCategory:
    h_category_pk: uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str


@dataclasses.dataclass(slots=True, frozen=True)
class HubProduct:
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str


@dataclasses.dataclass(slots=True, frozen=True)
class LinkOrderProduct:
    hk_order_product_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str


@dataclasses.dataclass(slots=True, frozen=True)
class LinkOrderUser:
    hk_order_user_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_user_pk: uuid.UUID
    load_dt: datetime
    load_src: str


@dataclasses.dataclass(slots=True, frozen=True)
class LinkProductCategory:
    hk_product_category_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_category_pk: uuid.UUID
    load_dt: datetime
    load_src: str


@dataclasses.dataclass(slots=True, frozen=True)
class LinkProductRestaurant:
    hk_product_restaurant_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_restaurant_pk: uuid.UUID
    load_dt: datetime
    load_src: str

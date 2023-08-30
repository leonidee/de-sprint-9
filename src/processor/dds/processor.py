from __future__ import annotations

import dataclasses
import json
import sys
from datetime import datetime

from src.logger import LogManager
from src.processor.common import MessageProcessor, Payload
from src.processor.dds.builder import Builder

log = LogManager().get_logger(__name__)


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
                    builder = Builder(
                        payload=payload,
                        source_system=json.dumps(
                            dict(
                                source="kafka",
                                topic=self.config["topic-in"],
                            )
                        ),
                    )

                    self.insert_hubs(builder)
                    self.insert_links(builder)
                    self.insert_satelities(builder)

                    self.producer.send(
                        topic=self.config["topic-out"],
                        value=json.dumps(
                            dataclasses.asdict(self._get_output_message(builder))
                        ).encode("utf-8"),
                    )

                else:
                    continue
            else:
                continue

    def insert_hubs(self, builder: Builder) -> ...:
        log.info("Inserting hubs")

        cur = self.pg.cursor()

        try:
            h_user = builder.get_h_user()

            cur.execute(
                f""" 
            INSERT INTO
                {self.config['target-tables']['hubs']['user']} (h_user_pk, user_id, load_dt, load_src)
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
                {self.config['target-tables']['hubs']['restaurant']} (h_restaurant_pk, restaurant_id, load_dt, load_src)
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
                    {self.config['target-tables']['hubs']['product']} (h_product_pk, product_id, load_dt, load_src)
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
                    {self.config['target-tables']['hubs']['category']} (h_category_pk, category_name, load_dt, load_src)
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
                {self.config['target-tables']['hubs']['order']} (h_order_pk, order_id, order_dt, load_dt, load_src)
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

            cur.close()
            self.pg.close()

            sys.exit(1)

    def insert_links(self, builder: Builder) -> ...:
        log.info("Inserting links")

        cur = self.pg.cursor()

        try:
            l_order_user = builder.get_l_order_user()

            cur.execute(
                f"""
            INSERT INTO
                {self.config['target-tables']['links']['order-user']} (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
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

            for link in builder.get_l_order_product():
                cur.execute(
                    f"""
                INSERT INTO
                    {self.config['target-tables']['links']['order-product']} (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                VALUES
                    ('{link.hk_order_product_pk}', '{link.h_order_pk}', '{link.h_product_pk}', '{link.load_dt}', '{link.load_src}')
                ON CONFLICT (hk_order_product_pk) DO UPDATE
                    SET
                        h_order_pk   = excluded.h_order_pk,
                        h_product_pk = excluded.h_product_pk,
                        load_dt      = excluded.load_dt,
                        load_src     = excluded.load_src;
                """
                )

            for link in builder.get_l_product_category():
                cur.execute(
                    f"""
                INSERT INTO
                    {self.config['target-tables']['links']['product-category']} (hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)
                VALUES
                    ('{link.hk_product_category_pk}', '{link.h_product_pk}', '{link.h_category_pk}', '{link.load_dt}', '{link.load_src}')
                ON CONFLICT (hk_product_category_pk) DO UPDATE
                    SET
                        h_product_pk  = excluded.h_product_pk,
                        h_category_pk = excluded.h_category_pk,
                        load_dt       = excluded.load_dt,
                        load_src      = excluded.load_src;
                """
                )

            for link in builder.get_l_product_restaurant():
                cur.execute(
                    f"""
                INSERT INTO
                    {self.config['target-tables']['links']['product-restaurant']} (hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src)
                VALUES
                    ('{link.hk_product_restaurant_pk}', '{link.h_product_pk}', '{link.h_restaurant_pk}', '{link.load_dt}', '{link.load_src}')
                ON CONFLICT (hk_product_restaurant_pk) DO UPDATE
                    SET
                        h_product_pk    = excluded.h_product_pk,
                        h_restaurant_pk = excluded.h_restaurant_pk,
                        load_dt         = excluded.load_dt,
                        load_src        = excluded.load_src;
                """
                )

            self.pg.commit()
            cur.close()

        except Exception as err:
            log.exception(err)

            self.pg.rollback()

            cur.close()
            self.pg.close()

            sys.exit(1)

    def insert_satelities(self, builder: Builder) -> ...:
        log.info("Inserting satelities")

        cur = self.pg.cursor()

        s_order_cost = builder.get_s_order_cost()

        try:
            cur.execute(
                f"""

                INSERT INTO
                    {self.config['target-tables']['satelities']['order-cost']} (h_order_pk, cost, payment, load_dt, load_src)
                VALUES
                    ('{s_order_cost.h_order_pk}', '{s_order_cost.cost}', '{s_order_cost.payment}',  '{s_order_cost.load_dt}', '{s_order_cost.load_src}')
                ON CONFLICT (h_order_pk) DO UPDATE
                    SET
                        cost                   = excluded.cost,
                        payment                = excluded.payment,
                        load_dt                = excluded.load_dt,
                        load_src               = excluded.load_src;
                """
            )

            s_order_status = builder.get_s_order_status()

            cur.execute(
                f"""
                INSERT INTO
                    {self.config['target-tables']['satelities']['order-status']} (h_order_pk, status, load_dt, load_src)
                VALUES
                    ('{s_order_status.h_order_pk}', '{s_order_status.status}', '{s_order_status.load_dt}', '{s_order_status.load_src}')
                ON CONFLICT (h_order_pk) DO UPDATE
                    SET
                        status                   = excluded.status,
                        load_dt                  = excluded.load_dt,
                        load_src                 = excluded.load_src;
                """
            )

            for product in builder.get_s_product_names():
                cur.execute(
                    f"""
                INSERT INTO
                    {self.config['target-tables']['satelities']['product-names']} (h_product_pk, name, load_dt, load_src)
                VALUES
                    ('{product.h_product_pk}', '{product.name}', '{product.load_dt}', '{product.load_src}')
                ON CONFLICT (h_product_pk) DO UPDATE
                    SET
                        name                      = excluded.name,
                        load_dt                   = excluded.load_dt,
                        load_src                  = excluded.load_src;
                """
                )

            s_restaurant_names = builder.get_s_restaurant_names()

            cur.execute(
                f"""
                INSERT INTO
                    {self.config['target-tables']['satelities']['restaurant-names']} (h_restaurant_pk, name, load_dt, load_src)
                VALUES
                    ('{s_restaurant_names.h_restaurant_pk}', '{s_restaurant_names.name}', '{s_restaurant_names.load_dt}', '{s_restaurant_names.load_src}')
                ON CONFLICT (h_restaurant_pk) DO UPDATE
                    SET
                        name                         = excluded.name,
                        load_dt                      = excluded.load_dt,
                        load_src                     = excluded.load_src;
                """
            )

            s_user_names = builder.get_s_user_names()

            cur.execute(
                f"""
                INSERT INTO
                    {self.config['target-tables']['satelities']['user-names']} (h_user_pk, username, load_dt, load_src)
                VALUES
                    ('{s_user_names.h_user_pk}', '{s_user_names.username}', '{s_user_names.load_dt}', '{s_user_names.load_src}')
                ON CONFLICT (h_user_pk) DO UPDATE
                    SET
                        username               = excluded.username,
                        load_dt                = excluded.load_dt,
                        load_src               = excluded.load_src;
                """
            )

            self.pg.commit()
            cur.close()

        except Exception as err:
            log.exception(err)

            self.pg.rollback()

            cur.close()
            self.pg.close()

            sys.exit(1)

    def _get_output_message(self, builder: Builder) -> DDSAppOutputMessage:
        return DDSAppOutputMessage(
            object_id=str(builder.get_h_order().h_order_pk),
            object_type="order-report",
            send_dttm=datetime.now(),
            payload=OutPayload(
                order_id=str(builder.get_h_order().h_order_pk),
                order_dt=builder.get_h_order().order_dt,
                status=builder.get_s_order_status().status,
                restaurant=dict(
                    id=str(builder.get_h_restaurant().h_restaurant_pk),
                    name=builder.get_s_restaurant_names().name,
                ),
                user=dict(
                    id=str(builder.get_h_user().h_user_pk),
                    username=builder.get_s_user_names().username,
                ),
                products=builder.get_products_for_output_message(),
            ),
        )


@dataclasses.dataclass(slots=True, frozen=True)
class DDSAppOutputMessage:
    object_id: str
    object_type: str
    send_dttm: datetime
    payload: OutPayload


@dataclasses.dataclass(slots=True, frozen=True)
class OutPayload:
    order_id: str
    order_dt: datetime
    status: str
    restaurant: dict
    user: dict
    products: list

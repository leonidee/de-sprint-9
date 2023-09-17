from __future__ import annotations

import itertools
import json
import sys
import time
from collections import deque
from datetime import datetime
from json.decoder import JSONDecodeError

from pandas import DataFrame

from src.logger import LogManager
from src.processor.common import MessageProcessor

log = LogManager().get_logger(__name__)


class CDMMessageProcessor(MessageProcessor):
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

        self.config = self.config["cdm-collector-app"]

    def run(self, batch_size: int = 50, delay: int = 30) -> ...:
        log.info(
            f"Running cdm layer message processor with {batch_size} batch size and {delay} secs delay"
        )

        self.consumer.subscribe(self.config["topic-in"])

        counter = itertools.count(1)

        while True:
            start = datetime.now()
            current_batch = next(counter)
            log.info(f"Processing {current_batch} batch")

            pack = self.consumer.poll(timeout_ms=5000, max_records=batch_size)

            queue = deque()

            for _, messages in pack.items():
                for message in messages:
                    log.debug(
                        f"Processing -> Offset: {message.offset} Partition: {message.partition} Timestamp: {message.timestamp}"
                    )
                    try:
                        value: dict = json.loads(message.value)
                    except JSONDecodeError:
                        log.warning(
                            f"Unable to decode {message.offset} offset. Skipping"
                        )
                        continue

                    for product in value["payload"]["products"]:
                        queue.append(
                            dict(
                                order_id=value["payload"]["order_id"],
                                order_dt=value["payload"]["order_dt"],
                                status=value["payload"]["status"],
                                restaurant_id=value["payload"]["restaurant"]["id"],
                                restaurant_name=value["payload"]["restaurant"]["name"],
                                user_id=value["payload"]["user"]["id"],
                                user_name=value["payload"]["user"]["username"],
                                product_id=product["product_id"],
                                product_name=product["product_name"],
                                price=product["price"],
                                quantity=product["quantity"],
                                category_id=product["category_id"],
                                category_name=product["category_name"],
                            )
                        )

            self.insert_to_postgres(frame=DataFrame(queue))

            log.info(f"{current_batch} batch processed in {datetime.now() - start}")

            log.info("Waiting for new batch")
            time.sleep(delay)

    def insert_to_postgres(self, frame: DataFrame) -> ...:
        cur = self.pg.cursor()

        log.debug(cur)

        try:
            df = (
                frame.groupby(["user_id", "product_id", "product_name"], as_index=False)[  # type: ignore
                    "order_id"
                ]
                .nunique()
                .rename(columns={"order_id": "order_cnt"})
            )

            for row in df.itertuples(index=False):
                cur.execute(
                    f"""
                INSERT INTO
                    {self.config['target-tables']['user_product_counters']} (id, user_id, product_id, product_name, order_cnt)
                VALUES
                    (MD5(CONCAT('{row.user_id}', '{row.product_id}'))::uuid, '{row.user_id}', '{row.product_id}', '{row.product_name}', '{row.order_cnt}')
                ON CONFLICT (id) DO UPDATE SET
                                            user_id      = excluded.user_id,
                                            product_id   = excluded.product_id,
                                            product_name = excluded.product_name,
                                            order_cnt    = excluded.order_cnt + user_product_counters.order_cnt;
                """
                )
            log.debug(
                f"Inserted {self.config['target-tables']['user_product_counters']}"
            )

            df = (
                frame.groupby(["user_id", "category_id", "category_name"], as_index=False)[  # type: ignore
                    "order_id"
                ]
                .nunique()
                .rename(columns={"order_id": "order_cnt"})
            )

            for row in df.itertuples(index=False):
                cur.execute(
                    f"""
                INSERT INTO
                    {self.config['target-tables']['user_category_counters']} (id, user_id, category_id, category_name, order_cnt)
                VALUES
                    (MD5(CONCAT('{row.user_id}', '{row.category_id}'))::uuid,
                    '{row.user_id}', '{row.category_id}', '{row.category_name}', {row.order_cnt})
                ON CONFLICT (id) DO UPDATE SET
                                            user_id       = excluded.user_id,
                                            category_id   = excluded.category_id,
                                            category_name = excluded.category_name,
                                            order_cnt     = excluded.order_cnt + user_category_counters.order_cnt;
                """
                )
            log.debug(
                f"Inserted {self.config['target-tables']['user_category_counters']}"
            )

            self.pg.commit()
            cur.close()

        except Exception as err:
            log.exception(err)
            self.pg.rollback()

            cur.close()
            self.pg.close()

            sys.exit(1)

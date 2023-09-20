from __future__ import annotations

import csv
import json
from datetime import datetime
from os import getenv
from typing import Any, Generator

from s3fs import S3FileSystem

from src.clients import KafkaClient, Mongo
from src.logger import LogManager

log = LogManager().get_logger(__name__)

s3 = S3FileSystem(
    key=getenv("S3_ACCESS_KEY_ID"),
    secret=getenv("S3_SECRET_ACCESS_KEY"),
    endpoint_url=getenv("S3_ENDPOINT_URL"),
)
mongo = Mongo()
producer = KafkaClient().get_producer()


def get_path(path: str) -> list[str]:
    return [path for path in s3.ls(path) if ".csv" or ".json" in path]


def produce_orders_stream(topic: str, input_data_path: str) -> None:
    def get_read_generator(path: str) -> Generator[dict[str, str | Any], Any, None]:
        with s3.open(path=path, mode="r") as f:
            for row in csv.DictReader(f):
                yield dict(
                    object_id=row.get("object_id", None),
                    payload=row.get("payload", None),
                    object_type=row.get("object_type", None),
                    sent_dttm=str(datetime.now()),
                )

    log.info(f"Starting produce data for {topic} topic")

    path = get_path(input_data_path)

    log.debug(f"Got s3 path -> {path}")

    generator = get_read_generator(*path)

    log.info("Sending messages")
    while True:
        try:
            message: dict = next(generator)
            log.debug(f"Sending message -> {message}")

            producer.send(topic=topic, value=json.dumps(message).encode("utf-8"))
        except StopIteration:
            log.info("No data left. Stopping")
            break


def produce_dictionary(collection: str, input_data_path: str) -> None:
    db = mongo.get_database("prod")
    collection = db[collection]

    path = get_path(input_data_path)

    with s3.open(*path, mode="r") as f:
        for row in json.loads(f):
            collection.insert_one(row)
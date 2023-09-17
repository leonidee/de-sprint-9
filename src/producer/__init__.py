from __future__ import annotations

import csv
import json
from datetime import datetime
from os import getenv
from typing import Any, Generator

from s3fs import S3FileSystem

from src.kafka import KafkaClient
from src.logger import LogManager

log = LogManager().get_logger(__name__)


class DataProducer:
    __slots__ = ("producer", "s3")

    def __init__(self) -> None:
        self.s3 = S3FileSystem(
            key=getenv("S3_ACCESS_KEY_ID"),
            secret=getenv("S3_SECRET_ACCESS_KEY"),
            endpoint_url=getenv("S3_ENDPOINT_URL"),
        )
        self.producer = KafkaClient().get_producer()

    def _get_data_path(self, path: str) -> list[str]:
        return [path for path in self.s3.ls(path) if ".csv" in path]

    def _read_generator(self, path: str) -> Generator[dict[str, str | Any], Any, None]:
        log.debug("Initialize generator")

        with self.s3.open(path=path, mode="r") as f:
            for row in csv.DictReader(f):
                yield dict(
                    object_id=row.get("object_id", None),
                    payload=row.get("payload", None),
                    object_type=row.get("object_type", None),
                    sent_dttm=str(datetime.now()),
                )

    def produce(self, topic: str, input_data_path: str):
        log.info(f"Starting produce data for {topic} topic")

        path = self._get_data_path(input_data_path)

        log.debug(f"Got s3 path -> {path}")

        generator = self._read_generator(*path)

        log.info("Sending messages")
        while True:
            try:
                message: dict = next(generator)
                log.debug(f"Sending message -> {message}")

                self.producer.send(
                    topic=topic, value=json.dumps(message).encode("utf-8")
                )
            except StopIteration:
                log.info("No data left. Stopping")
                break

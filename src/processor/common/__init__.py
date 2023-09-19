from __future__ import annotations

from abc import ABC, abstractmethod
from os import getenv
from pathlib import Path

import yaml

from src.kafka import KafkaClient
from src.postgre import PGClient
from src.redis import RedisClient


class MessageProcessor(ABC):
    def __init__(self) -> None:
        kafka = KafkaClient()

        self.redis = RedisClient()

        self.consumer = kafka.get_consumer()
        self.producer = kafka.get_producer()

        with open(Path(__file__).parents[3] / "config.yaml") as f:
            config_file = yaml.safe_load(f)

        self.environ = getenv("ENVIRON")

        if not self.environ:
            raise ValueError("Set type of environment as ENVIRON variable")

        match self.environ.strip().lower():
            case "prod":
                self.config = config_file["apps"]["prod"]
                self.pg = PGClient(environ="prod").get_connection()

            case "test":
                self.config = config_file["apps"]["test"]
                self.pg = PGClient(environ="test").get_connection()

            case _:
                raise ValueError(
                    "Specify correct type of environment as ENVIRON variable. Should be 'prod' or 'test'"
                )

        self.batch_size: int = int(getenv("PROCESSOR_BATCH_SIZE"))
        self.delay: int = int(getenv("PROCESSOR_DELAY"))
        self.timeout: int = int(getenv("PROCESSOR_TIMEOUT"))

    @abstractmethod
    def run_processor(self) -> ...:
        ...

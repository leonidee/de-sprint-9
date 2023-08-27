from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from datetime import datetime

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

        with open("/app/config.yaml") as f:

            config_file = yaml.safe_load(f)

        self.environ = config_file['environ']

        match self.environ:
            case "prod":
                self.config = config_file['apps']['prod']
                self.pg = PGClient(environ="prod").get_connection()

            case "test":
                self.config = config_file['apps']['test']
                self.pg = PGClient(environ="test").get_connection()

            case _:
                raise ValueError(
                    "Specify correct type of environment in config.yaml file. Should be 'prod' or 'test'"
                )


    @abstractmethod
    def run(self) -> ...:
        ...


@dataclasses.dataclass(slots=True, frozen=True)
class STGAppOutputMessage:
    object_id: int
    object_type: str
    payload: Payload


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

from __future__ import annotations

import dataclasses
import json
import sys
import uuid
from datetime import datetime
from typing import Any, Generator, List

import yaml

from src.kafka import KafkaClient
from src.logger import LogManager
from src.postgre import PGClient
from src.processor.common import MessageProcessor, Payload
from src.redis import RedisClient

log = LogManager().get_logger(__name__)


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


@dataclasses.dataclass(slots=True, frozen=True)
class SatOrderCost:
    h_order_pk: uuid.UUID
    cost: float
    payment: float
    load_dt: datetime
    load_src: str


@dataclasses.dataclass(slots=True, frozen=True)
class SatOrderStatus:
    h_order_pk: uuid.UUID
    status: str
    load_dt: datetime
    load_src: str


@dataclasses.dataclass(slots=True, frozen=True)
class SatProductNames:
    h_product_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str


@dataclasses.dataclass(slots=True, frozen=True)
class SatRestaurantNames:
    h_restaurant_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str


@dataclasses.dataclass(slots=True, frozen=True)
class SatUserNames:
    h_user_pk: uuid.UUID
    username: str
    load_dt: datetime
    load_src: str

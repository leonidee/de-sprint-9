from __future__ import annotations

import json
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Generator, List

import yaml

from src.kafka import KafkaClient
from src.logger import LogManager
from src.postgre import PGClient
from src.processor.common import MessageProcessor, Payload
from src.redis import RedisClient

log = LogManager().get_logger(__name__)

DEFAULT_KWARGS = dict(slots=True, frozen=True)


@dataclass(**DEFAULT_KWARGS)
class HubUser:
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str


@dataclass(**DEFAULT_KWARGS)
class HubRestaurant:
    h_restaurant_pk: uuid.UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str


@dataclass(**DEFAULT_KWARGS)
class HubOrder:
    h_order_pk: uuid.UUID
    order_id: int
    order_dt: datetime
    load_dt: datetime
    load_src: str


@dataclass(**DEFAULT_KWARGS)
class HubCategory:
    h_category_pk: uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str


@dataclass(**DEFAULT_KWARGS)
class HubProduct:
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str


@dataclass(**DEFAULT_KWARGS)
class LinkOrderProduct:
    hk_order_product_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str


@dataclass(**DEFAULT_KWARGS)
class LinkOrderUser:
    hk_order_user_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_user_pk: uuid.UUID
    load_dt: datetime
    load_src: str


@dataclass(**DEFAULT_KWARGS)
class LinkProductCategory:
    hk_product_category_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_category_pk: uuid.UUID
    load_dt: datetime
    load_src: str


@dataclass(**DEFAULT_KWARGS)
class LinkProductRestaurant:
    hk_product_restaurant_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_restaurant_pk: uuid.UUID
    load_dt: datetime
    load_src: str


@dataclass(**DEFAULT_KWARGS)
class SatOrderCost:
    h_order_pk: uuid.UUID
    cost: float
    payment: float
    load_dt: datetime
    load_src: str


@dataclass(**DEFAULT_KWARGS)
class SatOrderStatus:
    h_order_pk: uuid.UUID
    status: str
    load_dt: datetime
    load_src: str


@dataclass(**DEFAULT_KWARGS)
class SatProductNames:
    h_product_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str


@dataclass(**DEFAULT_KWARGS)
class SatRestaurantNames:
    h_restaurant_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str


@dataclass(**DEFAULT_KWARGS)
class SatUserNames:
    h_user_pk: uuid.UUID
    username: str
    load_dt: datetime
    load_src: str

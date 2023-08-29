from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Generator, Iterable

from src.logger import LogManager
from src.processor.common import Payload
from src.processor.dds.datamodel import (
    HubCategory,
    HubOrder,
    HubProduct,
    HubRestaurant,
    HubUser,
    LinkOrderProduct,
    LinkOrderUser,
    LinkProductCategory,
    LinkProductRestaurant,
    SatOrderCost,
    SatOrderStatus,
    SatProductNames,
    SatRestaurantNames,
    SatUserNames,
)

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


class Builder:
    __slots__ = (
        "payload",
        "source_system",
    )

    def __init__(self, payload: Payload, source_system: str) -> None:
        self.payload = payload
        self.source_system = source_system

    def get_uuid(self, obj: Iterable[Any] | Any) -> uuid.UUID:
        if isinstance(obj, Iterable):
            obj: str = "".join([str(_) for _ in obj])
        else:
            obj: str = str(obj)

        return uuid.uuid5(
            namespace=uuid.UUID("7f288a2e-0ad0-4039-8e59-6c9838d87307"),
            name=obj,
        )

    # def format_products(self, products: list[HubProduct]) -> list[dict[str, Any]]:
    #     return [dataclasses.asdict(product) for product in products]

    def get_h_order(self) -> HubOrder:
        return HubOrder(
            h_order_pk=self.get_uuid(obj=self.payload.id),
            order_id=self.payload.id,
            order_dt=datetime.strptime(self.payload.date, r"%Y-%m-%d %H:%M:%S"),
            load_dt=datetime.now(),
            load_src=self.source_system,
        )

    def get_h_user(self) -> HubUser:
        return HubUser(
            h_user_pk=self.get_uuid(self.payload.user["id"]),
            user_id=self.payload.user["id"],
            load_dt=datetime.now(),
            load_src=self.source_system,
        )

    def get_h_restaurant(self) -> HubRestaurant:
        return HubRestaurant(
            h_restaurant_pk=self.get_uuid(obj=self.payload.restaurant["id"]),
            restaurant_id=self.payload.restaurant["id"],
            load_dt=datetime.now(),
            load_src=self.source_system,
        )

    def get_h_category(self) -> Generator[HubCategory, None, None]:
        return (
            HubCategory(
                h_category_pk=self.get_uuid(product["category"]),
                category_name=product["category"],
                load_dt=datetime.now(),
                load_src=self.source_system,
            )
            for product in self.payload.products
        )

    def get_h_product(self) -> Generator[HubProduct, None, None]:
        return (
            HubProduct(
                h_product_pk=self.get_uuid(product["id"]),
                product_id=product["id"],
                load_dt=datetime.now(),
                load_src=self.source_system,
            )
            for product in self.payload.products
        )

    def get_l_order_user(self) -> LinkOrderUser:
        return LinkOrderUser(
            hk_order_user_pk=self.get_uuid(
                obj=(
                    self.get_uuid(obj=self.payload.id),
                    self.get_uuid(obj=self.payload.user["id"]),
                )
            ),
            h_order_pk=self.get_uuid(obj=self.payload.id),
            h_user_pk=self.get_uuid(obj=self.payload.user["id"]),
            load_dt=datetime.now(),
            load_src=self.source_system,
        )

    def get_l_order_product(self) -> Generator[LinkOrderProduct, None, None]:
        return (
            LinkOrderProduct(
                hk_order_product_pk=self.get_uuid(
                    obj=(self.get_uuid(self.payload.id), self.get_uuid(product["id"]))
                ),
                h_order_pk=self.get_uuid(self.payload.id),
                h_product_pk=self.get_uuid(product["id"]),
                load_dt=datetime.now(),
                load_src=self.source_system,
            )
            for product in self.payload.products
        )

    def get_l_product_category(self) -> Generator[LinkProductCategory, None, None]:
        return (
            LinkProductCategory(
                hk_product_category_pk=self.get_uuid(
                    (self.get_uuid(product["id"]), self.get_uuid(product["category"]))
                ),
                h_product_pk=self.get_uuid(product["id"]),
                h_category_pk=self.get_uuid(product["category"]),
                load_dt=datetime.now(),
                load_src=self.source_system,
            )
            for product in self.payload.products
        )

    def get_l_product_restaurant(self) -> Generator[LinkProductRestaurant, None, None]:
        return (
            LinkProductRestaurant(
                hk_product_restaurant_pk=self.get_uuid(
                    obj=(
                        self.get_uuid(self.payload.restaurant["id"]),
                        self.get_uuid(product["id"]),
                    )
                ),
                h_product_pk=self.get_uuid(product["id"]),
                h_restaurant_pk=self.get_uuid(self.payload.restaurant["id"]),
                load_dt=datetime.now(),
                load_src=self.source_system,
            )
            for product in self.payload.products
        )

    def get_s_order_cost(self) -> SatOrderCost:
        return SatOrderCost(
            h_order_pk=self.get_uuid(self.payload.id),
            cost=self.payload.cost,
            payment=self.payload.payment,
            load_dt=datetime.now(),
            load_src=self.source_system,
        )

    def get_s_order_status(self) -> SatOrderStatus:
        return SatOrderStatus(
            h_order_pk=self.get_uuid(self.payload.id),
            status=self.payload.status,
            load_dt=datetime.now(),
            load_src=self.source_system,
        )

    def get_s_product_names(self) -> Generator[SatProductNames, None, None]:
        return (
            SatProductNames(
                h_product_pk=self.get_uuid(product["id"]),
                name=product["name"],
                load_dt=datetime.now(),
                load_src=self.source_system,
            )
            for product in self.payload.products
        )

    def get_s_restaurant_names(self) -> SatRestaurantNames:
        return SatRestaurantNames(
            h_restaurant_pk=self.get_uuid(self.payload.restaurant["id"]),
            name=self.payload.restaurant["name"],
            load_dt=datetime.now(),
            load_src=self.source_system,
        )

    def get_s_user_names(self) -> SatUserNames:
        return SatUserNames(
            h_user_pk=self.get_uuid(self.payload.user["id"]),
            username=self.payload.user["name"],
            load_dt=datetime.now(),
            load_src=self.source_system,
        )

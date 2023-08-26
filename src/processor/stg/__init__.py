from __future__ import annotations

import sys
from datetime import datetime
import json
import yaml
import dataclasses

from src.kafka import KafkaClient
from src.logger import LogManager
from src.postgre import PGClient
from src.redis import RedisClient

log = LogManager().get_logger(__name__)

@dataclasses.dataclass(slots=True, frozen=True)
class OrderEvent:
    object_id: int
    object_type: str
    sent_dttm: datetime
    payload: json

@dataclasses.dataclass(slots=True, frozen=True)
class OutputMessage:
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

class STGMessageProcessor:
    __slots__ = ("consumer", "producer", "pg", "redis", "config",)

    def __init__(self) -> None:
        kafka = KafkaClient()
        
        self.pg = PGClient()
        self.redis = RedisClient()

        self.consumer = kafka.get_consumer()
        self.producer = kafka.get_producer()

        with open("/app/config.yaml") as f:
            self.config = yaml.safe_load(f)["apps"]["stg-collector-app"]

    def insert_order_event_row(self, message: OrderEvent) -> ...:

        with open('/app/src/processor/stg/insert-order-event-row.sql', 'r') as f:
            query = f.read()

        try:
            conn = self.pg.connect()

        except Exception as err:
            log.exception(err)
            conn.close()

            sys.exit(1)

        try:
            cur = conn.cursor()
            log.debug(f'Inserting order event row: {message}')
        
            cur.execute(query.format(table=self.config['target-tables']['order-events'], object_id=message.object_id, object_type=message.object_type, sent_dttm=message.sent_dttm, payload=message.payload))
               
            conn.commit()

        except Exception as err:
            log.exception(err)
            conn.rollback()

            sys.exit(1)

        finally:
            cur.close()
            conn.close()
    
    
    def get_order_event(self, value: dict) -> OrderEvent:

        object_id = int(value['object_id'])
        object_type = str(value['object_type'])
        payload = value['payload']
        
        order_event = OrderEvent(
                    object_id=object_id,
                    object_type=object_type,
                    sent_dttm=value['sent_dttm'],
                    payload=json.dumps(payload),
                )
        log.debug(f'Got order event: {order_event}')

        return order_event


    def get_output_message(self, value: dict) -> OutputMessage:
        def get_category_name(restaurant_id: str, product_id: str) -> str:
            menu = self.redis.get(key=restaurant_id)['menu']

            for product in menu:
                if product['_id'] == product_id:
                    return product['category']
        

        object_id = int(value['object_id'])
        object_type = str(value['object_type'])
        payload = value['payload']

        products = [] 

        for product in payload['order_items']:

            d = dict(
                id=product['id'],
                name=product['name'],
                price=product['price'],
                quantity=product['quantity'],
                category=get_category_name(payload['restaurant']['id'], product['id'])
            ) 

            products.append(d)

        message = OutputMessage(
            object_id=object_id,
            object_type=object_type,
            payload=Payload(
                id=object_id,
                date=payload['date'],
                cost=payload['cost'],
                payment=payload['payment'],
                status=payload['final_status'],
                restaurant=dict(id=payload['restaurant']['id'], name=self.redis.get(payload['restaurant']['id'])['name']),
                user=dict(id=payload['user']['id'], name=self.redis.get(payload['user']['id'])['name']),
                products=products,
            )
        )

        log.debug(f"Got output message: {message}")

        return message

    def run(self) -> ...:
        log.info('Running stg layer message processor')

        self.consumer.subscribe(self.config['topic.in'])

        log.info(f"Subscribed to {self.config['topic.in']}")

        log.info(f"Will send output messages to -> {self.config['topic.in']} topic")

        log.info("Processing messages...")
        for message in self.consumer:

            log.info(f"Processing -> Offset: {message.offset} Partition: {message.partition} Timestamp: {datetime.fromtimestamp(message.timestamp)}")

            value: dict = json.loads(message.value)


            if all(key in value for key in ('object_id', 'object_type', 'sent_dttm', 'payload')): # Workaround for unexpected format messages

                if value['object_type'] == 'order': # In this step we need only 'order' messages

                    self.insert_order_event_row(
                        message=self.get_order_event(value)
                    )

                    self.producer.send(
                        topic=self.config['topic.out'],
                        value=json.dumps(dataclasses.asdict(self.get_output_message(value))).encode("utf-8"),
                    )
                else:
                    continue
            else:
                continue

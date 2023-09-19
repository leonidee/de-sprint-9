import json
import time
from os import getenv
from typing import Any, Dict

import redis
from src.logger import LogManager

log = LogManager().get_logger(__name__)

DELAY = 2  # Delay in sec between attemts to connect or do something


class RedisClient:
    __slots__ = "client"

    def __init__(self) -> None:
        log.debug("Connecting to Redis cluster")

        self.client = redis.Redis(
            host=getenv("YC_REDIS_HOST"),
            port=getenv("YC_REDIS_PORT"),
            password=getenv("YC_REDIS_PASSWORD"),
            ssl_ca_certs=getenv("CERTIFICATE_PATH"),
            ssl=True,
        )
        self.client.ping()

    def set(self, key: str, value: Dict[Any, Any] | Any) -> ...:
        for i in range(1, 10 + 1):
            try:
                self.client.set(key, json.dumps(value))
            except redis.exceptions.ConnectionError as err:
                if i == 10:
                    raise err
                else:
                    log.warning(f"{err}. Retrying...")
                    time.sleep(DELAY)

                    continue

        log.debug(f"Successfully set {key} key")

    def get(self, key: str) -> Dict[Any, Any] | None:
        log.debug(f"Getting {key} key")

        for i in range(1, 10 + 1):
            try:
                result = self.client.get(key)
            except redis.exceptions.ConnectionError as err:
                if i == 10:
                    raise err
                else:
                    log.warning(f"{err}. Retrying...")
                    time.sleep(DELAY)

                    continue

        if not result:
            raise KeyError(f"No such key -> '{key}'")
        else:
            try:
                return json.loads(result.decode())
            except json.JSONDecodeError as err:
                log.warning(f"Unable to decode {key} key value")
                return None

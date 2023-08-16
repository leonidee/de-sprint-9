import json
from os import getenv
from typing import Any, Dict

import redis
from src.logger import LogManager

log = LogManager().get_logger(__name__)


class RedisClient:
    __slots__ = "client"

    def __init__(self) -> None:
        log.debug("Connecting to Redis cluster")

        self.client = redis.StrictRedis(
            host=getenv("REDIS_HOST"),
            port=getenv("REDIS_PORT"),
            password=getenv("REDIS_PASSWORD"),
            ssl_ca_certs=getenv("CERTIFICATE_PATH"),
            ssl=True,
        )
        log.debug("Success!")

    def set(self, key: str, value: Dict[Any, Any] | Any):
        self.client.set(key, json.dumps(value))

        log.debug(f"Successfully set '{key}' key")

    def get(self, key: str) -> Dict[Any, Any]:
        log.debug(f"Getting '{key}' key")

        result = self.client.get(key)

        if not result:
            raise KeyError(f"No such key -> '{key}'")
        else:
            return json.loads(result.decode())

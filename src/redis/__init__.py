import json
import sys
from os import getenv
from typing import Any, Dict

import redis

sys.path.append("/app")


class RedisClient:
    def __init__(self) -> None:
        self.client = redis.StrictRedis(
            host=getenv("REDIS_HOST"),
            port=getenv("REDIS_PORT"),
            password=getenv("REDIS_PASSWORD"),
            ssl_ca_certs=getenv("CERTIFICATE_PATH"),
            ssl=True,
        )

    def set(self, key: str, value: Dict[Any, Any] | Any):
        self.client.set(key, json.dumps(value))

    def get(self, key: str) -> Dict[Any, Any]:
        result = self.client.get(key)

        if not result:
            raise KeyError(f"No such key -> '{key}'")
        else:
            return json.loads(result.decode())

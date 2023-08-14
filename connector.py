import json
import os
from typing import Dict

import dotenv
import kafka
import redis

dotenv.load_dotenv()


class RedisClient:
    def __init__(self, host: str, port: int, password: str, cert_path: str) -> None:
        self._client = redis.StrictRedis(
            host=host,
            port=port,
            password=password,
            ssl=True,
            ssl_ca_certs=cert_path,
        )

    def set(self, k: str, v: Dict):
        self._client.set(k, json.dumps(v))

    def get(self, k: str) -> Dict:
        result = self._client.get(k)

        if not result:
            raise KeyError(f"No such key -> '{k}'")
        else:
            return json.loads(result.decode())


class KafkaClient:
    def __init__(self) -> None:
        self.producer = kafka.KafkaProducer(
            bootstrap_servers=[
                "rc1a-j1ulkqntckv14qrd.mdb.yandexcloud.net:9091",
                "rc1b-9rnkj4ds3r9g1bpl.mdb.yandexcloud.net:9091",
            ],
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=os.getenv("KAFKA_USERNAME"),
            sasl_plain_password=os.getenv("KAFKA_PASSWORD"),
            ssl_cafile="./CA.pem",
        )

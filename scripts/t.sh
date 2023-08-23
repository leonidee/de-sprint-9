#!/usr/bin/env bash
source .env

curl -X POST https://order-gen-service.sprint9.tgcloudenv.ru/register_kafka \
-H 'Content-Type: application/json; charset=utf-8' \
--data-binary @- << EOF
{
    "student": "leonidgrishenkov@yandex.ru",
    "kafka_connect":{
        "host": "rc1a-j1ulkqntckv14qrd.mdb.yandexcloud.net",
        "port": 9091,
        "topic": "order-service_orders",
        "producer_name": "producer_consumer",
        "producer_password": "4kkDxgdnwuagovH7JuQy"
    }
}
EOF

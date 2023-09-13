#!/usr/bin/env bash
source .env

curl -X POST https://order-gen-service.sprint9.tgcloudenv.ru/register_kafka \
-H 'Content-Type: application/json; charset=utf-8' \
--data-binary @- << EOF
{
    "student": "leonidgrishenkov@yandex.ru",
    "kafka_connect":{
        "host": "${YC_KAFKA_BOOTSTRAP_SERVER}",
        "port": 9091,
        "topic": "order-app-orders",
        "producer_name": "${YC_KAFKA_USERNAME}",
        "producer_password": "{$YC_KAFKA_PASSWORD}"
    }
}
EOF
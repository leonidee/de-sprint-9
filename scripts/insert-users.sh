#!/usr/bin/env bash
curl -X POST https://redis-data-service.sprint9.tgcloudenv.ru/load_users \
-H 'Content-Type: application/json; charset=utf-8' \
--data-binary @- << EOF
{
    "redis":{
        "host": "c-c9q8vco1rvdhk37tcvjr.rw.mdb.yandexcloud.net",
        "port": 6380,
        "password": "GmT7KRu7HZhwbr7D2P87"
    }
}
EOF


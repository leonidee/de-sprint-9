import json
import os
import subprocess

import dotenv
import redis


def get_client():
    dotenv.load_dotenv()

    return redis.StrictRedis(
        host=os.getenv("REDIS_HOST"),
        port=os.getenv("REDIS_PORT"),
        password=os.getenv("REDIS_PASSWORD"),
        ssl=True,
        ssl_ca_certs="./CA.pem",
    )


def insert_users():
    cmd = "/bin/bash ./scripts/insert-users.sh"

    result = subprocess.run(cmd, shell=True, capture_output=True)

    if result:
        print(result.stdout.decode())


def insert_restaurants():
    cmd = "/bin/bash ./scripts/insert-restaurants.sh"

    result = subprocess.run(cmd, shell=True, capture_output=True)

    if result:
        print(result.stdout.decode())


def get_key(key: str):
    client = get_client()

    result = client.get(key)

    if not result:
        raise KeyError(f"No such key -> '{key}'")
    else:
        return json.loads(result.decode())


def main() -> ...:
    key = get_key(key="ef8c42c19b7518a9aebec106")

    from pprint import pprint

    for i in key["menu"]:
        if i["name"] == "Панир Тикка":
            print(i)

    """
    >>> key = get_key(key="ef8c42c19b7518a9aebec106")
    dict_keys(['_id', 'name', 'menu', 'update_ts_utc'])

    {'_id': 'ef8c42c19b7518a9aebec106',
    'menu': [{'_id': '22744fcdb947be9aa795e797',
            'category': 'Закуски',
            'name': 'Бомбей Тикки',
            'price': 290},
            {'_id': '1878439998067c1905f455b3',
            'category': 'Закуски',
            'name': 'Чиккен Тикка',
            'price': 590},
            {'_id': '95f74625be0428e4b2380310',
            'category': 'Закуски',
            'name': 'Проун Пакора',
            'price': 790}],
    'name': 'Вкус Индии',
    'update_ts_utc': '2023-08-11 16:50:44'}

    >>> key = get_key(key="626a81ce9a8cd1920641e264")
    dict_keys(['_id', 'name', 'login', 'update_ts_utc'])

    {'_id': '626a81ce9a8cd1920641e264',
    'login': 'gavaii_2222',
    'name': 'Гаврила Архипович Игнатьев',
    'update_ts_utc': '2023-08-11 16:44:59'}
    """


if __name__ == "__main__":
    main()

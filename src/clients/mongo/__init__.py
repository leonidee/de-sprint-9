from os import getenv

from pymongo import MongoClient


class Mongo(MongoClient):
    def __init__(self) -> None:
        USER = getenv("MONGODB_ROOT_USER")
        PASSWORD = getenv("MONGODB_ROOT_PASSWORD")
        HOST = getenv("MONGODB_HOST")
        PORT = getenv("MONGODB_PORT")

        super().__init__(
            f"mongodb://{USER}:{PASSWORD}@{HOST}:{PORT}/",
        )

from os import getenv

import psycopg

from src.logger import LogManager

log = LogManager().get_logger(name=__name__)


class PGClient:
    __slots__ = ("environ",)

    def __init__(self, environ: str) -> None:
        self.environ = environ

    def get_connection(self) -> psycopg.Connection:
        match self.environ:
            case "prod":
                properties = dict(
                    dbname=getenv("POSTGRESQL_PROD_DB"),
                    user=getenv("POSTGRESQL_USERNAME"),
                    password=getenv("POSTGRESQL_PASSWORD"),
                    host=getenv("POSTGRESQL_HOST"),
                    port=getenv("POSTGRESQL_PORT"),
                )
                log.debug(
                    f"Connecting to PostgreSQL {getenv('POSTGRESQL_PROD_DB')} database"
                )
            case "test":
                properties = dict(
                    dbname=getenv("POSTGRESQL_TEST_DB"),
                    user=getenv("POSTGRESQL_USERNAME"),
                    password=getenv("POSTGRESQL_PASSWORD"),
                    host=getenv("POSTGRESQL_HOST"),
                    port=getenv("POSTGRESQL_PORT"),
                )
                log.debug(
                    f"Connecting to PostgreSQL {getenv('POSTGRESQL_TEST_DB')} database"
                )

        return psycopg.connect(**properties)  # type: ignore

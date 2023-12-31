from os import getenv

import psycopg2

from src.logger import LogManager

log = LogManager().get_logger(name=__name__)


class PGClient:
    __slots__ = ("environ",)

    def __init__(self, environ: str) -> None:
        self.environ = environ

    def get_connection(self):
        log.debug("Connecting to PostgreSQL")

        match self.environ:
            case "prod":
                properties = dict(
                    dbname=getenv("YC_POSTGRE_DB"),
                    user=getenv("YC_POSTGRE_USERNAME"),
                    password=getenv("YC_POSTGRE_PASSWORD"),
                    host=getenv("YC_POSTGRE_HOST"),
                    port=getenv("YC_POSTGRE_PORT"),
                    sslmode="verify-full",
                    sslrootcert=getenv("CERTIFICATE_PATH"),
                )
            case "test":
                properties = dict(
                    dbname=getenv("YC_POSTGRE_DB"),
                    user=getenv("YC_POSTGRE_USERNAME"),
                    password=getenv("YC_POSTGRE_PASSWORD"),
                    host=getenv("YC_POSTGRE_HOST"),
                    port=getenv("YC_POSTGRE_PORT"),
                    sslmode="verify-full",
                    sslrootcert=getenv("CERTIFICATE_PATH"),
                )

        return psycopg2.connect(**properties)  # type: ignore

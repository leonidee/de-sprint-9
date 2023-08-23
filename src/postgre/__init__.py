import psycopg2
from os import getenv

from src.logger import LogManager

log = LogManager().get_logger(name=__name__)


class PGClient:
    def __init__(self) -> None:

        self.properties = dict(
            dbname=getenv('YC_POSTGRE_DB'),      
            user=getenv('YC_POSTGRE_USERNAME'),        
            password=getenv('YC_POSTGRE_PASSWORD'),        
            host=getenv('YC_POSTGRE_HOST'),        
            port=getenv('YC_POSTGRE_PORT'),
            sslmode="verify-full", 
            sslrootcert=getenv('CERTIFICATE_PATH'),           
        )

    def connect(self):
        return psycopg2.connect(**self.properties)
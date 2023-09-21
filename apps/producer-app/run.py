import sys

sys.path.append("/app")
from src.logger import LogManager
from src.producer import produce_orders_stream

log = LogManager().get_logger(__name__)


def main() -> ...:
    produce_orders_stream(
        input_data_path="s3a://data-ice-lake-05/master/data/source/order-app",
        topic="order-app-orders",
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)

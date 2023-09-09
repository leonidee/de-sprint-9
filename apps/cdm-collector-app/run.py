import sys

sys.path.append("/app")
from src.logger import LogManager
from src.processor.cdm import CDMMessageProcessor

log = LogManager().get_logger(__name__)


def main() -> ...:
    proc = CDMMessageProcessor()
    proc.run()

    proc.pg.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)

import sys

sys.path.append("/app")
from src.logger import LogManager
from src.processor import STGMessageProcessor

log = LogManager().get_logger(__name__)

def main() -> ...:
    proc = STGMessageProcessor()
    proc.run()

if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)

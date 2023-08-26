import sys
import yaml

sys.path.append("/app")
from src.logger import LogManager
from src.processor import STGMessageProcessor

log = LogManager().get_logger(__name__)

with open("/app/config.yaml") as f:
    config = yaml.safe_load(f)
    config = config["apps"]["stg-collector-app"]

def main() -> ...:
    proc = STGMessageProcessor()
    proc.run(input_topic=config['topic.in'], output_topic=config['topic.out'])

if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)

import sys
from logging import Formatter, Logger, StreamHandler, getLogger

import yaml

STREAM_HANDLERS: list = ["console"]


class LogManager(Logger):
    """Python Logging Manager for project."""

    __slots__ = ("level", "stream_handler")

    def __init__(self) -> None:
        with open("/app/project-config.yaml") as f:
            config = yaml.safe_load(f)

        self.level = config["logging"]["level"].upper()
        self.stream_handler = config["logging"]["stream-handler"]

    def get_logger(self, name: str) -> Logger:
        """Returns configured Logger instance.

        ## Parameters
        `name` : `str`
            Name of the logger

        ## Returns
        `logging.Logger`
        """
        logger = getLogger(name=name)

        logger.setLevel(level=self.level)

        if logger.hasHandlers():
            logger.handlers.clear()

        if self.stream_handler == STREAM_HANDLERS[0]:
            handler = StreamHandler(stream=sys.stdout)
        else:
            raise ValueError(
                f"Please specify correct handler for output logging stream. Should be one of: {STREAM_HANDLERS}"
            )

        if self.level == "DEBUG":
            message_format = r"[%(asctime)s] {%(name)s.%(funcName)s:%(lineno)d} %(levelname)s: %(message)s"
        elif self.level == "INFO":
            message_format = (
                r"[%(asctime)s] {%(name)s.%(lineno)d} %(levelname)s: %(message)s"
            )
        else:
            message_format = r"[%(asctime)s] {%(name)s} %(levelname)s: %(message)s"

        handler.setFormatter(
            fmt=Formatter(
                fmt=message_format,
                datefmt=r"%Y-%m-%d %H:%M:%S",
            )
        )

        logger.addHandler(handler)
        logger.propagate = False

        return logger

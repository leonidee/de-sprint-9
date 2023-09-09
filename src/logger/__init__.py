import sys
from logging import Formatter, Logger, StreamHandler, getLogger

import yaml


class LogManager(Logger):
    """Python Logging Manager for project."""

    __slots__ = ("level", "handler")

    def __init__(self) -> None:
        with open("/app/config.yaml") as f:
            config = yaml.safe_load(f)

        self.level = config["logging"]["level"].upper()
        self.handler = config["logging"]["handler"]

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

        match self.handler:
            case "console":
                handler = StreamHandler(stream=sys.stdout)
            case "localfile":
                handler = StreamHandler(stream=sys.stdout)
            case _:
                raise ValueError(f"Please specify correct handler for logging output")

        match self.level:
            case "DEBUG":
                message_format = r"[%(asctime)s] {%(name)s.%(funcName)s:%(lineno)d} %(levelname)s: %(message)s"
            case "INFO":
                message_format = (
                    r"[%(asctime)s] {%(name)s.%(lineno)d} %(levelname)s: %(message)s"
                )
            case _:
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

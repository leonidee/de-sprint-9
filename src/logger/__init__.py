import sys
from logging import Formatter, Logger, StreamHandler, getLogger
from os import getenv


class LogManager(Logger):
    """Python Logging Manager for project."""

    __slots__ = ("level", "handler")

    def __init__(self) -> None:
        self.level: str = getenv("LOGGING_LEVEL")
        self.handler: str = getenv("LOGGING_HANDLER")

        if not self.level:
            raise ValueError(
                "Specify level for logger object as LOGGING_LEVEL variable"
            )
        if not self.handler:
            raise ValueError(
                "Specify handler for logger object as LOGGING_HANDLER variable"
            )

    def get_logger(self, name: str) -> Logger:
        """Gets configured Logger instance.

        ## Parameters
        `name` : `str`
            Name of the logger

        ## Returns
        `logging.Logger`
        """
        logger = getLogger(name=name)

        logger.setLevel(level=self.level.strip().upper())

        if logger.hasHandlers():
            logger.handlers.clear()

        match self.handler.strip().lower():
            case "console":
                handler = StreamHandler(stream=sys.stdout)
            case "localfile":
                handler = StreamHandler(stream=sys.stdout)
            case _:
                raise ValueError(
                    "Please specify correct handler for logging object as LOGGING_HANDLER variable"
                )

        match self.level.strip().lower():
            case "debug":
                message_format = r"[%(asctime)s] {%(name)s.%(funcName)s:%(lineno)d} %(levelname)s: %(message)s"
            case "info":
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

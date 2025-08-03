import logging
import sys
from logging import Formatter, Logger, StreamHandler
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

_DEFAULT_LOG_FILE_PATH = Path(__file__).parent / "log" / "default.log"


class CustomLogger:  # pragma: no cover
    @classmethod
    def get_logger(
        cls,
        name: str,
        log_file_path: Path = _DEFAULT_LOG_FILE_PATH,
        log_level: int = logging.INFO,
        when: str = "midnight",
        backup_count: int = 7,
    ) -> Logger:
        logger = logging.getLogger(name)
        logger.setLevel(log_level)

        if not logger.hasHandlers():
            formatter = Formatter(
                fmt="%(asctime)s - %(levelname)s - %(module)s.%(funcName)s - %(message)s",  # noqa E501
                datefmt="%Y-%m-%d %H:%M:%S",
            )

            file_handler = TimedRotatingFileHandler(
                log_file_path, when=when, backupCount=backup_count
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(log_level)
            logger.addHandler(file_handler)

            stream_handler = StreamHandler(sys.stdout)
            stream_handler.setFormatter(formatter)
            stream_handler.setLevel(log_level)
            logger.addHandler(stream_handler)

        return logger

import logging
import sys
from logging import Logger
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

_LOG_FILE_PATH = Path(__file__).parent / "log" / "test.log"


class CustomLogger:  # pragma: no cover
    _instance: Logger | None = None

    @classmethod
    def get_logger(
        cls,
        log_file_path: Path = _LOG_FILE_PATH,
        log_level: int = logging.INFO,
        when: str = "midnight",
        backup_count: int = 7,
    ) -> Logger:
        if cls._instance is None:
            cls._instance = cls._initialize_logger(
                log_file_path, log_level, when, backup_count
            )
        return cls._instance

    @classmethod
    def _initialize_logger(
        cls,
        log_file_path: Path,
        log_level: int,
        when: str,
        backup_count: int,
    ) -> Logger:
        logger = logging.getLogger(__name__)
        logger.setLevel(log_level)

        file_handler = TimedRotatingFileHandler(
            log_file_path, when=when, backupCount=backup_count
        )
        file_handler.setLevel(log_level)
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        return logger

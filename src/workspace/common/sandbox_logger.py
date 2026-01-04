import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


class SandboxLogger:
    _LOG_DIR = Path("/workspace/log")
    _LOGGER_NAME = "sandbox"
    _MAX_BYTES = 10 * 1024 * 1024
    _BACKUP_COUNT = 5

    @classmethod
    def get_logger(cls) -> logging.Logger:
        logger = logging.getLogger(cls._LOGGER_NAME)

        if logger.handlers:
            return logger

        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(pathname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        log_path = cls._LOG_DIR / f"{cls._LOGGER_NAME}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=cls._MAX_BYTES,
            backupCount=cls._BACKUP_COUNT,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger

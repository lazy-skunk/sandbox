import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path("/workspace/log")
LOGGER_NAME = "sandbox"
MAX_BYTES = 10 * 1024 * 1024
BACKUP_COUNT = 5


def get_logger() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)

    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(pathname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    log_path = LOG_DIR / f"{LOGGER_NAME}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

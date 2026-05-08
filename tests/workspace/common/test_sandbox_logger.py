import logging
from pathlib import Path

from pytest import MonkeyPatch

from workspace.common import sandbox_logger


def test_get_logger(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    # Arrange
    logger = logging.getLogger(sandbox_logger.LOGGER_NAME)
    logger.handlers.clear()
    logger.propagate = False

    monkeypatch.setattr(sandbox_logger, "LOG_DIR", tmp_path)
    log_path = tmp_path / f"{sandbox_logger.LOGGER_NAME}.log"

    # Act
    logger = sandbox_logger.get_logger()
    logger.info("test")

    # Assert
    assert log_path.exists()

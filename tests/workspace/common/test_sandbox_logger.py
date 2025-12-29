import logging
from pathlib import Path
from unittest.mock import Mock

from pytest import MonkeyPatch

from workspace.common.sandbox_logger import SandboxLogger


def test_get_logger(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    # Arrange
    experiment_name = "test_dir"
    logger = logging.getLogger(experiment_name)
    logger.handlers.clear()
    logger.propagate = False

    monkeypatch.setattr(SandboxLogger, "_DEFAULT_SANDBOX_PATH", tmp_path)
    monkeypatch.setattr(
        "workspace.common.sandbox_logger.currentframe", lambda: None
    )
    monkeypatch.setattr(
        "workspace.common.sandbox_logger.getouterframes",
        lambda _frame: [
            None,
            None,
            Mock(filename=str(tmp_path / experiment_name / "test.py")),
        ],
    )

    log_path = tmp_path / experiment_name / "log" / "test_dir.log"

    # Act
    logger = SandboxLogger.get_logger()
    logger.info("test")

    # Assert
    assert log_path.exists()

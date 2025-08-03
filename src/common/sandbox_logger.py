import logging
from inspect import currentframe, getouterframes
from pathlib import Path

_DEFAULT_SANDBOX_PATH = Path("/app/src")


class SandboxLogger:
    @classmethod
    def get_logger(cls) -> logging.Logger:
        experiment_name = cls._get_experiment_name()
        logger = logging.getLogger(experiment_name)

        if logger.hasHandlers():
            return logger

        log_dir = _DEFAULT_SANDBOX_PATH / experiment_name
        log_path = log_dir / f"{experiment_name}.log"

        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(module)s.%(funcName)s - %(message)s",  # noqa E501
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        log_path.parent.mkdir(parents=True, exist_ok=True)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger

    @classmethod
    def _get_experiment_name(cls) -> str:
        outer_frames = getouterframes(currentframe())
        CALLER_FRAME_INDEX = 1
        caller_frame_info = outer_frames[CALLER_FRAME_INDEX]
        caller_file_path = Path(caller_frame_info.filename).resolve()
        path_parts = caller_file_path.parts

        try:
            sandbox_index = path_parts.index(_DEFAULT_SANDBOX_PATH.name)
            experiment_name = path_parts[sandbox_index + 1]
        except (ValueError, IndexError) as e:
            raise RuntimeError("Invalid directory structure") from e

        return experiment_name

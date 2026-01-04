import logging
from inspect import currentframe, getouterframes
from pathlib import Path


class SandboxLogger:
    _DEFAULT_SANDBOX_PATH = Path("/workspace/src/workspace")

    @classmethod
    def get_logger(cls) -> logging.Logger:
        experiment_name = SandboxLogger._get_experiment_name()
        logger = logging.getLogger(experiment_name)

        if logger.handlers:
            return logger

        log_dir = cls._DEFAULT_SANDBOX_PATH / experiment_name / "log"
        log_path = log_dir / f"{experiment_name}.log"

        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s",
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
        CALLER_FRAME_INDEX = 2
        caller_frame_info = outer_frames[CALLER_FRAME_INDEX]
        caller_file_path = Path(caller_frame_info.filename).resolve()
        sandbox_path = cls._DEFAULT_SANDBOX_PATH.resolve()

        try:
            relative_path = caller_file_path.relative_to(sandbox_path)
            experiment_name = relative_path.parts[0]
        except (ValueError, IndexError) as e:
            raise RuntimeError("Invalid directory structure") from e

        return experiment_name

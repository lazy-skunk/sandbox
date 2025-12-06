import json
import logging
import tracemalloc
from collections.abc import Callable
from dataclasses import dataclass
from functools import wraps
from time import perf_counter, process_time
from typing import Any, ParamSpec, Protocol, TypeVar, cast

import psutil

_BYTES_PER_MIB = 1024**2
logging.basicConfig(level=logging.DEBUG)
_LOGGER = logging.getLogger(__name__)

Parameters = ParamSpec("Parameters")
ReturnType = TypeVar("ReturnType")
PsutilResult = TypeVar("PsutilResult")


class ProcessMemoryInfo(Protocol):
    rss: int


class ProcessMemoryFullInfo(ProcessMemoryInfo, Protocol):
    uss: int


class ProcessIOCounters(Protocol):
    read_bytes: int
    write_bytes: int
    read_count: int
    write_count: int


@dataclass(frozen=True)
class ProcessSnapshot:
    resident_memory_bytes: int
    unique_memory_bytes: float | None
    process_io_counters: ProcessIOCounters | None


@dataclass(frozen=True)
class ProcessMetrics:
    memory_info: dict[str, float]
    io_counters: dict[str, float | int] | None


@dataclass(frozen=True)
class TracemallocState:
    is_tracing: bool
    initial_allocated_bytes: int
    initial_peak_bytes: int


def _call_psutil_process_method_safely[PsutilResult](
    process_method: Callable[[], PsutilResult],
) -> PsutilResult | None:
    try:
        return process_method()
    except (psutil.Error, AttributeError) as exception:
        method_name = getattr(process_method, "__name__", repr(process_method))
        _LOGGER.debug(f"Failed to call {method_name}: {exception}")
        return None


def _capture_process_snapshot() -> ProcessSnapshot:
    psutil_process = psutil.Process()

    process_memory_info = _call_psutil_process_method_safely(
        cast(Callable[[], ProcessMemoryInfo], psutil_process.memory_info)
    )
    resident_memory_bytes = (
        process_memory_info.rss if process_memory_info else 0
    )

    process_memory_full_info = _call_psutil_process_method_safely(
        cast(
            Callable[[], ProcessMemoryFullInfo],
            psutil_process.memory_full_info,
        )
    )
    unique_memory_bytes = (
        float(process_memory_full_info.uss)
        if process_memory_full_info is not None
        else None
    )

    process_io_counters = _call_psutil_process_method_safely(
        cast(
            Callable[[], ProcessIOCounters],
            psutil_process.io_counters,
        )
    )

    return ProcessSnapshot(
        resident_memory_bytes=resident_memory_bytes,
        unique_memory_bytes=unique_memory_bytes,
        process_io_counters=process_io_counters,
    )


def _build_psutil_memory_metrics(
    before_snapshot: ProcessSnapshot, after_snapshot: ProcessSnapshot
) -> dict[str, float]:
    psutil_memory_metrics = {
        "rss_delta_mib": round(
            (
                after_snapshot.resident_memory_bytes
                - before_snapshot.resident_memory_bytes
            )
            / _BYTES_PER_MIB,
            3,
        )
    }
    if (
        before_snapshot.unique_memory_bytes is not None
        and after_snapshot.unique_memory_bytes is not None
    ):
        psutil_memory_metrics["uss_delta_mib"] = round(
            (
                after_snapshot.unique_memory_bytes
                - before_snapshot.unique_memory_bytes
            )
            / _BYTES_PER_MIB,
            3,
        )

    return psutil_memory_metrics


def _build_io_metrics(
    before_process_snapshot: ProcessSnapshot,
    after_process_snapshot: ProcessSnapshot,
) -> dict[str, float | int] | None:
    if (
        before_process_snapshot.process_io_counters is None
        or after_process_snapshot.process_io_counters is None
    ):
        return None

    before_io_counters = before_process_snapshot.process_io_counters
    after_io_counters = after_process_snapshot.process_io_counters

    return {
        "read_mib": round(
            (after_io_counters.read_bytes - before_io_counters.read_bytes)
            / _BYTES_PER_MIB,
            3,
        ),
        "write_mib": round(
            (after_io_counters.write_bytes - before_io_counters.write_bytes)
            / _BYTES_PER_MIB,
            3,
        ),
        "read_operations": (
            after_io_counters.read_count - before_io_counters.read_count
        ),
        "write_operations": (
            after_io_counters.write_count - before_io_counters.write_count
        ),
    }


def _build_process_metrics(
    before_snapshot: ProcessSnapshot, after_snapshot: ProcessSnapshot
) -> ProcessMetrics:
    psutil_memory_metrics = _build_psutil_memory_metrics(
        before_snapshot, after_snapshot
    )

    io_metrics = _build_io_metrics(before_snapshot, after_snapshot)

    return ProcessMetrics(
        memory_info=psutil_memory_metrics, io_counters=io_metrics
    )


def _initialize_tracemalloc_capture() -> TracemallocState:
    is_tracing = tracemalloc.is_tracing()
    if not is_tracing:
        tracemalloc.start()

    if tracemalloc.is_tracing():
        current_bytes, peak_bytes = tracemalloc.get_traced_memory()
    else:
        current_bytes, peak_bytes = 0, 0

    return TracemallocState(
        is_tracing=is_tracing,
        initial_allocated_bytes=current_bytes,
        initial_peak_bytes=peak_bytes,
    )


def _finalize_tracemalloc_capture(
    tracemalloc_state: TracemallocState,
) -> dict[str, float] | None:
    if not tracemalloc.is_tracing():
        return None

    current_bytes, peak_bytes = tracemalloc.get_traced_memory()

    metrics = {
        "allocated_delta_mib": round(
            (current_bytes - tracemalloc_state.initial_allocated_bytes)
            / _BYTES_PER_MIB,
            3,
        ),
        "peak_delta_mib": round(
            (peak_bytes - tracemalloc_state.initial_peak_bytes)
            / _BYTES_PER_MIB,
            3,
        ),
    }

    if not tracemalloc_state.is_tracing:
        tracemalloc.stop()

    return metrics


def _create_log_payload(
    wrapped_function_name: str,
    status: str,
    before_process_snapshot: ProcessSnapshot,
    after_process_snapshot: ProcessSnapshot,
    tracemalloc_state: TracemallocState,
    start_time: float,
    start_cpu_time: float,
) -> dict[str, Any]:
    elapsed_time = perf_counter() - start_time
    elapsed_cpu_time = process_time() - start_cpu_time
    io_wait_seconds = max(elapsed_time - elapsed_cpu_time, 0.0)

    cpu_utilization_percent = (
        (elapsed_cpu_time / elapsed_time * 100.0) if elapsed_time else 0.0
    )

    process_metrics = _build_process_metrics(
        before_process_snapshot, after_process_snapshot
    )

    processing_time_payload = {
        "elapsed_wall_seconds": round(elapsed_time, 3),
        "cpu_seconds": round(elapsed_cpu_time, 3),
        "io_wait_seconds": round(io_wait_seconds, 3),
    }

    io_payload: dict[str, float | int | str] = {}
    if process_metrics.io_counters is not None:
        io_payload.update(process_metrics.io_counters)

    if not io_payload:
        io_payload["metrics"] = "unavailable"

    memory_payload: dict[str, dict[str, float]] = {
        "psutil": process_metrics.memory_info
    }

    tracemalloc_metrics = _finalize_tracemalloc_capture(tracemalloc_state)
    if tracemalloc_metrics is not None:
        memory_payload["python"] = tracemalloc_metrics

    return {
        "function": wrapped_function_name,
        "status": status,
        "processing_time": processing_time_payload,
        "cpu": {"utilization_percent": round(cpu_utilization_percent, 1)},
        "memory": memory_payload,
        "io": io_payload,
    }


def measure_performance[**Parameters, ReturnType](
    wrapped_function: Callable[Parameters, ReturnType],
) -> Callable[Parameters, ReturnType]:
    @wraps(wrapped_function)
    def wrapper(
        *wrapped_function_args: Parameters.args,
        **wrapped_function_kwargs: Parameters.kwargs,
    ) -> ReturnType:
        start_time = perf_counter()
        start_cpu_time = process_time()

        before_process_snapshot = _capture_process_snapshot()
        tracemalloc_state = _initialize_tracemalloc_capture()

        try:
            return_value = wrapped_function(
                *wrapped_function_args, **wrapped_function_kwargs
            )
        except BaseException:
            after_process_snapshot = _capture_process_snapshot()
            log_payload = _create_log_payload(
                wrapped_function.__name__,
                "error",
                before_process_snapshot,
                after_process_snapshot,
                tracemalloc_state,
                start_time,
                start_cpu_time,
            )
            structured_log_payload = json.dumps(log_payload, indent=2)
            _LOGGER.debug(structured_log_payload)
            raise
        else:
            after_process_snapshot = _capture_process_snapshot()
            log_payload = _create_log_payload(
                wrapped_function.__name__,
                "success",
                before_process_snapshot,
                after_process_snapshot,
                tracemalloc_state,
                start_time,
                start_cpu_time,
            )
            structured_log_payload = json.dumps(log_payload, indent=2)
            _LOGGER.debug(structured_log_payload)
            return return_value

    return wrapper


if __name__ == "__main__":
    from pathlib import Path
    from tempfile import TemporaryDirectory

    import numpy as np
    import pandas as pd

    @measure_performance
    def test_function(rows: int = 500000) -> pd.DataFrame:
        temp_df = pd.DataFrame(
            {
                "id": np.arange(rows, dtype=np.int64),
                "value": np.linspace(0, 1, rows, dtype=np.float64),
            }
        )
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "temp.csv"
            temp_df.to_csv(csv_path, index=False)
            return pd.read_csv(
                csv_path, dtype={"id": "int64", "value": "float64"}
            )

    temp_df = test_function()
    print(temp_df.head())

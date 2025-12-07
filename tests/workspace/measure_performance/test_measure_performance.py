from collections.abc import Iterator
from unittest.mock import Mock

import psutil
import pytest
from pytest import LogCaptureFixture, MonkeyPatch

import workspace.measure_performance.measure_performance as measure_performance_module  # noqa: E501
from workspace.measure_performance.measure_performance import (
    ProcessIOCounters,
    ProcessSnapshot,
    _capture_process_snapshot,
    measure_performance,
)


def _create_process_snapshot(
    resident_set_size_bytes: int,
    unique_set_size_bytes: float | None,
    io_counters_values: tuple[int, int, int, int] | None = None,
) -> ProcessSnapshot:
    io_counters: ProcessIOCounters | None = None
    if io_counters_values is not None:
        read_bytes, write_bytes, read_count, write_count = io_counters_values
        io_counters = Mock(spec=ProcessIOCounters)
        io_counters.read_bytes = read_bytes
        io_counters.write_bytes = write_bytes
        io_counters.read_count = read_count
        io_counters.write_count = write_count

    return ProcessSnapshot(
        resident_set_size_bytes=resident_set_size_bytes,
        unique_set_size_bytes=unique_set_size_bytes,
        process_io_counters=io_counters,
    )


def _create_process_snapshots() -> Iterator[ProcessSnapshot]:
    before_process_snapshot = _create_process_snapshot(
        1024, 512.0, (2, 4, 1, 2)
    )
    after_process_snapshot = _create_process_snapshot(
        2048, 1024.0, (8, 16, 3, 4)
    )
    snapshots = [before_process_snapshot, after_process_snapshot]
    return iter(snapshots)


@pytest.mark.parametrize(
    ("enable_tracemalloc", "should_log_python_section"),
    [(False, False), (True, True)],
)
def test_measure_performance_success_logs(
    monkeypatch: MonkeyPatch,
    caplog: LogCaptureFixture,
    enable_tracemalloc: bool,
    should_log_python_section: bool,
) -> None:
    # Arrange
    snapshots = _create_process_snapshots()
    monkeypatch.setattr(
        measure_performance_module,
        "_capture_process_snapshot",
        lambda: next(snapshots),
    )

    caplog.set_level("DEBUG")

    @measure_performance(enable_tracemalloc=enable_tracemalloc)
    def add(a: int, b: int) -> int:
        return a + b

    # Act
    result = add(1, 2)

    # Assert
    assert result == 3
    assert any(
        '"status": "success"' in record.message for record in caplog.records
    )
    is_python_section_logged = any(
        '"python": ' in record.message for record in caplog.records
    )
    assert is_python_section_logged == should_log_python_section


@pytest.mark.parametrize(
    ("enable_tracemalloc", "should_log_python_section"),
    [(False, False), (True, True)],
)
def test_measure_performance_error_logs(
    monkeypatch: MonkeyPatch,
    caplog: LogCaptureFixture,
    enable_tracemalloc: bool,
    should_log_python_section: bool,
) -> None:
    # Arrange
    snapshots = _create_process_snapshots()
    monkeypatch.setattr(
        measure_performance_module,
        "_capture_process_snapshot",
        lambda: next(snapshots),
    )

    caplog.set_level("DEBUG")

    @measure_performance(enable_tracemalloc=enable_tracemalloc)
    def raise_runtime_error() -> None:
        raise RuntimeError("RuntimeError occured.")

    # Act / Assert
    with pytest.raises(RuntimeError, match="RuntimeError occured."):
        raise_runtime_error()

    assert any(
        '"status": "error"' in record.message for record in caplog.records
    )
    is_python_section_logged = any(
        '"python": ' in record.message for record in caplog.records
    )
    assert is_python_section_logged == should_log_python_section


def test_capture_process_snapshot_reads_psutil(
    monkeypatch: MonkeyPatch,
) -> None:
    # Arrange
    process_mock = Mock(spec=psutil.Process)

    memory_info_mock = Mock()
    memory_info_mock.rss = 2048
    process_mock.memory_info.return_value = memory_info_mock

    memory_full_info_mock = Mock()
    memory_full_info_mock.rss = 2048
    memory_full_info_mock.uss = 1024
    process_mock.memory_full_info.return_value = memory_full_info_mock

    io_counters_mock = Mock()
    io_counters_mock.read_bytes = 10
    io_counters_mock.write_bytes = 20
    io_counters_mock.read_count = 1
    io_counters_mock.write_count = 2
    process_mock.io_counters.return_value = io_counters_mock

    monkeypatch.setattr(
        "workspace.measure_performance.measure_performance.psutil.Process",
        lambda: process_mock,
    )

    # Act
    snapshot = _capture_process_snapshot()

    # Assert
    assert snapshot.resident_set_size_bytes == 2048
    assert snapshot.unique_set_size_bytes == 1024
    assert snapshot.process_io_counters is not None
    assert snapshot.process_io_counters.read_bytes == 10
    assert snapshot.process_io_counters.write_bytes == 20
    assert snapshot.process_io_counters.read_count == 1
    assert snapshot.process_io_counters.write_count == 2

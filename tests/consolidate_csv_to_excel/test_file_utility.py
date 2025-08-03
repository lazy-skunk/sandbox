import os
from pathlib import Path

import pytest

from src.consolidate_csv_to_excel.file_utility import (
    _EXCEL_FOLDER_PATH,
    FileUtility,
)


def test_create_target_based_excel_path() -> None:
    target_fullname = "target_0"

    result = FileUtility.create_target_based_excel_path(target_fullname)
    expected = os.path.join(_EXCEL_FOLDER_PATH, f"{target_fullname}.xlsx")
    assert result == expected


def test_create_date_based_excel_path() -> None:
    date = "19880209"
    suffix = "target_0"

    result = FileUtility.create_date_based_excel_path(date, suffix)
    expected = os.path.join(_EXCEL_FOLDER_PATH, date, f"{date}_{suffix}.xlsx")
    assert result == expected


def test_create_directory(tmp_path: str) -> None:
    excel_file_path = os.path.join(tmp_path, "19880209", "19880209_file.xlsx")
    excel_directory = os.path.dirname(excel_file_path)

    assert not os.path.exists(excel_directory)
    FileUtility.create_directory(excel_file_path)
    assert os.path.exists(excel_directory)


@pytest.mark.parametrize(
    "target_folder_path, date, expected",
    [
        (
            "data/target_0/",
            "19880209",
            "/app/tests/consolidate_csv_to_excel/data/target_0/test_19880209.csv",
        ),
        ("data/target_4/", "19880209", None),
    ],
)
def test_get_csv_path(
    target_folder_path: str,
    date: str,
    expected: str | None,
) -> None:
    test_folders_base_path = Path(__file__).parent / target_folder_path

    result = FileUtility.get_csv_path(test_folders_base_path, date)
    assert result == expected

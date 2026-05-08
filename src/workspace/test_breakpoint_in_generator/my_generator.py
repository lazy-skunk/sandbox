from collections.abc import Generator
from pathlib import Path

import pandas as pd

_BASE_DIR = Path(__file__).parent
_DATA_DIR = _BASE_DIR / "data"


def load_phonetic_dfs(directory: Path) -> Generator[pd.DataFrame]:
    csv_file_paths = directory.glob("*.csv")

    for csv_file_path in csv_file_paths:
        phonetic_data = pd.read_csv(csv_file_path)

        if phonetic_data.empty:
            print("0件")
            continue

        yield phonetic_data


def _print_with_for(directory: Path) -> None:
    for phonetic_data in load_phonetic_dfs(directory):
        print(phonetic_data.head())


def _print_with_next(directory: Path) -> None:
    csv_file_paths = list(directory.glob("*.csv"))

    phonetic_dfs = load_phonetic_dfs(directory)
    try:
        for _ in range(len(csv_file_paths)):
            print(next(phonetic_dfs))
    except StopIteration:
        print("StopIteration 時にやりたいことをする。")


if __name__ == "__main__":
    _print_with_for(_DATA_DIR)
    print("==========")
    _print_with_next(_DATA_DIR)

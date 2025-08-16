from collections.abc import Generator
from pathlib import Path

import pandas as pd

_BASE_DIR = Path(__file__).parent
_DATA_DIR = _BASE_DIR / "data"


def load_phonetic_dfs(directory: Path) -> Generator[pd.DataFrame]:
    csv_file_paths = directory.glob("*.csv")

    for csv_file_path in csv_file_paths:
        df = pd.read_csv(csv_file_path)

        if not df.empty:
            yield df
        else:
            print("0件")


def _print_with_for(directory: Path) -> None:
    for df in load_phonetic_dfs(directory):
        print(df.head())


def _print_with_next(directory: Path) -> None:
    csv_file_paths = list(directory.glob("*.csv"))

    df = load_phonetic_dfs(directory)
    try:
        for _ in range(len(csv_file_paths)):
            print(next(df))
    except StopIteration:
        print("StopIteration 時にやりたいことをする。")


if __name__ == "__main__":
    _print_with_for(_DATA_DIR)
    print("==========")
    _print_with_next(_DATA_DIR)

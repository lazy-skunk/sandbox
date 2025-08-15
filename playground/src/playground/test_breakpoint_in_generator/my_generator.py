from collections.abc import Generator

import pandas as pd


def generate_phonetic_dfs() -> Generator[pd.DataFrame]:
    codes = ["Alpha", "Bravo", "Charlie", "Delta", "Echo"]
    for code in codes:
        yield pd.DataFrame({"code": [code]})

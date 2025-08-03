from typing import Self

import pandas as pd


class FeatureEngineer:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df.copy()

    def add_square_feature(self, col: str) -> Self:
        self._df[f"{col}_squared"] = self._df[col] ** 2
        return self

    def add_length_feature(self, col: str) -> Self:
        self._df[f"{col}_length"] = self._df[col].astype(str).str.len()
        return self

    def get_df(self) -> pd.DataFrame:
        return self._df

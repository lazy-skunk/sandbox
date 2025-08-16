from pathlib import Path

import pandas as pd

from playground.test_breakpoint_in_generator.my_generator import (
    load_phonetic_dfs,
)

_BASE_DIR = Path(__file__).parent
_DATA_DIR = _BASE_DIR / "data"


def test_load_phonetic_dfs() -> None:
    expected_dfs = [
        pd.DataFrame({"code": ["Alpha"]}),
        pd.DataFrame({"code": ["Bravo"]}),
        pd.DataFrame({"code": ["Charlie"]}),
        pd.DataFrame({"code": ["Delta"]}),
        pd.DataFrame({"code": ["Echo"]}),
    ]
    for actual_df, expected_df in zip(
        load_phonetic_dfs(_DATA_DIR), expected_dfs, strict=True
    ):
        pd.testing.assert_frame_equal(actual_df, expected_df)

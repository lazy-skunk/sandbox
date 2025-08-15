import pandas as pd

from playground.test_breakpoint_in_generator.my_generator import (
    generate_phonetic_dfs,
)


def test_generate_phonetic_dfs() -> None:
    expected_dfs = [
        pd.DataFrame({"code": ["Alpha"]}),
        pd.DataFrame({"code": ["Bravo"]}),
        pd.DataFrame({"code": ["Charlie"]}),
        pd.DataFrame({"code": ["Delta"]}),
        pd.DataFrame({"code": ["Echo"]}),
    ]
    actual_dfs = generate_phonetic_dfs()
    for actual_df, expected_df in zip(actual_dfs, expected_dfs, strict=True):
        pd.testing.assert_frame_equal(actual_df, expected_df)

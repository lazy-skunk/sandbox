from workspace.test_breakpoint_in_generator.my_generator import (
    _DATA_DIR,
    load_phonetic_dfs,
)


def test_load_phonetic_dfs() -> None:
    expected_codes = ["Alpha", "Bravo", "Charlie", "Delta", "Echo"]

    actual_codes = [
        actual_df.loc[0, "code"] for actual_df in load_phonetic_dfs(_DATA_DIR)
    ]

    assert sorted(actual_codes) == expected_codes

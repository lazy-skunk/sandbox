import pandas as pd
import pytest
from pandas import testing as tm


def test_assert_frame_equal_pass() -> None:
    df1 = pd.DataFrame({"A": [1], "B": [2]})
    df2 = pd.DataFrame({"A": [1], "B": [2]})

    tm.assert_frame_equal(df1, df2)


def test_assert_frame_equal_fail() -> None:
    df1 = pd.DataFrame({"A": [1], "B": [2]}, dtype="int")
    df2 = pd.DataFrame({"A": [1], "B": [2]}, dtype="float")

    with pytest.raises(AssertionError):
        tm.assert_frame_equal(df1, df2)


def test_assert_series_equal_pass() -> None:
    s1 = pd.Series([1, 2, 3])
    s2 = pd.Series([1, 2, 3])

    tm.assert_series_equal(s1, s2)


def test_assert_series_equal_fail() -> None:
    s1 = pd.Series([1, 2, 3], dtype="int")
    s2 = pd.Series([1, 2, 3], dtype="float")

    with pytest.raises(AssertionError):
        tm.assert_series_equal(s1, s2)

import pandas as pd
import pytest

from src.test_pytest.feature_engineer import FeatureEngineer


@pytest.fixture
def base_df() -> pd.DataFrame:
    return pd.DataFrame({"value": [1, 2], "name": ["foo", "bar"]})


class TestFeatureEngineer:
    def test_add_features(self, base_df: pd.DataFrame) -> None:
        result = (
            FeatureEngineer(base_df)
            .add_square_feature("value")
            .add_length_feature("name")
            .get_df()
        )

        expected = pd.DataFrame(
            {
                "value": [1, 2],
                "name": ["foo", "bar"],
                "value_squared": [1, 4],
                "name_length": [3, 3],
            }
        )

        pd.testing.assert_frame_equal(result, expected)

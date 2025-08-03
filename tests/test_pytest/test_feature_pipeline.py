import pandas as pd
import pytest
from pytest_mock import MockerFixture

from sandbox.test_pytest.src.feature_pipeline import run_feature_pipeline


class TestRunFeaturePipeline:
    @pytest.fixture
    def sample_df(self) -> pd.DataFrame:
        return pd.DataFrame({"value": [1, 2], "name": ["foo", "bar"]})

    @pytest.fixture
    def expected_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "value": [1, 2],
                "name": ["foo", "bar"],
                "value_squared": [1, 4],
                "name_length": [3, 3],
            }
        )

    @pytest.fixture(autouse=True)
    def setup(
        self,
        mocker: MockerFixture,
        sample_df: pd.DataFrame,
        expected_df: pd.DataFrame,
    ) -> None:
        self.mock_client_class = mocker.patch(
            "sandbox.test_pytest.src.feature_pipeline.DatabaseClient"
        )
        self.mock_client = self.mock_client_class.return_value
        self.mock_client.fetch_table.return_value = sample_df

        self.mock_engineer_class = mocker.patch(
            "sandbox.test_pytest.src.feature_pipeline.FeatureEngineer"
        )
        self.mock_engineer = self.mock_engineer_class.return_value
        self.mock_engineer.add_square_feature.return_value = self.mock_engineer
        self.mock_engineer.add_length_feature.return_value = self.mock_engineer
        self.mock_engineer.get_df.return_value = expected_df

        self.sample_df = sample_df
        self.expected_df = expected_df

    def test_pipeline_success(self) -> None:
        result = run_feature_pipeline(
            dbname="dummy",
            user="user",
            password="pass",
            host="localhost",
            port=5432,
            table_name="users",
        )

        self.mock_client_class.assert_called_once()
        self.mock_client.fetch_table.assert_called_once_with("users")
        self.mock_engineer_class.assert_called_once_with(self.sample_df)
        self.mock_engineer.add_square_feature.assert_called_once_with("value")
        self.mock_engineer.add_length_feature.assert_called_once_with("name")
        self.mock_engineer.get_df.assert_called_once()

        pd.testing.assert_frame_equal(result, self.expected_df)

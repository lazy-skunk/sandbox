import pandas as pd
import pytest
from pytest_mock import MockerFixture

from sandbox.test_pytest.database_client import DatabaseClient


@pytest.fixture
def db_params() -> dict:
    return {
        "dbname": "postgres",
        "user": "user",
        "password": "pass",
        "host": "localhost",
        "port": 5432,
    }


class TestDatabaseClient:
    def test_fetch_table(self, mocker: MockerFixture, db_params: dict) -> None:
        mock_conn = mocker.MagicMock()
        mock_connect = mocker.patch(
            "sandbox.test_pytest.database_client.psycopg2.connect",
            return_value=mock_conn,
        )

        expected_df = pd.DataFrame({"id": [1], "name": ["test"]})
        mock_read_sql = mocker.patch(
            "sandbox.test_pytest.database_client.pd.read_sql",
            return_value=expected_df,
        )

        client = DatabaseClient(**db_params)
        result = client.fetch_table("users")

        mock_connect.assert_called_once_with(**db_params)
        mock_read_sql.assert_called_once_with("SELECT * FROM users", mock_conn)
        pd.testing.assert_frame_equal(result, expected_df)

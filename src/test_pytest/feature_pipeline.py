import pandas as pd

from src.test_pytest.database_client import DatabaseClient
from src.test_pytest.feature_engineer import FeatureEngineer


def run_feature_pipeline(
    dbname: str,
    user: str,
    password: str,
    host: str,
    port: int,
    table_name: str,
) -> pd.DataFrame:
    db_client = DatabaseClient(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    df = db_client.fetch_table(table_name)

    df_with_features = (
        FeatureEngineer(df)
        .add_square_feature("value")
        .add_length_feature("name")
        .get_df()
    )

    return df_with_features

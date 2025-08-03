import pandas as pd

from sandbox.test_pytest.src.database_client import DatabaseClient
from sandbox.test_pytest.src.feature_engineer import FeatureEngineer


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

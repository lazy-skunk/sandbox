import pandas as pd
import psycopg2
from psycopg2.extensions import connection


class DatabaseClient:
    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str,
        port: int = 5432,
    ) -> None:
        self.conn: connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )

    def fetch_table(self, table_name: str) -> pd.DataFrame:
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, self.conn)

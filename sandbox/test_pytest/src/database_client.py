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

    def setup_table(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT NOT NULL
                );
            """
            )
            cur.execute(
                """
                INSERT INTO users (name, email)
                VALUES 
                    ('Alice', 'alice@example.com'),
                    ('Bob', 'bob@example.com'),
                    ('Charlie', 'charlie@example.com')
                ON CONFLICT DO NOTHING;
            """
            )
            print("Table 'users' created and populated.")

    def fetch_table(self, table_name: str) -> pd.DataFrame:
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, self.conn)


if __name__ == "__main__":
    import os

    client = DatabaseClient(
        dbname=os.getenv("POSTGRES_DB", "testdb"),
        user=os.getenv("POSTGRES_USER", "testuser"),
        password=os.getenv("POSTGRES_PASSWORD", "secret"),
        host="postgres",
    )

    client.setup_table()

    df = client.fetch_table("users")
    print(df)

from collections.abc import Generator, Sequence
from contextlib import contextmanager
from types import TracebackType
from typing import Self

import psycopg2
from psycopg2.extras import RealDictCursor, RealDictRow

from workspace.common.postgres_settings import (
    PostgresSettings,
    load_postgres_settings,
)


class PostgresClient:
    def __init__(self, settings: PostgresSettings) -> None:
        self._settings = settings
        self._connection: psycopg2.extensions.connection | None = None

    def _connection_params(self) -> dict[str, str | int]:
        return {
            "dbname": self._settings.postgres_db_name,
            "user": self._settings.postgres_user,
            "password": self._settings.postgres_password,
            "host": self._settings.postgres_host,
            "port": self._settings.postgres_port,
        }

    def execute(self, sql: str, params: tuple = ()) -> None:
        if self._connection is None:
            raise RuntimeError(
                "Database connection not established. Use context manager."
            )

        with self._cursor() as cursor:
            cursor.execute(sql, params)

    def executemany(self, sql: str, param_list: Sequence[tuple]) -> None:
        if self._connection is None:
            raise RuntimeError(
                "Database connection not established. Use context manager."
            )

        with self._cursor() as cursor:
            cursor.executemany(sql, param_list)

    def fetchone(self, sql: str, params: tuple = ()) -> RealDictRow | None:
        if self._connection is None:
            raise RuntimeError(
                "Database connection not established. Use context manager."
            )

        with self._cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone()

    def fetchall(self, sql: str, params: tuple = ()) -> list[RealDictRow]:
        if self._connection is None:
            raise RuntimeError(
                "Database connection not established. Use context manager."
            )

        with self._cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def _connect(self) -> None:
        if self._connection is None:
            self._connection = psycopg2.connect(**self._connection_params())

    @contextmanager
    def _cursor(self) -> Generator[psycopg2.extensions.cursor]:
        if self._connection is None:
            raise RuntimeError(
                "Database connection not established. Use context manager."
            )

        with self._connection.cursor(cursor_factory=RealDictCursor) as cursor:
            yield cursor

    def _commit(self) -> None:
        if self._connection:
            self._connection.commit()

    def _rollback(self) -> None:
        if self._connection:
            self._connection.rollback()

    def _close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None

    def __del__(self) -> None:
        self._close()

    def __enter__(self) -> Self:
        self._connect()
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        exception_traceback: TracebackType | None,
    ) -> None:
        if exception_type:
            self._rollback()
        else:
            self._commit()
        self._close()


def _how_to_use() -> None:
    settings = load_postgres_settings()

    with PostgresClient(settings) as db:
        db.execute(
            """
            CREATE TEMP TABLE temp_users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER
            )
            """
        )

        db.executemany(
            "INSERT INTO temp_users (name, age) VALUES (%s, %s)",
            [("Alice", 10), ("Bob", 20), ("Charlie", 30)],
        )

        user = db.fetchone(
            "SELECT * FROM temp_users WHERE name = %s", ("Alice",)
        )
        if user:
            print("fetchone result:")
            print(user["name"], user["age"])

        users = db.fetchall("SELECT * FROM temp_users")
        print("Fetchall results:")
        for user in users:
            print(user["name"], user["age"])


if __name__ == "__main__":
    _how_to_use()

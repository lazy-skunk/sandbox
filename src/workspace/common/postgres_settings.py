from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parents[3] / ".env",
        extra="ignore",
    )

    postgres_host: str
    postgres_port: int
    postgres_db_name: str
    postgres_user: str
    postgres_password: str


@lru_cache
def load_postgres_settings() -> PostgresSettings:
    return PostgresSettings()  # type: ignore


settings = load_postgres_settings()

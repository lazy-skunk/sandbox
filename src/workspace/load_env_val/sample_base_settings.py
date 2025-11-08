from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from workspace.load_env_val.sample_base_model import SampleBaseModel


class SampleBaseSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).with_name(".env"),
        env_nested_delimiter="__",
        extra="ignore",
    )
    string: str = "dummy"
    sample_base_model: SampleBaseModel = Field(default_factory=SampleBaseModel)

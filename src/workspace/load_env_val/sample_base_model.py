from typing import Any

from pydantic import BaseModel, Field, field_validator


class SampleBaseModel(BaseModel):
    num_int: int | None = Field(default=None)
    num_float: float | None = Field(default=None)
    num_int_none: int | None = Field(default=None)
    num_float_none: float | None = Field(default=None)

    @field_validator(
        "num_int", "num_float", "num_int_none", "num_float_none", mode="before"
    )
    @staticmethod
    def empty_string_to_none(value: Any) -> Any:
        if value == "":
            return None
        return value

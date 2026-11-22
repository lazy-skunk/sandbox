from typing import Annotated, Literal, TypedDict

from pydantic import BaseModel, Field

CalculationMethod = Literal["add", "multiply"]


class CalculationPair(TypedDict):
    num_a: int
    num_b: int
    method: CalculationMethod


CalculationRequest = Annotated[list[CalculationPair], Field(max_length=100)]


class CalculationResult(CalculationPair):
    score: int


class CalculationResults(TypedDict):
    scores: list[CalculationResult]


class CalculationResponse(BaseModel):
    status: Literal[0]
    message: str
    results: CalculationResults

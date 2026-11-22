from typing import Annotated

from fastapi import Body, Depends, FastAPI, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from schemas import (
    CalculationRequest,
    CalculationResponse,
    CalculationResult,
)

app = FastAPI()
bearer = HTTPBearer(auto_error=False)


def validate_bearer_token(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Depends(bearer)
    ],
) -> None:
    if credentials is None or credentials.credentials != "sample-token":
        raise HTTPException(
            401,
            "有効な Bearer トークンが必要です。",
            headers={"WWW-Authenticate": "Bearer"},
        )


@app.post("/calculate", dependencies=[Depends(validate_bearer_token)])
def calculate(
    body: Annotated[CalculationRequest, Body()],
) -> CalculationResponse:
    scores: list[CalculationResult] = []
    for pair in body:
        num_a = pair["num_a"]
        num_b = pair["num_b"]
        if pair["method"] == "add":  # noqa: SIM108
            score = num_a + num_b
        else:
            score = num_a * num_b
        scores.append({**pair, "score": score})

    return CalculationResponse(
        status=0, message="Success", results={"scores": scores}
    )

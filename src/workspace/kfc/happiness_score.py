from pathlib import Path
from typing import Any

import yaml
from pulp import LpMaximize, LpProblem, LpStatus, LpVariable, lpSum, value

from workspace.common.sandbox_logger import SandboxLogger

_CONFIG_PATH = Path(__file__).with_suffix(".yaml")
_logger = SandboxLogger.get_logger()


def _load_config(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as file:
        return yaml.safe_load(file)


def _happiness_score(
    item: dict[str, float], weight: dict[str, float]
) -> float:
    return sum(weight[key] * item[key] for key in weight)


def _compute_scores(
    items: dict[str, dict[str, float]], weights: dict[str, float]
) -> dict[str, float]:
    return {
        name: _happiness_score(data, weights) for name, data in items.items()
    }


def _build_problem(
    items: dict[str, dict[str, float]], scores: dict[str, float], budget: float
) -> tuple[LpProblem, dict[str, LpVariable]]:
    problem = LpProblem("happiness_maximization", LpMaximize)
    quantities = {
        name: LpVariable(f"quantity_{name}", lowBound=0, cat="Integer")
        for name in items
    }
    problem += lpSum(scores[name] * quantities[name] for name in items)
    problem += (
        lpSum(items[name]["price"] * quantities[name] for name in items)
        <= budget
    )
    return problem, quantities


def _summarize_solution(
    items: dict[str, dict[str, float]],
    scores: dict[str, float],
    quantities: dict[str, LpVariable],
) -> tuple[float, float]:
    total_price = 0.0
    total_happiness = 0.0
    for name in items:
        count = int(value(quantities[name]))
        price = items[name]["price"] * count
        happiness = scores[name] * count

        total_price += price
        total_happiness += happiness

        _logger.info(f"{name}: {count=}, {happiness=:.1f}, {price=:.0f}")
    return total_price, total_happiness


def main() -> None:
    config = _load_config(_CONFIG_PATH)
    items: dict[str, dict[str, float]] = config["items"]
    weights: dict[str, float] = config["weights"]
    budget = config["budget"]

    scores = _compute_scores(items, weights)

    problem, quantities = _build_problem(items, scores, budget)
    problem.solve()

    _logger.info(f"{LpStatus[problem.status]=}")
    total_price, total_happiness = _summarize_solution(
        items, scores, quantities
    )
    _logger.info(f"{total_happiness=:.1f}, {total_price=:.0f}")


if __name__ == "__main__":
    main()

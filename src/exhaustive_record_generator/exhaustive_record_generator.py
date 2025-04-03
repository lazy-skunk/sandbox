from itertools import product
from typing import Any

import pandas as pd


class ExhaustiveRecordGenerator:
    def __init__(self, column_value_map: dict[str, list[Any]]) -> None:
        self._column_value_map = column_value_map

    def generate_tuples(self) -> list[tuple[Any, ...]]:
        column_names = list(self._column_value_map.keys())
        value_combinations = [
            self._column_value_map[column_name] for column_name in column_names
        ]

        records: list[tuple[Any, ...]] = []
        for record in product(*value_combinations):
            records.append(record)

        return records

    def generate_dataframe(self) -> pd.DataFrame:
        column_names = list(self._column_value_map.keys())
        return pd.DataFrame(self.generate_tuples(), columns=column_names)


def example() -> None:
    sample_data: dict[str, list] = {
        "bool": [True, False],
        "int": [0, 1, 2],
        "str": ["A", "B", "C", "D"],
    }
    generator = ExhaustiveRecordGenerator(sample_data)

    df = generator.generate_dataframe()
    print(df)

    records = generator.generate_tuples()
    for record in records:
        print(record)


if __name__ == "__main__":
    example()
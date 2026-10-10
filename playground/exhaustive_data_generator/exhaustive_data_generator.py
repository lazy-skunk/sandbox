from itertools import product
from typing import Any

import pandas as pd


def create_cartesian_df(
    column_value_map: dict[str, list[Any]],
) -> pd.DataFrame:
    column_names = list(column_value_map.keys())

    value_lists = []
    for column_name in column_names:
        column_values = column_value_map[column_name]
        value_lists.append(column_values)

    cartesian_rows = product(*value_lists)
    cartesian_df = pd.DataFrame(cartesian_rows, columns=column_names)

    return cartesian_df

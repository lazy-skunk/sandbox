from itertools import product
from typing import Any

import pandas
from pandas import DataFrame


def generate_data_frame(
    column_value_map: dict[str, list[Any]],
) -> DataFrame:
    column_names = list(column_value_map.keys())
    value_combinations = [
        column_value_map[column_name] for column_name in column_names
    ]
    exhaustive_data_frame = DataFrame(
        product(*value_combinations), columns=column_names
    )
    return exhaustive_data_frame


def cross_join(
    left_data_frame: DataFrame, right_data_frame: DataFrame
) -> DataFrame:
    CROSS_JOIN_KEY = "_cross_join_key"
    DUMMY_VALUE = 0
    left_data_frame[CROSS_JOIN_KEY] = DUMMY_VALUE
    right_data_frame[CROSS_JOIN_KEY] = DUMMY_VALUE

    cross_joined_data_frame = pandas.merge(
        left_data_frame, right_data_frame, on=CROSS_JOIN_KEY
    )
    cross_joined_data_frame = cross_joined_data_frame.drop(
        columns=CROSS_JOIN_KEY
    )

    return cross_joined_data_frame


def generate_exhaustive_combinations(
    data_frames: list[DataFrame],
) -> DataFrame:
    base_data_frame = data_frames[0]
    for data_frame in data_frames[1:]:
        base_data_frame = cross_join(base_data_frame, data_frame)

    return base_data_frame


def split_combined_data_frame(
    combined_data_frame: DataFrame,
    original_data_dicts: list[dict[str, list[Any]]],
) -> list[DataFrame]:
    split_data_frames = []
    for data_dict in original_data_dicts:
        column_names = list(data_dict.keys())
        split_data_frame = combined_data_frame[column_names].copy()
        split_data_frames.append(split_data_frame)

    return split_data_frames


def add_sequential_id_column(
    data_frame: DataFrame, column_name: str = "_sequential_id"
) -> DataFrame:
    data_frame[column_name] = range(1, len(data_frame) + 1)
    return data_frame


def example() -> None:
    data_dicts = []
    table_1_data: dict[str, list[Any]] = {
        "table_1_col_1": ["い", "ろ", "は"],
        "table_1_col_2": [1],
        "table_1_col_3": [True, False, None],
    }
    data_dicts.append(table_1_data)

    table_2_data: dict[str, list[Any]] = {
        "table_2_col_1": ["イ", "ロ"],
        "table_2_col_2": [10, 20],
        "table_2_col_3": [True, False, None],
    }
    data_dicts.append(table_2_data)

    table_3_data: dict[str, list[Any]] = {
        "table_3_col_1": ["i"],
        "table_3_col_2": [100, 200, 300],
        "table_3_col_3": [True, False, None],
    }
    data_dicts.append(table_3_data)

    original_data_frames = []
    for data_dict in data_dicts:
        data_frame = generate_data_frame(data_dict)
        original_data_frames.append(data_frame)

    combined_data_frame = generate_exhaustive_combinations(
        original_data_frames
    )

    split_data_frames = split_combined_data_frame(
        combined_data_frame, data_dicts
    )
    for i, data_frame in enumerate(split_data_frames, 1):
        data_frame_id_attached = add_sequential_id_column(data_frame)
        print(data_frame_id_attached)
        data_frame.to_csv(f"output{i}.csv", index=False, encoding="utf-8")


if __name__ == "__main__":
    example()

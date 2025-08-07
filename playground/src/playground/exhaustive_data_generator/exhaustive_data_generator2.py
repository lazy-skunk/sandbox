import copy
import logging
import sys
from collections.abc import Callable
from datetime import datetime, timedelta
from itertools import product
from pathlib import Path
from typing import Any, TypedDict

import pandas as pd

_YYYY_MM_DD = "%Y-%m-%d"
_YEAR_ONLY = "%Y"
_NOW = datetime.now()
_TODAY = _NOW.strftime(_YYYY_MM_DD)
_THIS_YEAR = _NOW.strftime(_YEAR_ONLY)
_LAST_YEAR = (_NOW - timedelta(days=365)).strftime(_YEAR_ONLY)


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(module)s.%(funcName)s - %(message)s",  # noqa E501
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
_logger = logging.getLogger(__name__)


class TableMeta(TypedDict):
    schema_name: str
    table_name: str
    join_key_column: str
    column_value_map: dict[str, list[str | int | bool | datetime | None]]


_base_tables_meta1: list[TableMeta] = [
    {
        "schema_name": "sandbox",
        "table_name": "table_1",
        "join_key_column": "table_1_join_key_column",
        "column_value_map": {
            "table_1_join_key_column": [""],
            "table_1_col_1": ["い"],
            "table_1_col_2": [1],
            "table_1_col_3": [True, False, None],
            "datetime": [_TODAY, _THIS_YEAR, _LAST_YEAR],
        },
    },
    {
        "schema_name": "sandbox",
        "table_name": "table_2",
        "join_key_column": "table_2_join_key_column",
        "column_value_map": {
            "table_2_join_key_column": [""],
            "table_2_col_1": ["ろ"],
            "table_2_col_2": [2],
            "table_2_col_3": [True, False, None],
        },
    },
    {
        "schema_name": "sandbox",
        "table_name": "table_3",
        "join_key_column": "table_3_join_key_column",
        "column_value_map": {
            "table_3_join_key_column": [""],
            "table_3_col_1": ["は"],
            "table_3_col_2": [3],
            "table_3_col_3": [True, False, None],
        },
    },
]

_base_tables_meta2: list[TableMeta] = [
    {
        "schema_name": "sandbox",
        "table_name": "table_4",
        "join_key_column": "table_4_join_key_column",
        "column_value_map": {
            "table_4_join_key_column": [""],
            "table_4_col_1": ["あ"],
            "table_4_col_2": ["い"],
        },
    },
    {
        "schema_name": "sandbox",
        "table_name": "table_5",
        "join_key_column": "table_5_join_key_column",
        "column_value_map": {
            "table_5_join_key_column": [""],
            "table_5_col_1": ["か"],
        },
    },
]
_base_tables_meta_list = [_base_tables_meta1, _base_tables_meta2]

ColumnValueMap = dict[str, list[str | int | bool | datetime | None]]
TableOverwrite = dict[str, ColumnValueMap]
DataPatternMeta = dict[str, TableOverwrite]


class TestCaseMeta(TypedDict):
    data_pattern: DataPatternMeta


class TestPatternMeta(TypedDict):
    cases: dict[str, TestCaseMeta]
    expected_condition: Callable[[pd.DataFrame], pd.Series]


TestPatterns = dict[str, TestPatternMeta]
_test_scenarios: TestPatterns = {
    "flag_1": {
        "cases": {
            "true": {
                "data_pattern": {
                    "parent_table_1": {
                        "table_1": {
                            "table_1_col_1": [1],
                            "table_1_col_2": [None],
                            "table_1_col_3": [None],
                            "datetime": [None],
                        },
                    },
                    "parent_table_2": {
                        "table_4": {"table_4_col_1": [None]},
                        "table_5": {"table_5_col_1": [None]},
                    },
                }
            },
            "false": {
                "data_pattern": {
                    "parent_table_1": {
                        "table_1": {"table_1_col_1": [None]},
                        "table_2": {"table_2_col_3": [None]},
                    }
                }
            },
        },
        "expected_condition": lambda df: (
            (df["table_1_col_1"] == 1) & (df["table_3_col_3"])
        ),
    },
    "flag_2": {
        "cases": {
            "true": {
                "data_pattern": {
                    "parent_table_1": {
                        "table_1": {"table_1_col_3": [None]},
                        "table_2": {"table_2_col_3": [None]},
                    },
                    "parent_table_2": {
                        "table_1": {"table_1_col_3": [None]},
                        "table_2": {"table_2_col_3": [None]},
                    },
                }
            },
            "false": {
                "data_pattern": {
                    "parent_table_1": {
                        "table_1": {"table_1_col_3": [True]},
                        "table_2": {"table_2_col_3": [None]},
                    }
                }
            },
        },
        "expected_condition": lambda df: (
            (df["table_1_col_3"].isna()) & (df["table_2_col_3"].isna())
        ),
    },
}

_columns_to_keep = [
    "table_1_col_1",
    "table_2_col_2",
    "table_3_col_3",
    "datetime",
]


def _estimate_combination_count(tables_meta: list[TableMeta]) -> int:
    total_combination_count = 1

    for table_meta in tables_meta:
        table_combination_count = 1
        schema_name = table_meta["schema_name"]
        table_name = table_meta["table_name"]
        column_value_map = table_meta["column_value_map"]

        for column_values in column_value_map.values():
            num_values = len(column_values)
            table_combination_count *= num_values

        _logger.debug(
            f"{schema_name}.{table_name}: {table_combination_count=:,}"
        )
        total_combination_count *= table_combination_count

    _logger.debug(f"{total_combination_count=:,}")
    return total_combination_count


def _generate_cartesian_df(
    column_value_map: dict[str, list[Any]],
) -> pd.DataFrame:
    column_names = list(column_value_map.keys())
    value_lists = [
        column_value_map[column_name] for column_name in column_names
    ]
    cartesian_df = pd.DataFrame(product(*value_lists), columns=column_names)

    return cartesian_df


def _expand_df_to_total_rows(
    df: pd.DataFrame, total_combinations: int
) -> pd.DataFrame:
    repeat_count = total_combinations // len(df)
    return pd.concat([df] * repeat_count, ignore_index=True)


def _assign_sequential_id(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    df[column_name] = range(1, len(df) + 1)
    return df


def _generate_insert_sql(
    df: pd.DataFrame, schema_name: str, table_name: str
) -> str:
    columns = ", ".join(df.columns)
    values = _convert_df_to_sql_insert_values(df)
    sql = (
        "INSERT INTO\n"
        f"{schema_name}.{table_name}\n"
        f"({columns})\n"
        "VALUES\n"
        f"{values};"
    )
    return sql


def _convert_df_to_sql_insert_values(df: pd.DataFrame) -> str:
    rows = [
        "(" + ", ".join(_format_value(cell) for cell in row) + ")"
        for row in df.itertuples(index=False, name=None)
    ]
    insert_values = ",\n".join(rows)

    return insert_values


def _format_value(value: Any) -> str:
    if pd.isna(value):
        return "NULL"
    elif isinstance(value, str):
        return f"'{value}'"
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def _cross_join_all(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    base_df = dfs[0]
    for df in dfs[1:]:
        base_df = base_df.merge(df, how="cross")
    return base_df


def example() -> None:  # noqa C901
    base_dir = Path("src/exhaustive_data_generator/output")
    base_dir.mkdir(parents=True, exist_ok=True)

    for target_name, scenario in _test_scenarios.items():
        cases = scenario["cases"]
        expected_condition = scenario["expected_condition"]

        for case_name, case in cases.items():
            test_dir = base_dir / target_name / case_name
            test_dir.mkdir(parents=True, exist_ok=True)

            data_pattern = case["data_pattern"]
            base_tables_meta_list = copy.deepcopy(_base_tables_meta_list)
            base_tables = ["parent_table_1", "parent_table_2"]

            for tables_meta, parent_table in zip(
                base_tables_meta_list,
                base_tables,
                strict=False,
            ):
                for table_meta in tables_meta:
                    table_name = table_meta["table_name"]
                    meta_overwrites = data_pattern.get(parent_table, {})
                    table_overwrites = meta_overwrites.get(table_name, {})

                    for column, value_map in table_overwrites.items():
                        column_value_map = table_meta["column_value_map"]
                        column_value_map[column] = value_map

            representative_table_dfs = []
            for meta_group in base_tables_meta_list:
                total_combinations = _estimate_combination_count(meta_group)
                dfs_before_join = []

                for table_meta in meta_group:
                    schema_name = table_meta["schema_name"]
                    table_name = table_meta["table_name"]
                    join_key_column = table_meta["join_key_column"]
                    column_value_map = table_meta["column_value_map"]

                    cartesian_df = _generate_cartesian_df(column_value_map)
                    expanded_df = _expand_df_to_total_rows(
                        cartesian_df, total_combinations
                    )
                    expanded_df_with_sequential_id = _assign_sequential_id(
                        expanded_df, join_key_column
                    )
                    dfs_before_join.append(expanded_df_with_sequential_id)

                    csv_file_path = (
                        test_dir / f"{schema_name}_{table_name}.csv"
                    )
                    expanded_df_with_sequential_id.to_csv(
                        csv_file_path, index=False
                    )

                    sql = _generate_insert_sql(
                        expanded_df_with_sequential_id, schema_name, table_name
                    )
                    sql_file_path = (
                        test_dir / f"insert_{schema_name}_{table_name}.sql"
                    )
                    sql_file_path.write_text(sql, encoding="utf-8")

                representative_table_df = pd.concat(dfs_before_join, axis=1)
                representative_table_dfs.append(representative_table_df)

            cross_joined_df = _cross_join_all(representative_table_dfs)
            # cross_joined_df_path = test_dir / "cross_joined_df.csv"
            # cross_joined_df.to_csv(cross_joined_df_path, index=False)

            row_extracted_df = cross_joined_df.loc[
                expected_condition(cross_joined_df)
            ]
            # row_extracted_df_path = test_dir / "row_extracted_df.csv"
            # row_extracted_df.to_csv(row_extracted_df_path, index=False)

            column_extracted_df = row_extracted_df[_columns_to_keep]
            # column_extracted_df_path = test_dir / "column_extracted_df.csv"
            # column_extracted_df.to_csv(column_extracted_df_path, index=False)

            expected_df = column_extracted_df.drop_duplicates()
            expected_df_path = test_dir / "expected.csv"
            expected_df.to_csv(expected_df_path, index=False)


if __name__ == "__main__":
    example()

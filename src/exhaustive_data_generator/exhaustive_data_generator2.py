import logging
import sys
from datetime import datetime, timedelta
from itertools import product
from pathlib import Path
from typing import Any, TypedDict

import pandas as pd

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
    column_value_map: dict[str, list[Any]]


def _generate_date_variations() -> dict[str, str]:
    YYYY_MM_DD = "%Y-%m-%d"
    YEAR_ONLY = "%Y"
    now = datetime.now()

    return {
        "today": now.strftime(YYYY_MM_DD),
        "this_year": now.strftime(YEAR_ONLY),
        "last_year": (now - timedelta(days=365)).strftime(YEAR_ONLY),
        "2_years_ago": (now - timedelta(days=365 * 2)).strftime(YEAR_ONLY),
        "3_years_ago": (now - timedelta(days=365 * 3)).strftime(YEAR_ONLY),
    }


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


def _convert_df_to_sql_insert_values(df: pd.DataFrame) -> str:
    def _format_value(value: Any) -> str:
        if pd.isna(value):
            return "NULL"
        elif isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        return str(value)

    rows = [
        "(" + ", ".join(_format_value(cell) for cell in row) + ")"
        for row in df.itertuples(index=False, name=None)
    ]
    insert_values = ",\n".join(rows)

    return insert_values


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


def _cross_merge_all(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    base_df = dfs[0]
    for df in dfs[1:]:
        base_df = base_df.merge(df, how="cross")
    return base_df


def example() -> None:
    date_variations = _generate_date_variations()

    tables_meta1: list[TableMeta] = [
        {
            "schema_name": "sandbox",
            "table_name": "table_1",
            "join_key_column": "table_1_join_key_column",
            "column_value_map": {
                "table_1_join_key_column": [""],
                "table_1_col_1": ["い", "ろ", "は"],
                "table_1_col_2": [1],
                "table_1_col_3": [True, False, None],
                "datetime": [
                    date_variations["today"],
                    date_variations["this_year"],
                    date_variations["last_year"],
                ],
            },
        },
        {
            "schema_name": "sandbox",
            "table_name": "table_2",
            "join_key_column": "table_2_join_key_column",
            "column_value_map": {
                "table_2_join_key_column": [""],
                "table_2_col_1": ["イ"],
                "table_2_col_2": [10],
                "table_2_col_3": [True, False, None],
            },
        },
        {
            "schema_name": "sandbox",
            "table_name": "table_3",
            "join_key_column": "table_3_join_key_column",
            "column_value_map": {
                "table_3_join_key_column": [""],
                "table_3_col_1": ["i"],
                "table_3_col_2": [100],
                "table_3_col_3": [True, False, None],
            },
        },
    ]

    tables_meta2: list[TableMeta] = [
        {
            "schema_name": "sandbox",
            "table_name": "table_4",
            "join_key_column": "table_4_join_key_column",
            "column_value_map": {
                "table_4_join_key_column": [""],
                "table_4_col_1": ["い"],
                "table_4_col_2": ["ろ"],
                "table_4_col_3": ["は"],
            },
        },
        {
            "schema_name": "sandbox",
            "table_name": "table_5",
            "join_key_column": "table_5_join_key_column",
            "column_value_map": {
                "table_5_join_key_column": [""],
                "table_5_col_1": ["に"],
                "table_5_col_2": ["ほ"],
                "table_5_col_3": ["へ"],
            },
        },
    ]

    tables_meta_list = [tables_meta1, tables_meta2]
    representative_table_dfs: list[pd.DataFrame] = []

    base_dir = Path("src/exhaustive_data_generator/output")
    base_dir.mkdir(parents=True, exist_ok=True)

    for tables_meta in tables_meta_list:
        total_combinations = _estimate_combination_count(tables_meta)
        _logger.info(f"{total_combinations=}")

        if total_combinations > 5000:
            _logger.warning(
                "環境次第では、Out of Memory の可能性があるので処理を中断します。"
            )
            return

        dfs_before_join = []
        for table_meta in tables_meta:
            column_value_map = table_meta["column_value_map"]
            join_key_column = table_meta["join_key_column"]

            cartesian_df = _generate_cartesian_df(column_value_map)
            expanded_df = _expand_df_to_total_rows(
                cartesian_df, total_combinations
            )
            expanded_df_with_sequential_id = _assign_sequential_id(
                expanded_df, join_key_column
            )

            dfs_before_join.append(expanded_df_with_sequential_id)

        for df, table_meta in zip(dfs_before_join, tables_meta):
            schema_name = table_meta["schema_name"]
            table_name = table_meta["table_name"]

            csv_file_name = f"{schema_name}_{table_name}.csv"
            csv_file_path = base_dir / csv_file_name
            df.to_csv(csv_file_path, index=False, encoding="utf-8")

            sql_file_name = f"insert_{schema_name}_{table_name}.sql"
            sql_file_path = base_dir / sql_file_name
            sql = _generate_insert_sql(df, schema_name, table_name)
            sql_file_path.write_text(sql, encoding="utf-8")

        representative_table_df = pd.concat(dfs_before_join, axis=1)
        representative_table_dfs.append(representative_table_df)

    cross_merged_df = _cross_merge_all(representative_table_dfs)

    must_include_condition = (
        (cross_merged_df["table_1_col_1"] == "い")
        & (pd.isna(cross_merged_df["table_1_col_3"]))
        & (pd.isna(cross_merged_df["table_2_col_3"]))
    )
    row_extracted_df = cross_merged_df.loc[must_include_condition]

    columns_to_keep = [
        "table_1_col_1",
        "table_2_col_2",
        "table_3_col_3",
        "datetime",
    ]
    column_extracted_df = row_extracted_df[columns_to_keep]

    expected_df = column_extracted_df.drop_duplicates()

    expected_csv_file_path = base_dir / "expected.csv"
    expected_df.to_csv(expected_csv_file_path, index=False, encoding="utf-8")


if __name__ == "__main__":
    example()

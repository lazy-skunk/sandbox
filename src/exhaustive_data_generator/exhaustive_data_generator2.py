from datetime import datetime, timedelta
from itertools import product
from pathlib import Path
from typing import Any, TypedDict

import pandas as pd


class TableMeta(TypedDict):
    schema_name: str
    table_name: str
    join_key_column: str
    column_value_map: dict[str, list[Any]]


def _generate_date_variations() -> dict[str, str]:
    YEAR = "%Y%m%d"
    now = datetime.now()

    return {
        "today": now.strftime(YEAR),
        "yesterday": (now - timedelta(days=1)).strftime(YEAR),
        "tomorrow": (now + timedelta(days=1)).strftime(YEAR),
        "30_days_ago": (now - timedelta(days=30)).strftime(YEAR),
        "1_year_ago": (now - timedelta(days=365)).strftime(YEAR),
        "3_years_ago": (now - timedelta(days=365 * 3)).strftime(YEAR),
        "10_years_ago": (now - timedelta(days=365 * 10)).strftime(YEAR),
    }


def _estimate_combination_count(tables_meta: list[TableMeta]) -> int:
    total_combination_count = 1

    for table_meta in tables_meta:
        table_name = f"{table_meta['schema_name']}.{table_meta['table_name']}"
        table_combination_count = 1
        print(f"{table_name=}")

        for column_values in table_meta["column_value_map"].values():
            num_values = len(column_values)
            table_combination_count *= num_values

        print(f"  {table_combination_count=:,} patterns")
        total_combination_count *= table_combination_count

    print(f"{total_combination_count=:,} total patterns")
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


def _prepare_df_for_join(
    table_meta: TableMeta, total_combinations: int
) -> pd.DataFrame:
    column_value_map = table_meta["column_value_map"]
    join_key_column = table_meta["join_key_column"]

    base_df = _generate_cartesian_df(column_value_map)

    repeat_count = total_combinations // len(base_df)
    expanded_df = pd.concat([base_df] * repeat_count, ignore_index=True)
    expanded_df[join_key_column] = range(1, total_combinations + 1)

    return expanded_df


def _prepare_output_file_path(table_meta: TableMeta, extension: str) -> Path:
    base_dir = Path("src/exhaustive_data_generator/output")
    base_dir.mkdir(parents=True, exist_ok=True)

    schema_name = table_meta["schema_name"]
    table_name = table_meta["table_name"]
    file_name = f"{schema_name}_{table_name}.{extension}"

    file_path = base_dir / file_name

    return file_path


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


def _save_insert_sql_to_file(
    df: pd.DataFrame, table_name: str, file_path: Path
) -> None:
    columns = ", ".join(df.columns)
    values = _convert_df_to_sql_insert_values(df)
    sql = f"INSERT INTO {table_name} ({columns})\nVALUES\n{values};"
    file_path.write_text(sql, encoding="utf-8")


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
                    date_variations["yesterday"],
                ],
            },
        },
        {
            "schema_name": "sandbox",
            "table_name": "table_2",
            "join_key_column": "table_2_join_key_column",
            "column_value_map": {
                "table_2_join_key_column": [""],
                "table_2_col_1": ["イ", "ロ"],
                "table_2_col_2": [10, 20],
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
                "table_3_col_2": [100, 200, 300],
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
    combined_dfs: list[pd.DataFrame] = []

    for tables_meta in tables_meta_list:

        total_combinations = _estimate_combination_count(tables_meta)

        dfs_before_join = [
            _prepare_df_for_join(table_meta, total_combinations)
            for table_meta in tables_meta
        ]

        for df, table_meta in zip(dfs_before_join, tables_meta):
            csv_file_path = _prepare_output_file_path(table_meta, "csv")
            df.to_csv(csv_file_path, index=False, encoding="utf-8")

            sql_file_path = _prepare_output_file_path(table_meta, "sql")
            _save_insert_sql_to_file(
                df, table_meta["table_name"], sql_file_path
            )

        combined_df = pd.concat(dfs_before_join, axis=1)
        combined_dfs.append(combined_df)

    df1 = combined_dfs[0]
    df2 = combined_dfs[1]
    expected_df = df1.merge(df2, how="cross")

    must_include_condition = (
        (expected_df["table_1_col_3"] == True)
        & (expected_df["table_2_col_3"] == True)
        & (expected_df["table_3_col_3"] == True)
    )
    columns_to_keep = [
        "table_1_col_1",
        "table_2_col_2",
        "table_3_col_3",
        "datetime",
    ]
    expected_df = expected_df.loc[must_include_condition, columns_to_keep]

    expected_df.to_csv(
        "src/exhaustive_data_generator/output/expected_df.csv",
        index=False,
        encoding="utf-8",
    )


if __name__ == "__main__":
    example()

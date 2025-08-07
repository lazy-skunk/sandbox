from datetime import datetime, timedelta
from itertools import product
from pathlib import Path
from typing import Any, TypedDict

import pandas as pd

_now = datetime.now()


class TableMeta(TypedDict):
    schema_name: str
    table_name: str
    join_key_column: str
    column_value_map: dict[str, list[Any]]


def _generate_date_variations() -> dict[str, str]:
    FORMAT = "%Y%m%d"
    now = datetime.now()

    return {
        "today": now.strftime(FORMAT),
        "yesterday": (now - timedelta(days=1)).strftime(FORMAT),
        "tomorrow": (now + timedelta(days=1)).strftime(FORMAT),
        "30_days_ago": (now - timedelta(days=30)).strftime(FORMAT),
        "1_year_ago": (now - timedelta(days=365)).strftime(FORMAT),
        "3_years_ago": (now - timedelta(days=365 * 3)).strftime(FORMAT),
        "10_years_ago": (now - timedelta(days=365 * 10)).strftime(FORMAT),
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


def _generate_cartesian_df_from_column_value_map(
    column_value_map: dict[str, list[Any]],
) -> pd.DataFrame:
    column_names = list(column_value_map.keys())
    value_lists = [
        column_value_map[column_name] for column_name in column_names
    ]
    cartesian_df = pd.DataFrame(product(*value_lists), columns=column_names)

    return cartesian_df


def _generate_pseudo_joined_df(
    column_value_map: dict[str, list[Any]], total_combinations: int
) -> pd.DataFrame:
    cartesian_df = _generate_cartesian_df_from_column_value_map(
        column_value_map
    )
    table_pattern_count = len(cartesian_df)
    repetition = total_combinations // table_pattern_count
    repeated_df = pd.concat([cartesian_df] * repetition, ignore_index=True)
    return repeated_df


def _cross_join_dfs(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    CROSS_JOIN_KEY = "_cross_join_key"
    DUMMY_VALUE = 0

    left_df = dfs[0]
    for right_df in dfs[1:]:
        left_df[CROSS_JOIN_KEY] = DUMMY_VALUE
        right_df[CROSS_JOIN_KEY] = DUMMY_VALUE

        cross_joined_df = pd.merge(left_df, right_df, on=CROSS_JOIN_KEY)
        left_df = cross_joined_df.drop(columns=CROSS_JOIN_KEY)

    return left_df


def _split_df_by_table_meta(
    combined_df: pd.DataFrame, tables_meta: list[TableMeta]
) -> list[pd.DataFrame]:
    split_dfs = []
    for table_meta in tables_meta:
        column_names = list(table_meta["column_value_map"].keys())
        split_dfs.append(combined_df[column_names])

    return split_dfs


def _assign_join_key_values(
    df: pd.DataFrame, column_name: str
) -> pd.DataFrame:
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
    df.loc[:, column_name] = range(1, len(df) + 1)
    return df


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

    tables_meta: list[TableMeta] = [
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

    total_combinations = _estimate_combination_count(tables_meta)

    pseudo_joined_dfs = [
        _generate_pseudo_joined_df(
            table_meta["column_value_map"], total_combinations
        )
        for table_meta in tables_meta
    ]
    for i, df in enumerate(pseudo_joined_dfs, 1):
        df.to_csv(f"{i}.csv", index=False)

    cartesian_dfs = [
        _generate_cartesian_df_from_column_value_map(
            table_meta["column_value_map"]
        )
        for table_meta in tables_meta
    ]

    # 全網羅のデータを組み合わせた場合の件数を知りたいので結合する。
    combined_cartesian_df = _cross_join_dfs(cartesian_dfs)

    # 各テーブルの挿入データを作るため、分解する。
    split_cartesian_dfs = _split_df_by_table_meta(
        combined_cartesian_df, tables_meta
    )

    dfs_to_create_expected_df = []
    for table_meta, split_cartesian_df in zip(
        tables_meta, split_cartesian_dfs, strict=False
    ):
        # テーブル結合用の列に共通の番号を付与する。
        joined_key_assigned_df = _assign_join_key_values(
            split_cartesian_df, table_meta["join_key_column"]
        )

        dfs_to_create_expected_df.append(joined_key_assigned_df)

        csv_file_path = _prepare_output_file_path(table_meta, "csv")
        joined_key_assigned_df.to_csv(
            csv_file_path, index=False, encoding="utf-8"
        )

        sql_file_path = _prepare_output_file_path(table_meta, "sql")
        _save_insert_sql_to_file(
            joined_key_assigned_df, table_meta["table_name"], sql_file_path
        )

    expected_df = dfs_to_create_expected_df[0]

    for df, table_meta in zip(
        dfs_to_create_expected_df[1:], tables_meta[1:], strict=False
    ):
        expected_df = pd.merge(
            expected_df,
            df,
            left_on=tables_meta[0]["join_key_column"],
            right_on=table_meta["join_key_column"],
            how="inner",
        )

    exclude_conditions = {
        "all_col3_null": (
            expected_df["table_1_col_3"].isna()
            & expected_df["table_2_col_3"].isna()
            & expected_df["table_3_col_3"].isna()
        ),
        "today_is_invalid": expected_df["datetime"]
        == date_variations["today"],
    }
    for exclude_condition in exclude_conditions.values():
        remaining_rows = ~exclude_condition
        expected_df = expected_df.loc[remaining_rows]

    expected_df_path = Path(
        "src/exhaustive_data_generator/output/expected_df.csv"
    )
    expected_df.to_csv(expected_df_path, index=False, encoding="utf-8")


if __name__ == "__main__":
    example()

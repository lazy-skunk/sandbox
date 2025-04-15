from datetime import datetime, timedelta
from itertools import cycle, product
from pathlib import Path
from typing import Any, Generator, Hashable, TypedDict

import pandas as pd


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
        table_combination_count = 1

        for column_values in table_meta["column_value_map"].values():
            table_combination_count *= len(column_values)

        total_combination_count *= table_combination_count

    return total_combination_count


def _generate_cartesian_df_from_column_value_map(
    column_value_map: dict[str, list[Any]],
) -> pd.DataFrame:
    column_names = list(column_value_map.keys())
    value_lists = [column_value_map[name] for name in column_names]
    return pd.DataFrame(product(*value_lists), columns=column_names)


def _generate_pseudo_joined_rows(
    column_value_map: dict[str, list[Any]],
    total_combinations: int,
    join_key_column: str | None = None,
) -> Generator[dict[Hashable, Any], Any, None]:
    base_df = _generate_cartesian_df_from_column_value_map(column_value_map)
    repeated_iter = cycle(base_df.to_dict(orient="records"))

    for i in range(1, total_combinations + 1):
        row = next(repeated_iter)

        if join_key_column:
            row = row.copy()
            row[join_key_column] = i
        yield row


def _write_pseudo_joined_csv(
    column_value_map: dict[str, list[Any]],
    total_combinations: int,
    output_path: Path,
    join_key_column: str | None = None,
    batch_size: int = 100_000,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows = _generate_pseudo_joined_rows(
        column_value_map, total_combinations, join_key_column
    )
    buffer = []
    with output_path.open("w", encoding="utf-8") as f:
        for i, row in enumerate(rows, 1):
            buffer.append(row)

            if i % batch_size == 0:
                pd.DataFrame(buffer).to_csv(
                    f, index=False, header=(i == batch_size)
                )
                buffer.clear()
        if buffer:
            pd.DataFrame(buffer).to_csv(
                f, index=False, header=(i < batch_size)
            )


def _format_sql_value(value: Any) -> str:
    if pd.isna(value):
        return "NULL"
    elif isinstance(value, str):
        return f"'{value}'"
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def _write_pseudo_joined_sql(
    column_value_map: dict[str, list[Any]],
    total_combinations: int,
    table_name: str,
    output_path: Path,
    join_key_column: str | None = None,
    batch_size: int = 100_000,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows = _generate_pseudo_joined_rows(
        column_value_map, total_combinations, join_key_column
    )

    column_names = list(column_value_map.keys())
    if join_key_column and join_key_column not in column_names:
        column_names = [join_key_column] + column_names

    column_list_sql = ", ".join(column_names)

    with output_path.open("w", encoding="utf-8") as f:
        buffer = []
        for i, row in enumerate(rows, 1):
            values = [_format_sql_value(row[col]) for col in column_names]
            buffer.append(f"({', '.join(values)})")

            if i % batch_size == 0:
                insert_stmt = (
                    f"INSERT INTO {table_name} ({column_list_sql})\nVALUES\n"
                )
                insert_stmt += ",\n".join(buffer) + ";\n\n"
                f.write(insert_stmt)
                buffer.clear()

        if buffer:
            insert_stmt = (
                f"INSERT INTO {table_name} ({column_list_sql})\nVALUES\n"
            )
            insert_stmt += ",\n".join(buffer) + ";\n\n"
            f.write(insert_stmt)


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
                "table_1_col_2": list(range(1, 10)),
                "table_1_col_3": [True, False, None],
                "datetime": list(date_variations.values()),
            },
        },
        {
            "schema_name": "sandbox",
            "table_name": "table_2",
            "join_key_column": "table_2_join_key_column",
            "column_value_map": {
                "table_2_join_key_column": [""],
                "table_2_col_1": ["イ", "ロ"],
                "table_2_col_2": list(range(10, 100, 10)),
                "table_2_col_3": [True, False, None],
                "datetime": list(date_variations.values()),
            },
        },
        {
            "schema_name": "sandbox",
            "table_name": "table_3",
            "join_key_column": "table_3_join_key_column",
            "column_value_map": {
                "table_3_col_1": ["i"],
                "table_3_join_key_column": [""],
                "table_3_col_2": list(range(100, 1000, 100)),
                "table_3_col_3": [True, False, None],
                "datetime": list(date_variations.values()),
            },
        },
    ]

    total_combinations = _estimate_combination_count(tables_meta)

    for table_meta in tables_meta:
        output_path = Path(
            f"src/exhaustive_data_generator/output/{table_meta['schema_name']}_{table_meta['table_name']}.csv"
        )
        _write_pseudo_joined_csv(
            table_meta["column_value_map"],
            total_combinations,
            output_path,
            join_key_column=table_meta["join_key_column"],
        )

        sql_output_path = Path(
            f"src/exhaustive_data_generator/output/{table_meta['schema_name']}_{table_meta['table_name']}.sql"
        )
        _write_pseudo_joined_sql(
            column_value_map=table_meta["column_value_map"],
            total_combinations=total_combinations,
            table_name=table_meta["table_name"],
            output_path=sql_output_path,
            join_key_column=table_meta["join_key_column"],
        )


if __name__ == "__main__":
    example()

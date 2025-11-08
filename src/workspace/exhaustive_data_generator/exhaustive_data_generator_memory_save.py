import logging
import sys
from collections.abc import Generator
from datetime import datetime, timedelta
from itertools import cycle, product
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
    YEAR = "%Y"
    now = datetime.now()

    return {
        "today": now.strftime(YEAR),
        "yesterday": (now - timedelta(days=1)).strftime(YEAR),
        "tomorrow": (now + timedelta(days=1)).strftime(YEAR),
        "30_days_ago": (now - timedelta(days=30)).strftime(YEAR),
        "3_years_ago": (now - timedelta(days=365 * 3)).strftime(YEAR),
    }


def _estimate_combination_count(tables_meta: list[TableMeta]) -> int:
    total_combination_count = 1

    for table_meta in tables_meta:
        schema = table_meta["schema_name"]
        table = table_meta["table_name"]
        table_full_name = f"{schema}.{table}"

        table_combination_count = 1
        for _column, values in table_meta["column_value_map"].items():
            value_count = len(values)
            table_combination_count *= value_count

        _logger.debug(
            f"{table_full_name} の組み合わせ数: {table_combination_count:,}"
        )
        total_combination_count *= table_combination_count

    return total_combination_count


def _generate_pseudo_joined_rows(
    table_meta: TableMeta,
    total_combinations: int,
) -> Generator[dict[str, Any]]:
    column_value_map = table_meta["column_value_map"]
    join_key_column = table_meta["join_key_column"]

    column_names = list(column_value_map.keys())
    value_lists = [column_value_map[name] for name in column_names]
    cartesian_iter = product(*value_lists)

    for i, values in enumerate(cycle(cartesian_iter), 1):
        if i > total_combinations:
            break

        row = dict(zip(column_names, values, strict=False))

        if join_key_column:
            row[join_key_column] = i

        yield row


def _write_pseudo_joined_csv(
    table_meta: TableMeta,
    total_combinations: int,
    file_path: Path,
    batch_size: int = 10_000,
) -> None:
    rows = _generate_pseudo_joined_rows(table_meta, total_combinations)
    buffer = []
    with file_path.open("w", encoding="utf-8") as file:
        for i, row in enumerate(rows, 1):
            buffer.append(row)

            if i % batch_size == 0:
                pd.DataFrame(buffer).to_csv(
                    file, index=False, header=(i == batch_size)
                )
                buffer.clear()
        if buffer:
            pd.DataFrame(buffer).to_csv(
                file, index=False, header=(i < batch_size)
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
    table_meta: TableMeta,
    total_combinations: int,
    file_path: Path,
    batch_size: int = 100_000,
) -> None:
    column_value_map = table_meta["column_value_map"]
    schema_name = table_meta["schema_name"]
    table_name = table_meta["table_name"]
    rows = _generate_pseudo_joined_rows(table_meta, total_combinations)

    column_names = list(column_value_map.keys())
    column_list_sql = ", ".join(column_names)

    with file_path.open("w", encoding="utf-8") as f:
        buffer = []
        for i, row in enumerate(rows, 1):
            values = [_format_sql_value(row[col]) for col in column_names]
            buffer.append(f"({', '.join(values)})")

            if i % batch_size == 0:
                insert_stmt = f"INSERT INTO\n{schema_name}.{table_name}"
                insert_stmt += f" ({column_list_sql})\nVALUES\n"
                insert_stmt += ",\n".join(buffer) + ";\n\n"
                f.write(insert_stmt)
                buffer.clear()

        if buffer:
            insert_stmt = f"INSERT INTO\n{schema_name}.{table_name}"
            insert_stmt += f" ({column_list_sql})\nVALUES\n"
            insert_stmt += ",\n".join(buffer) + ";\n\n"
            f.write(insert_stmt)


def main() -> None:
    _logger.info("開始")
    date_variations = _generate_date_variations()

    tables_meta: list[TableMeta] = [
        {
            "schema_name": "sandbox",
            "table_name": "table_1",
            "join_key_column": "table_1_join_key_column",
            "column_value_map": {
                "table_1_join_key_column": [""],
                "table_1_col_1": ["い", "ろ", "は"],
                "table_1_col_2": list(range(1, 10, 5)),
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
                "table_2_col_2": list(range(10, 100, 50)),
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
                "table_3_col_2": list(range(100, 1000, 500)),
                "table_3_col_3": [True, False, None],
                "datetime": list(date_variations.values()),
            },
        },
    ]

    total_combinations = _estimate_combination_count(tables_meta)
    _logger.info(f"全体の組み合わせ総数: {total_combinations:,}")

    base_dir = Path("src/exhaustive_data_generator/output")
    base_dir.mkdir(parents=True, exist_ok=True)

    for table_meta in tables_meta:
        schema_name = table_meta["schema_name"]
        table_name = table_meta["table_name"]

        csv_file_name = f"{schema_name}_{table_name}.csv"
        csv_file_path = Path(base_dir / csv_file_name)
        _logger.info(f"{csv_file_path} を作成します。")
        _write_pseudo_joined_csv(table_meta, total_combinations, csv_file_path)
        _logger.info(f"{csv_file_path} を作成しました。")

        sql_file_name = f"{schema_name}_{table_name}.sql"
        sql_file_path = Path(base_dir / sql_file_name)
        _logger.info(f"{sql_file_path} を作成します。")
        _write_pseudo_joined_sql(table_meta, total_combinations, sql_file_path)
        _logger.info(f"{sql_file_path} を作成しました。")


if __name__ == "__main__":
    main()

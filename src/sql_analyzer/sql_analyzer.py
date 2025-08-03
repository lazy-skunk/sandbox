from typing import Any

from sqlglot import exp, parse_one
from sqlglot.dialects.dialect import DialectType
from sqlglot.expressions import Expression


class SQLAnalyzer:
    def __init__(
        self,
        sql: str,
        read: DialectType | None = None,
        dialect: DialectType | None = None,
        **parse_options: Any,
    ) -> None:
        self._sql: str = sql
        self._syntax_tree: Expression = parse_one(
            sql, read=read, dialect=dialect, **parse_options
        )

    def analyze_ctes(self) -> None:
        print("# [CTEs]")

        for i, cte in enumerate(self._syntax_tree.find_all(exp.CTE), 1):
            print(f"## CTE {i}: `{cte.alias}`")
            print("``` sql")
            print(f"{cte.this.sql()}")
            print("```")
            print()

    def analyze_joins(self) -> None:
        print("# [JOINs]")

        for i, join in enumerate(self._syntax_tree.find_all(exp.Join), 1):
            on_expression = join.args.get("on")

            if on_expression is not None:
                print(f"- JOIN {i}: `{on_expression.sql()}`")
            print()

    def analyze_wheres(self) -> None:
        print("# [WHEREs]")

        for i, where in enumerate(self._syntax_tree.find_all(exp.Where), 1):
            print(f"- WHERE {i}: `{where.this.sql()}`")
            print()

    def analyze_case_when(self) -> None:
        print("# [CASE WHEN]")

        for i, case in enumerate(self._syntax_tree.find_all(exp.Case), 1):
            print(f"## CASE {i}")

            for j, if_clause in enumerate(case.args.get("ifs", []), 1):
                when_condition = if_clause.this
                then_value = if_clause.args.get("true")
                print(f"- WHEN {j}: `{when_condition.sql()}`")
                print(f"  THEN: `{then_value.sql()}`")

            default = case.args.get("default")
            if default:
                print(f"- ELSE: `{default.sql()}`")

            print()


if __name__ == "__main__":
    sql = """
    CREATE TABLE final_result AS
    WITH
    filtered_orders AS (
        SELECT
            o.user_id AS id,
            o.amount,
            CASE
                WHEN u.status = 'active'
                    AND o.amount > 100
                    AND (
                        o.created_at >= '2024-01-01'
                        OR o.priority = 'high'
                    )
                    THEN 'active_high'
                WHEN u.status = 'vip'
                    AND o.amount > 300
                    THEN 'vip_premium'
                ELSE 'other'
            END AS flag
        FROM orders o
        JOIN users u ON u.id = o.user_id
        WHERE (
            u.status = 'active'
            AND o.amount > 100
            AND (
                o.created_at >= '2024-01-01'
                OR o.priority = 'high'
            )
        )
        OR (
            u.status = 'vip'
            AND o.amount > 300
        )
    ),
    tmp AS (
        SELECT id, amount FROM filtered_orders
    )
    SELECT id, SUM(amount) AS total_amount
    FROM tmp
    GROUP BY id
    HAVING total_amount > 200;
    """

    analyzer = SQLAnalyzer(sql)
    analyzer.analyze_ctes()
    analyzer.analyze_joins()
    analyzer.analyze_wheres()
    analyzer.analyze_case_when()

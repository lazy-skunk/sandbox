import marimo

__generated_with = "0.25.2"
app = marimo.App()


@app.cell
def _():
    import polars as pl

    from playground.common.postgres_client import PostgresClient
    from playground.common.postgres_settings import PostgresSettings

    settings = PostgresSettings()
    return PostgresClient, pl, settings


@app.cell
def _(PostgresClient, settings):
    with PostgresClient(settings) as _db:
        _db.execute("DROP TABLE IF EXISTS EMPLOYEES CASCADE")
        _db.execute("DROP TABLE IF EXISTS ATTENDANCES CASCADE")
        _db.execute(
            "\n        CREATE TABLE IF NOT EXISTS EMPLOYEES (\n            EMPLOYEE_ID SERIAL PRIMARY KEY,\n            NAME TEXT NOT NULL\n        );\n        "
        )
        _db.execute(
            "\n        CREATE TABLE IF NOT EXISTS ATTENDANCES (\n            ATTENDANCE_ID SERIAL PRIMARY KEY,\n            EMPLOYEE_ID INTEGER NOT NULL REFERENCES EMPLOYEES (EMPLOYEE_ID),\n            ATTENDANCE_DATE DATE NOT NULL\n        );\n        "
        )
        _db.executemany(
            "\n        INSERT INTO\n            EMPLOYEES (NAME)\n        VALUES\n            (%s)\n        ",
            [("Alfa",), ("Bravo",), ("Charlie",)],
        )
        _db.executemany(
            "\n        INSERT INTO\n            ATTENDANCES (EMPLOYEE_ID, ATTENDANCE_DATE)\n        VALUES\n            (%s, CURRENT_DATE)\n        ",
            [(1,), (2,)],
        )
    database_ready = True
    return (database_ready,)


@app.cell
def _(PostgresClient, database_ready, pl, settings):
    assert database_ready
    with PostgresClient(settings) as _db:
        employees = _db.fetchall("SELECT * FROM EMPLOYEES")
    employees_df = pl.DataFrame(employees)
    employees_df
    return


@app.cell
def _(PostgresClient, database_ready, pl, settings):
    assert database_ready
    with PostgresClient(settings) as _db:
        attendances = _db.fetchall("SELECT * FROM ATTENDANCES")
    attendances_df = pl.DataFrame(attendances)
    attendances_df
    return


@app.cell
def _(PostgresClient, database_ready, pl, settings):
    assert database_ready
    with PostgresClient(settings) as _db:
        before_extract = _db.fetchall(
            "\n        SELECT\n            CAST(EMPLOYEES.EMPLOYEE_ID AS INTEGER),\n            EMPLOYEES.NAME,\n            ATTENDANCES.ATTENDANCE_ID,\n            ATTENDANCES.ATTENDANCE_DATE\n        FROM\n            EMPLOYEES\n            LEFT JOIN ATTENDANCES ON EMPLOYEES.EMPLOYEE_ID = ATTENDANCES.EMPLOYEE_ID\n        "
        )
    before_extract_df = pl.DataFrame(before_extract)
    before_extract_df  # noqa: E501
    return


@app.cell
def _(PostgresClient, database_ready, pl, settings):
    assert database_ready
    with PostgresClient(settings) as _db:
        after_extract = _db.fetchall(
            "\n        SELECT\n            CAST(EMPLOYEES.EMPLOYEE_ID AS INTEGER),\n            EMPLOYEES.NAME,\n            ATTENDANCES.ATTENDANCE_ID,\n            ATTENDANCES.ATTENDANCE_DATE\n        FROM\n            EMPLOYEES\n            LEFT JOIN ATTENDANCES ON EMPLOYEES.EMPLOYEE_ID = ATTENDANCES.EMPLOYEE_ID\n        WHERE\n            ATTENDANCES.EMPLOYEE_ID IS NULL\n        "
        )
    after_extract_df = pl.DataFrame(after_extract)
    after_extract_df  # noqa: E501
    return


if __name__ == "__main__":
    app.run()

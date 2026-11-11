import marimo

__generated_with = "0.25.2"
app = marimo.App()

with app.setup:
    import marimo as mo
    import polars as pl

    from playground.common.postgres_client import PostgresClient
    from playground.common.postgres_settings import PostgresSettings


@app.cell
def _():
    settings = PostgresSettings()
    return (settings,)


@app.cell
def _(settings):
    with PostgresClient(settings) as _db:
        _db.execute("DROP TABLE IF EXISTS EMPLOYEES CASCADE")
        _db.execute("DROP TABLE IF EXISTS ATTENDANCES CASCADE")
        _db.execute(
            """
                CREATE TABLE IF NOT EXISTS EMPLOYEES (
                EMPLOYEE_ID SERIAL PRIMARY KEY,
                NAME TEXT NOT NULL
                );
            """
        )
        _db.execute(
            """
                CREATE TABLE IF NOT EXISTS ATTENDANCES (
                ATTENDANCE_ID SERIAL PRIMARY KEY,
                EMPLOYEE_ID INTEGER NOT NULL REFERENCES EMPLOYEES
                (EMPLOYEE_ID),
                ATTENDANCE_DATE DATE NOT NULL
                );
            """
        )
        _db.executemany(
            (
                """
                    INSERT INTO
                    EMPLOYEES (NAME)
                    VALUES
                    (%s)
                """
            ),
            [("Alfa",), ("Bravo",), ("Charlie",)],
        )
        _db.executemany(
            (
                """
                    INSERT INTO
                    ATTENDANCES (EMPLOYEE_ID, ATTENDANCE_DATE)
                    VALUES
                    (%s, CURRENT_DATE)
                """
            ),
            [(1,), (2,)],
        )
    database_ready = True
    return (database_ready,)


@app.cell
def _(database_ready, settings):
    assert database_ready
    with PostgresClient(settings) as _db:
        employees = _db.fetchall("SELECT * FROM EMPLOYEES")
    employees_df = pl.DataFrame(employees)
    mo.output.append(employees_df)

    return


@app.cell
def _(database_ready, settings):
    assert database_ready
    with PostgresClient(settings) as _db:
        attendances = _db.fetchall("SELECT * FROM ATTENDANCES")
    attendances_df = pl.DataFrame(attendances)
    mo.output.append(attendances_df)

    return


@app.cell
def _(database_ready, settings):
    assert database_ready
    with PostgresClient(settings) as _db:
        before_extract = _db.fetchall(
            """
                SELECT
                CAST(EMPLOYEES.EMPLOYEE_ID AS INTEGER),
                EMPLOYEES.NAME,
                ATTENDANCES.ATTENDANCE_ID,
                ATTENDANCES.ATTENDANCE_DATE
                FROM
                EMPLOYEES
                LEFT JOIN ATTENDANCES ON EMPLOYEES.EMPLOYEE_ID =
                ATTENDANCES.EMPLOYEE_ID
            """
        )
    before_extract_df = pl.DataFrame(before_extract)
    mo.output.append(before_extract_df)

    return


@app.cell
def _(database_ready, settings):
    assert database_ready
    with PostgresClient(settings) as _db:
        after_extract = _db.fetchall(
            """
                SELECT
                CAST(EMPLOYEES.EMPLOYEE_ID AS INTEGER),
                EMPLOYEES.NAME,
                ATTENDANCES.ATTENDANCE_ID,
                ATTENDANCES.ATTENDANCE_DATE
                FROM
                EMPLOYEES
                LEFT JOIN ATTENDANCES ON EMPLOYEES.EMPLOYEE_ID =
                ATTENDANCES.EMPLOYEE_ID
                WHERE
                ATTENDANCES.EMPLOYEE_ID IS NULL
            """
        )
    after_extract_df = pl.DataFrame(after_extract)
    mo.output.append(after_extract_df)

    return


if __name__ == "__main__":
    app.run()

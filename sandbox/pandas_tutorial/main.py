from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

_ABSOLUTE_FILE_PATH = Path(__file__).resolve()
_TUTORIAL_DIR = _ABSOLUTE_FILE_PATH.parent
_DATA_DIR = _TUTORIAL_DIR / "data"

_TITANIC_CSV_PATH = _DATA_DIR / "titanic.csv"
_TITANIC_XLSX_PATH = _DATA_DIR / "titanic.xlsx"

_AIR_QUALITY_NO2_CSV_PATH = _DATA_DIR / "air_quality_no2.csv"
_AIR_QUALITY_LONG_CSV_PATH = _DATA_DIR / "air_quality_long.csv"
_AIR_QUALITY_NO2_LONG_CSV_PATH = _DATA_DIR / "air_quality_no2_long.csv"
_AIR_QUALITY_PM25_LONG_CSV_PATH = _DATA_DIR / "air_quality_pm25_long.csv"

_EMPLOYEES_CSV_PATH = _DATA_DIR / "employees.csv"
_DEPARTMENTS_CSV_PATH = _DATA_DIR / "departments.csv"


def _what_kind_of_data_does_pandas_handle() -> None:
    """
    # REMEMBER
    * Import the package, aka import pandas as pd
    * A table of data is stored as a pandas DataFrame
    * Each column in a DataFrame is a Series
    * You can do things by applying a method to a DataFrame or Series
    """
    df = pd.DataFrame(
        {
            "Name": [
                "Braund, Mr. Owen Harris",
                "Allen, Mr. William Henry",
                "Bonnell, Miss. Elizabeth",
            ],
            "Age": [22, 35, 58],
            "Sex": ["male", "male", "female"],
        }
    )
    print(df)
    print(df["Age"])

    ages = pd.Series([22, 35, 58], name="Age")
    print(ages)
    print(df["Age"].max())
    print(ages.max())

    print(df.describe())


def _how_do_I_read_and_write_tabular_data() -> None:
    """
    # REMEMBER
    * Getting data in to pandas from many different file formats
      or data sources is supported by read_* functions.
    * Exporting data out of pandas is provided by different to_*methods.
    * The head/tail/info methods and the dtypes attribute
      are convenient for a first check.
    """
    titanic_read_csv = pd.read_csv(_TITANIC_CSV_PATH)
    print(titanic_read_csv)
    print(titanic_read_csv.head(8))
    print(titanic_read_csv.tail(10))
    print(titanic_read_csv.dtypes)

    titanic_read_csv.to_excel(
        _TITANIC_XLSX_PATH, sheet_name="passengers", index=False
    )
    titanic_read_xlsx = pd.read_excel(
        _TITANIC_XLSX_PATH, sheet_name="passengers"
    )
    print(titanic_read_xlsx.head())
    titanic_read_xlsx.info()


def _how_do_I_select_a_subset_of_a_dataframe() -> None:
    """
    # REMEMBER
    * When selecting subsets of data, square brackets [] are used.
    * Inside these brackets, you can use a single column/row label
      , a list of column/row labels, a slice of labels
      , a conditional expression or a colon.
    * Select specific rows and/or columns using loc
      when using the row and column names.
    * Select specific rows and/or columns using iloc
      when using the positions in the table.
    * You can assign new values to a selection based on loc/iloc.
    """
    titanic = pd.read_csv(_TITANIC_CSV_PATH)
    print(titanic.head())

    ages = titanic["Age"]
    print(ages.head())
    print(type(titanic["Age"]))
    print(titanic["Age"].shape)

    age_sex = titanic[["Age", "Sex"]]
    print(age_sex.head())
    print(type(titanic[["Age", "Sex"]]))
    print(titanic[["Age", "Sex"]].shape)

    above_35 = titanic[titanic["Age"] > 35]
    print(above_35.head())
    print(titanic["Age"] > 35)

    class_23 = titanic[titanic["Pclass"].isin([2, 3])]
    print(class_23.head())
    class_23 = titanic[(titanic["Pclass"] == 2) | (titanic["Pclass"] == 3)]
    print(class_23.head())

    age_no_na = titanic[titanic["Age"].notna()]
    print(age_no_na.head())
    print(age_no_na.shape)

    adult_names = titanic.loc[titanic["Age"] > 35, "Name"]
    print(adult_names.head())

    print(titanic.iloc[9:25, 2:5])

    titanic.iloc[0:3, 3] = "anonymous"
    print(titanic.head())


def _how_do_I_create_plots_in_pandas() -> None:
    """
    # REMEMBER
    * The .plot.* methods are applicable on both Series and DataFrames.
    * By default, each of the columns is plotted
      as a different element (line, boxplot,…).
    * Any plot created by pandas is a Matplotlib object.
    """
    air_quality = pd.read_csv(
        _AIR_QUALITY_NO2_CSV_PATH, index_col=0, parse_dates=True
    )
    print(air_quality.head())
    air_quality.plot()

    air_quality["station_paris"].plot()

    air_quality.plot.scatter(x="station_london", y="station_paris", alpha=0.5)

    print(
        [
            method_name
            for method_name in dir(air_quality.plot)
            if not method_name.startswith("_")
        ]
    )

    air_quality.plot.box()

    axs = air_quality.plot.area(figsize=(12, 4), subplots=True)

    fig, axs = plt.subplots(figsize=(12, 4))
    air_quality.plot.area(ax=axs)
    axs.set_ylabel("NO$_2$ concentration")
    PNG_PATH = _DATA_DIR / "no2_concentrations.png"
    fig.savefig(PNG_PATH)
    plt.show()


def _how_to_create_new_columns_derived_from_existing_columns() -> None:
    """
    # REMEMBER
    * Create a new column by assigning the output to the DataFrame
      with a new column name in between the [].
    * Operations are element-wise, no need to loop over rows.
    * Use rename with a dictionary or function
      to rename row labels or column names.
    """
    air_quality = pd.read_csv(
        _AIR_QUALITY_NO2_CSV_PATH, index_col=0, parse_dates=True
    )
    print(air_quality.head())

    air_quality["london_mg_per_cubic"] = air_quality["station_london"] * 1.882
    print(air_quality.head())

    air_quality["ratio_paris_antwerp"] = (
        air_quality["station_paris"] / air_quality["station_antwerp"]
    )
    print(air_quality.head())

    air_quality_renamed = air_quality.rename(
        columns={
            "station_antwerp": "BETR801",
            "station_paris": "FR04014",
            "station_london": "London Westminster",
        }
    )
    print(air_quality_renamed.head())

    air_quality_renamed = air_quality_renamed.rename(columns=str.lower)
    print(air_quality_renamed.head())


def _how_to_calculate_summary_statistics() -> None:
    """
    # REMEMBER
    * Aggregation statistics can be calculated on entire columns or rows.
    * groupby provides the power of the split-apply-combine pattern.
    * value_counts is a convenient shortcut
      to count the number of entries in each category of a variable.
    """
    titanic = pd.read_csv(_TITANIC_CSV_PATH)
    print(titanic.head())
    print(titanic["Age"].mean())
    print(titanic[["Age", "Fare"]].median())
    print(titanic[["Age", "Fare"]].describe())
    print(
        titanic.agg(
            {
                "Age": ["min", "max", "median", "skew"],
                "Fare": ["min", "max", "median", "mean"],
            }
        )
    )
    print(titanic[["Sex", "Age"]].groupby("Sex").mean())
    print(titanic.groupby("Sex").mean(numeric_only=True))
    print(titanic.groupby("Sex")["Age"].mean())
    print(titanic.groupby(["Sex", "Pclass"])["Fare"].mean())
    print(titanic["Pclass"].value_counts())
    print(titanic.groupby("Pclass")["Pclass"].count())


def _how_to_reshape_the_layout_of_tables() -> None:
    """
    # REMEMBER
    * Sorting by one or more columns is supported by sort_values.
    * The pivot function is purely restructuring of the data
      , pivot_table supports aggregations.
    * The reverse of pivot (long to wide format) is melt (wide to long format).
    """
    titanic = pd.read_csv(_TITANIC_CSV_PATH)
    print(titanic.head())

    air_quality = pd.read_csv(
        _AIR_QUALITY_LONG_CSV_PATH, index_col="date.utc", parse_dates=True
    )
    print(air_quality.head())

    print(titanic.sort_values(by="Age").head())
    print(titanic.sort_values(by=["Pclass", "Age"], ascending=False).head())

    no2 = air_quality[air_quality["parameter"] == "no2"]
    no2_subset = no2.sort_index().groupby(["location"]).head(2)
    print(no2_subset)
    print(no2_subset.pivot(columns="location", values="value"))
    print(no2.head())

    no2.pivot(columns="location", values="value").plot()

    print(
        air_quality.pivot_table(
            values="value",
            index="location",
            columns="parameter",
            aggfunc="mean",
        )
    )
    print(
        air_quality.pivot_table(
            values="value",
            index="location",
            columns="parameter",
            aggfunc="mean",
            margins=True,
        )
    )
    print(air_quality.groupby(["parameter", "location"])[["value"]].mean())

    no2_pivoted = no2.pivot(columns="location", values="value").reset_index()
    print(no2_pivoted.head())

    no_2 = no2_pivoted.melt(id_vars="date.utc")
    print(no_2.head())

    no_2 = no2_pivoted.melt(
        id_vars="date.utc",
        value_vars=["BETR801", "FR04014", "London Westminster"],
        value_name="NO_2",
        var_name="id_location",
    )
    print(no_2.head())
    plt.show()


def _how_to_combine_data_from_multiple_tables() -> None:
    """
    # REMEMBER
    * Multiple tables can be concatenated both column-wise
      and row-wise using the concat function.
    * For database-like merging/joining of tables, use the merge function.
    """
    air_quality_no2 = pd.read_csv(
        _AIR_QUALITY_NO2_LONG_CSV_PATH, parse_dates=True
    )
    air_quality_no2 = air_quality_no2[
        ["date.utc", "location", "parameter", "value"]
    ]
    print(air_quality_no2.head())

    air_quality_pm25 = pd.read_csv(
        _AIR_QUALITY_PM25_LONG_CSV_PATH, parse_dates=True
    )
    air_quality_pm25 = air_quality_pm25[
        ["date.utc", "location", "parameter", "value"]
    ]
    print(air_quality_pm25.head())

    air_quality = pd.concat([air_quality_pm25, air_quality_no2], axis=0)
    print(air_quality.head())

    print("Shape of the ``air_quality_pm25`` table: ", air_quality_pm25.shape)
    print("Shape of the ``air_quality_no2`` table: ", air_quality_no2.shape)
    print("Shape of the resulting ``air_quality`` table: ", air_quality.shape)

    air_quality = air_quality.sort_values("date.utc")
    print(air_quality.head())

    air_quality_ = pd.concat(
        [air_quality_pm25, air_quality_no2], keys=["PM25", "NO2"]
    )
    print(air_quality_.head())

    employees = pd.read_csv(_EMPLOYEES_CSV_PATH, parse_dates=True)
    departments = pd.read_csv(_DEPARTMENTS_CSV_PATH, parse_dates=True)
    employees_with_department = pd.merge(
        employees,
        departments,
        how="left",
        left_on="department_id",
        right_on="department_id",
    )

    print(employees_with_department.head())


def _how_to_handle_time_series_data_with_ease() -> None:
    """
    # REMEMBER
    * Valid date strings can be converted to datetime objects
      using to_datetime function or as part of read functions.
    * Datetime objects in pandas support calculations, logical operations
      and convenient date-related properties using the dt accessor.
    * A DatetimeIndex contains these date-related properties
      and supports convenient slicing.
    * Resample is a powerful method to change the frequency of a time series.
    """
    air_quality = pd.read_csv(_AIR_QUALITY_NO2_LONG_CSV_PATH)
    air_quality = air_quality.rename(columns={"date.utc": "datetime"})
    print(air_quality.head())

    air_quality["datetime"] = pd.to_datetime(air_quality["datetime"])
    print(air_quality["datetime"])
    print(air_quality["datetime"].min(), air_quality["datetime"].max())

    air_quality["month"] = air_quality["datetime"].dt.month
    print(air_quality.head())

    print(
        air_quality.groupby([air_quality["datetime"].dt.weekday, "location"])[
            "value"
        ].mean()
    )

    fig, axs = plt.subplots(figsize=(12, 4))
    air_quality.groupby(air_quality["datetime"].dt.hour)["value"].mean().plot(
        kind="bar", rot=0, ax=axs
    )
    plt.xlabel("Hour of the day")
    plt.ylabel("$NO_2 (µg/m^3)$")

    no_2 = air_quality.pivot(
        index="datetime", columns="location", values="value"
    )
    print(no_2.head())
    print(no_2.index.year, no_2.index.weekday)

    no_2.loc["2019-05-20":"2019-05-21"].plot()  # type: ignore[misc]

    monthly_max = no_2.resample("ME").max()
    print(monthly_max)
    print(monthly_max.index.freq)
    no_2.resample("D").mean().plot(style="-o", figsize=(10, 5))
    plt.show()


def _how_to_manipulate_textual_data() -> None:
    """
    # REMEMBER
    * String methods are available using the str accessor.
    * String methods work element-wise
      and can be used for conditional indexing.
    * The replace method is a convenient method to convert values
      according to a given dictionary.
    """
    titanic = pd.read_csv(_TITANIC_CSV_PATH)
    print(titanic.head())
    print(titanic["Name"].str.lower())
    print(titanic["Name"].str.split(","))
    titanic["Surname"] = titanic["Name"].str.split(",").str.get(0)
    print(titanic["Surname"])
    print(titanic["Name"].str.contains("Countess"))
    print(titanic[titanic["Name"].str.contains("Countess")])
    print(titanic["Name"].str.len())
    print(titanic["Name"].str.len().idxmax())
    print(titanic.loc[titanic["Name"].str.len().idxmax(), "Name"])
    titanic["Sex_short"] = titanic["Sex"].replace({"male": "M", "female": "F"})
    print(titanic["Sex_short"])


def main() -> None:
    _what_kind_of_data_does_pandas_handle()
    _how_do_I_read_and_write_tabular_data()
    _how_do_I_select_a_subset_of_a_dataframe()
    _how_do_I_create_plots_in_pandas()
    _how_to_create_new_columns_derived_from_existing_columns()
    _how_to_calculate_summary_statistics()
    _how_to_reshape_the_layout_of_tables()
    _how_to_combine_data_from_multiple_tables()
    _how_to_handle_time_series_data_with_ease()
    _how_to_manipulate_textual_data()


if __name__ == "__main__":
    main()

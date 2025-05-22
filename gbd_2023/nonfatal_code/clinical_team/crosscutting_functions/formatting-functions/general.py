import warnings
from typing import List, Optional, Union

import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import functions as sfuncs


def date_formatter(
    df: Union[pd.DataFrame, ps.DataFrame],
    columns: Optional[List[str]] = None,
    errors: Optional[str] = "coerce",
    parse_fmt: Optional[str] = None,
    conv_fmt: str = "%Y-%m-%d",
) -> Union[pd.DataFrame, ps.DataFrame]:
    """Adjusts datetime columns in a DataFrame to be string type. By default
    converts to YYYY-MM-DD format.

    NOTE: This function will cast either columns passed to the columns arg OR
    ALL datetime columns present in the table to a string with nulls stored as
    python's NoneType.

    Args:
        df: DataFrame to format.
        columns: Specific columns of datetime
            like values to convert. If None, will attempt to
            infer datetime columns to convert via dtype inspection.
        errors: How to handle invalid parsing. Ex: 'raise' or 'coerce'.
        parse_fmt: Format to expected when parsing dates.
            None will infer the format.
        conv_fmt: Format of the final date strings.

    Raises:
        TypeError: Not a pandas Dataframe (vanilla or pyspark).
        RuntimeError: Trying to operate on numeric column. Can't assume unix time.
        RuntimeError: If any rows were lost or gained.

    Returns:
        Input DataFrame with any dates as formatted strings.
    """

    if not isinstance(df, pd.DataFrame) and not isinstance(df, ps.DataFrame):
        raise TypeError(f"Needs to be a DataFrame is not a pandas DataFrame. Found {type(df)}")

    start_shape = df.shape

    if isinstance(df, pd.DataFrame):
        dt_func = getattr(pd, "to_datetime")
        null_func = getattr(pd, "notnull")
    elif isinstance(df, ps.DataFrame):
        dt_func = getattr(ps, "to_datetime")
        null_func = getattr(ps, "notnull")

    # Infer date columns if not supplied.
    if not columns:
        columns = [col for col in df.columns if "datetime" in str(df[col].dtype)]
        if len(columns) < 1:
            warnings.warn(
                "No datetime columns inferred and none were passed. \nReturning input table."
            )
            return df
    else:
        for col in columns:
            col_dtype = str(df[col].dtype)
            if "int" in col_dtype or "float" in col_dtype:
                raise RuntimeError(
                    "Function may not perform as expected with numeric column data types."
                )
            elif "datetime" not in col_dtype:
                df[col] = dt_func(df[col], errors=errors, format=parse_fmt)

    for col in columns:
        df[col] = df[col].dt.strftime(date_format=conv_fmt)
        df[col] = df[col].where(null_func(df[col]), None)

    end_shape = df.shape

    if end_shape != start_shape:
        raise RuntimeError(f"Expected shape of {start_shape}, got {end_shape}")

    return df


def calculate_los(
    df: Union[pd.DataFrame, ps.DataFrame],
    discharge_col: str = "discharge_date",
    admission_col: str = "admission_date",
    errors: str = "coerce",
) -> Union[pd.DataFrame, ps.DataFrame]:
    """Calculates Length of Stay in days for designated date columns.

    Args:
        df (Union[pd.DataFrame, ps.DataFrame]): Table with 2 datetime like columns.
        discharge_col (str, optional): Name of column to use for discharge date.
            Defaults to "discharge_date".
        admission_col (str, optional): Name of column to use for admission date.
            Defaults to "admission_date".
        errors (Optional[str]): How to handle invalid parsing. Ex: 'raise' or 'coerce'.
            Defaults to 'coerce'.

    Returns:
        Union[pd.DataFrame, ps.DataFrame]: Input table with 'los' column added.
    """
    if isinstance(df, pd.DataFrame):
        dt_func = getattr(pd, "to_datetime")
    elif isinstance(df, ps.DataFrame):
        dt_func = getattr(ps, "to_datetime")
    else:
        raise TypeError(f"Only pandas DataFrames currenly accepted. Found {type(df)}")

    df[discharge_col] = dt_func(df[discharge_col], errors=errors)
    df[admission_col] = dt_func(df[admission_col], errors=errors)

    # pyspark.pandas timedelta methods diverge under the hood making dt accessor unusable.
    if isinstance(df, pd.DataFrame):
        df["los"] = df[discharge_col].subtract(df[admission_col])
        df["los"] = df["los"].dt.days
    elif isinstance(df, ps.DataFrame):
        df = df.to_spark()  # type:ignore[assignment]
        df = df.withColumn(
            "los", sfuncs.datediff(df[discharge_col], df[admission_col]).alias("diff")
        )
        df = df.pandas_api()  # type:ignore[operator]

    return df

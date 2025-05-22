import numpy as np
import pandas as pd
from pandas import DataFrame


def validate_columns(df: DataFrame, columns: list[str], name: str) -> None:
    """
    Validates that all specified columns are present in the DataFrame.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to validate.
    columns : list of str
        A list of expected column names that should be present in the DataFrame.
    name : str
        A name for the DataFrame, used in error messages.

    Raises
    ------
    KeyError
        If any of the specified columns are missing from the DataFrame.
    """
    missing = [col for col in columns if col not in df.columns]
    if missing:
        error_message = (
            f"{name} has missing columns: {len(missing)} columns are missing.\n"
        )
        error_message += f"Missing columns: {', '.join(missing)}\n"
        if len(missing) > 5:
            error_message += "First 5 missing columns: \n"
        error_message += ", \n".join(missing[:5])
        error_message += "\n"
        raise KeyError(error_message)


def validate_index(df: DataFrame, index: list[str], name: str) -> None:
    """
    Validates that the DataFrame does not contain duplicate indices based on specified columns.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to validate.
    index : list of str
        A list of column names to be used as the index for validation.
    name : str
        A name for the DataFrame, used in error messages.

    Raises
    ------
    ValueError
        If duplicate indices are found in the DataFrame based on the specified columns.
    """
    duplicated_index = pd.MultiIndex.from_frame(
        df[df[index].duplicated()][index]
    ).to_list()
    if duplicated_index:
        error_message = f"{name} has duplicated index with {len(duplicated_index)} indices \n"
        error_message += f"Index columns: ({', '.join(index)})\n"
        if len(duplicated_index) > 5:
            error_message += "First 5: \n"
        error_message += ", \n".join(str(idx) for idx in duplicated_index[:5])
        error_message += "\n"
        raise ValueError(error_message)


def validate_nonan(df: DataFrame, name: str) -> None:
    """
    Validates that the DataFrame does not contain any NaN values.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to validate.
    name : str
        A name for the DataFrame, used in error messages.

    Raises
    ------
    ValueError
        If any NaN values are found in the DataFrame.
    """
    nan_columns = df.columns[df.isna().any(axis=0)].to_list()
    if nan_columns:
        error_message = (
            f"{name} has NaN values in {len(nan_columns)} columns. \n"
        )
        error_message += f"Columns with NaN values: {', '.join(nan_columns)}\n"
        if len(nan_columns) > 5:
            error_message += "First 5 columns with NaN values: \n"
        error_message += ", \n".join(nan_columns[:5])
        error_message += "\n"
        raise ValueError(error_message)


def validate_positive(
    df: DataFrame, columns: list[str], name: str, strict: bool = False
) -> None:
    """
    Validates that specified columns contain non-negative or strictly positive values.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to validate.
    columns : list of str
        A list of column names to check for positive values.
    name : str
        A name for the DataFrame, used in error messages.
    strict : bool, optional
        If True, checks that values are strictly greater than zero.
        If False, checks that values are greater than or equal to zero.
        Default is False.

    Raises
    ------
    ValueError
        If any of the specified columns contain invalid (negative or zero) values.
    """
    op = "<=" if strict else "<"
    negative = [col for col in columns if df.eval(f"{col} {op} 0").any()]
    if negative:
        message = "0 or negative values in" if strict else "negative values in"
        raise ValueError(f"{name} has {message}: {negative}")


def validate_interval(
    df: DataFrame, lwr: str, upr: str, index: list[str], name: str
) -> None:
    """
    Validates that lower interval bounds are strictly less than upper bounds.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame containing interval data to validate.
    lwr : str
        The name of the column representing the lower bound of the interval.
    upr : str
        The name of the column representing the upper bound of the interval.
    index : list of str
        A list of column names to be used as the index for identifying intervals.
    name : str
        A name for the DataFrame, used in error messages.

    Raises
    ------
    ValueError
        If any lower bound is not strictly less than its corresponding upper bound.
    """
    invalid_index = pd.MultiIndex.from_frame(
        df.query(f"{lwr} >= {upr}")[index]
    ).to_list()
    if invalid_index:
        error_message = f"{name} has invalid interval with {len(invalid_index)} indices. \nLower bound must be strictly less than upper bound.\n"
        error_message += f"Index columns: ({', '.join(index)})\n"
        if len(invalid_index) > 5:
            error_message += "First 5 indices with invalid interval: \n"
        error_message += ", \n".join(str(idx) for idx in invalid_index[:5])
        error_message += "\n"
        raise ValueError(error_message)


def validate_noindexdiff(
    df_ref: DataFrame, df: DataFrame, index: list[str], name: str
) -> None:
    """
    Validates that the indices of two DataFrames match.

    Parameters
    ----------
    df_ref : pandas.DataFrame
        The reference DataFrame containing the expected indices.
    df : pandas.DataFrame
        The DataFrame to validate against the reference.
    index : list of str
        A list of column names to be used as the index for comparison.
    name : str
        A name for the validation context, used in error messages.

    Raises
    ------
    ValueError
        If there are indices in the reference DataFrame that are missing in the DataFrame to validate.
    """
    index_ref = pd.MultiIndex.from_frame(df_ref[index])
    index_to_check = pd.MultiIndex.from_frame(df[index])
    missing_index = index_ref.difference(index_to_check).to_list()

    if missing_index:
        error_message = (
            f"Missing {name} info for {len(missing_index)} indices \n"
        )
        error_message += f"Index columns: ({', '.join(index_ref.names)})\n"
        if len(missing_index) > 5:
            error_message += "First 5 missing indices: \n"
        error_message += ", \n".join(str(idx) for idx in missing_index[:5])
        error_message += "\n"
        raise ValueError(error_message)


def validate_pat_coverage(
    df: DataFrame,
    lwr: str,
    upr: str,
    pat_lwr: str,
    pat_upr: str,
    index: list[str],
    name: str,
) -> None:
    """
    Validates that the pattern intervals cover the data intervals completely without gaps or overlaps.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame containing both data intervals and pattern intervals.
    lwr : str
        The name of the column representing the data's lower bound.
    upr : str
        The name of the column representing the data's upper bound.
    pat_lwr : str
        The name of the column representing the pattern's lower bound.
    pat_upr : str
        The name of the column representing the pattern's upper bound.
    index : list of str
        A list of column names to group by when validating intervals.
    name : str
        A name for the DataFrame or validation context, used in error messages.

    Raises
    ------
    ValueError
        If the pattern intervals have gaps or overlaps, or if they do not fully cover the data intervals.
    """
    # Sort dataframe
    df = df.sort_values(index + [lwr, upr, pat_lwr, pat_upr], ignore_index=True)
    df_group = df.groupby(index)

    # Check overlap or gap in pattern
    shifted_pat_upr = df_group[pat_upr].shift(1)
    connect_index = shifted_pat_upr.notnull()
    connected = np.allclose(
        shifted_pat_upr[connect_index], df.loc[connect_index, pat_lwr]
    )

    if not connected:
        raise ValueError(
            f"{name} pattern has overlap or gap between the lower and upper "
            "bounds across categories."
        )

    # Check coverage of head and tail
    head_covered = df_group.first().eval(f"{lwr} >= {pat_lwr}").all()
    tail_covered = df_group.last().eval(f"{upr} <= {pat_upr}").all()

    if not (head_covered and tail_covered):
        raise ValueError(
            f"{name} pattern does not cover the data lower and/or upper bound"
        )


def validate_realnumber(df: DataFrame, columns: list[str], name: str) -> None:
    """
    Validates that specified columns contain real numbers, are non-zero, and are not NaN or Inf.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame containing the data to validate.
    columns : list of str
        A list of column names to validate within the DataFrame.
    name : str
        A name for the DataFrame, used in error messages.

    Raises
    ------
    ValueError
        If any column contains values that are not real numbers, are zero, or are NaN/Inf.
    """
    # Check for non-real, zero, NaN, or Inf values in the specified columns
    invalid = []
    for col in columns:
        if (
            not df[col]
            .apply(
                lambda x: isinstance(x, (int, float))
                and x != 0
                and pd.notna(x)
                and np.isfinite(x)
            )
            .all()
        ):
            invalid.append(col)

    if invalid:
        raise ValueError(f"{name} has non-real or zero values in: {invalid}")


def validate_set_uniqueness(df: DataFrame, column: str, name: str) -> None:
    """
    Validates that each list in the specified column contains unique elements.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame containing the data to validate.
    column : str
        The name of the column containing lists to validate.
    name : str
        A name for the DataFrame or validation context, used in error messages.

    Raises
    ------
    ValueError
        If any list in the specified column contains duplicate elements.
    """
    invalid_rows = df[df[column].apply(lambda x: len(x) != len(set(x)))]
    if not invalid_rows.empty:
        error_message = f"{name} has rows in column '{column}' where list elements are not unique.\n"
        error_message += (
            f"Indices of problematic rows: {invalid_rows.index.tolist()}\n"
        )
        raise ValueError(error_message)

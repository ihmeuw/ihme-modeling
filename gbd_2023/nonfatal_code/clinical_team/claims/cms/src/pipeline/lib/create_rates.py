import warnings

import pandas as pd


def create_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Given a pandas df with val (cases) and sample size (denominators)
    create an estimate rate as the 'mean' column.

    Args:
        df (pd.DataFrame): DataFrame with columns 'val' and 'sample_size'.

    Returns:
        pd.DataFrame: Input DataFrame with rate space column added.
    """

    df["mean"] = df["val"] / df["sample_size"]

    validate_rates(df)

    # identifies that the mean column is in rate space
    df["metric_id"] = 3

    # remove count space col
    df.drop("val", axis=1, inplace=True)
    return df


def validate_rates(df: pd.DataFrame) -> None:
    """Run some validations on a DataFrame after creating rates from
    val and sample size.

    Args:
        df (pd.DataFrame): DataFrame with column 'mean'.

    Raises:
        ValueError: Any negative or null 'mean' values
    """

    failures = []
    if df["mean"].any() < 0:
        failures.append("Mean should never be less than zero, but it is")

    if df["mean"].isnull().any():
        m = df["mean"].isnull().sum()
        failures.append(f"All mean rates should be non-null. There are {m} null means")

    big_mean = len(df[df["mean"] > 1])
    if big_mean > 0:
        warnings.warn(
            f"There are {big_mean} rows with means over 1. This "
            "usually identifies an issue with the data processing or is a symptom "
            "of small sample sizes"
        )

    if failures:
        raise ValueError("\n".join(failures))

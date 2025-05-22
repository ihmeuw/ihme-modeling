"""
The under 1 data needs an adjustment to convert from full year counts into
period counts which match the GBD population so that we can correctly convert
to rate space. Population is a snapshot around June for a given
under 1 time period while our admissions cover an entire year. So we can't
correctly divide 52 weeks of admits by 1 week of pop, for example.

There is also some as-yet-unused functionality to convert estimates back to
full year space using the conversion argument

conversion options:
'count_to_rate' - Use when converting full year admissions to match population
                  terms. Formula used is
                  admissions / (days_in_year / days_in_age_group)
                  e.g. to convert 100 admits for 0-6.99 day olds to match pop
                  the formula used is 100 / (365 / 6.999999) = ~1.9178 admits
'rate_to_count' - (development, currently unused) Use when converting population
                  term matching admits back to full year admissions.
"""
from typing import List, Union

import numpy as np
import pandas as pd
from crosscutting_functions import demographic
from db_tools.ezfuncs import query


def df_calculate_scalar(
    df: pd.DataFrame, conversion: str, full_agedf: pd.DataFrame
) -> pd.DataFrame:
    """Divide cohort days by this value to get denom adj. data

    Params:
        age_group_id (int):
                IHME age group id, valid on the age group table
        conversion (str):
                are we going from counts to rates, or otherwayround
            Note, this doesn't actually convert from counts to
            rates, it just prepares the under 1 data for that
            conversion.
        fill_agedf (pd.DataFrame):
            age group df pulled from DB

    Returns:
        pd.DataFrame:
            The df with adjustment values that we'll need to divide counts by
    """

    df["scalar"] = np.nan  # init col

    for age_group_id in df["age_group_id"].unique():
        agedf = full_agedf.query("age_group_id == @age_group_id")

        if len(agedf) != 1:
            raise ValueError("We expect 1 and exactly 1 row from shared age query")
        if agedf["age_group_days_end"].iloc[0] > 365:
            raise ValueError("This should only be used with under 1 age groups")

        # age_end exclusive
        age_term = (
            agedf["age_group_days_end"].iloc[0] - agedf["age_group_days_start"].iloc[0]
        ) + 0.999999

        if conversion == "count_to_rate":
            scalar = 365 / age_term
        elif conversion == "rate_to_count":
            scalar = age_term / 365
        else:
            raise ValueError(f"Can't convert {conversion}")

        df.loc[df["age_group_id"] == age_group_id, "scalar"] = scalar
    assert df["scalar"].isnull().sum() == 0

    return df


def adjust_under1_data(
    df: pd.DataFrame,
    col_to_adjust: Union[str, List[str]],
    conversion: str,
    clinical_age_group_set_id: int,
) -> pd.DataFrame:
    """Adjust the under 1 inpatient hospital data to account for person-years

    df (pd.DataFrame):
        DF of clinical data with a column to adjust and under1 data present
    col_to_adjust (Union[str, List[str]]):
        Which column(s) should have the scalar applied to it
    conversion (str):
        Sort of tricky but I built the calc scalar function to both shrink down
        the admissions so they represent 1 age group cohort, but also scale them
        back up to represent a full year, incase we need to do that in the future
    clinical_age_group_set_id (int):
        Identifies which set of 'good' clinical age groups to use

    Returns:
        pd.DataFrame: adjusted df
    """
    full_agedf = query(
        (
            """
                    QUERY
                    """"
        ),
        conn_def="epi",
    )

    # birth prevalence needs to be manually adjusted, the scalar isn't correct
    full_agedf.loc[full_agedf["age_group_id"] == 164, "age_group_days_end"] = 364

    # split out U1
    ages = demographic.get_hospital_age_groups(clinical_age_group_set_id)
    ages = ages[ages["age_end"] <= 1]
    if len(ages) == 1:
        print(
            (
                f"There is only a single under 1 age group in age set "
                f"{clinical_age_group_set_id}. Returning data unadjusted"
            )
        )
        return df
    u1 = df[df["age_group_id"].isin(ages["age_group_id"])].copy()
    df = df[~df["age_group_id"].isin(ages["age_group_id"])]

    if len(u1) == 0:
        print("There is no Under 1 data present")
        return df

    print("Performing the under 1 adjustment to account for pop-time differences")
    u1 = df_calculate_scalar(df=u1, conversion=conversion, full_agedf=full_agedf)

    # This is the adjustment to align denoms
    if isinstance(col_to_adjust, str):
        u1[col_to_adjust] = u1[col_to_adjust] / u1["scalar"]
    else:
        for col in col_to_adjust:
            u1[col] = u1[col] / u1["scalar"]

    u1 = u1.drop("scalar", axis=1)

    if set(u1.columns).symmetric_difference(df.columns):
        raise ValueError("The column names don't match")

    df = pd.concat([df, u1], sort=False, ignore_index=True)

    return df

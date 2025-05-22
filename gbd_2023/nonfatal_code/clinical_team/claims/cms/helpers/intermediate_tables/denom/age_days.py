import argparse

import numpy as np
import pandas as pd
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser

import cms.helpers.intermediate_tables.denom.U1_age_days as U1_ad
from cms.helpers.intermediate_tables.funcs import parquet_writer


def monthly_age_range(df: pd.DataFrame) -> pd.DataFrame:
    """
    NOTE: the number of days at a given age is related to the number of eligibility months.
    Age days are not calculated for missing months or gaps in eligibility.

    For example, if a bene is 18 (who turns 19 on July 1st) but is not enrolled until March
    their days_start and days_end would be 122 (4 * 30.5)
    """
    df = df.join(
        pd.DataFrame(
            {
                "age_start": df.year_id - df.dob.dt.year,
                "age_end": df.year_id - df.dob.dt.year,
                "days_start": 30.5,
                "days_end": 30.5,
            },
            index=df.index,
        )
    )

    df["age_start"] = np.where(
        df.dob.dt.month >= df.month_id, (df.year_id - 1) - df.dob.dt.year, df.age_start
    )

    df["age_end"] = np.where(df.dob.dt.month <= df.month_id, df.age_end, df.age_start)

    df["days_start"] = np.where(
        df.dob.dt.month == df.month_id, df.dob.dt.day - 1, df.days_start
    )

    df["days_end"] = np.where(
        df.dob.dt.month == df.month_id, 30.5 - df.days_start, df.days_end
    )

    return df


def age_months_diff(df: pd.DataFrame) -> pd.DataFrame:
    """
    age_months helper function
    """
    id_vars = [e for e in df.columns if not e.startswith("days") | e.startswith("age")]
    df_w = df.melt(
        id_vars=id_vars, value_vars=["days_start", "days_end", "age_start", "age_end"]
    )

    df_list = []
    for var in ["days", "age"]:
        cols = [f"{var}_start", f"{var}_end"]
        temp = df_w[df_w["variable"].isin(cols)]
        temp.rename({"value": f"{var}"}, axis=1, inplace=True)
        temp["variable"] = [e.split("_")[-1] for e in temp.variable]
        df_list.append(temp)

    return pd.merge(df_list[0], df_list[1], on=id_vars + ["variable"]).drop("variable", axis=1)


def age_months(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reshape data from age_start, age_end, days_start, days_end
    to age, days. Can be used to process both U1 and O1 benes
    """
    same_age = df[df.age_start == df.age_end]
    same_age = same_age.drop(["days_end", "age_end"], axis=1).rename(
        {"days_start": "days", "age_start": "age"}, axis=1
    )

    diff = df[df.age_start != df.age_end]
    diff = age_months_diff(diff)
    return (
        pd.concat([same_age, diff], sort=False)
        .sort_values(by=["bene_id", "month_id", "age"])
        .reset_index(drop=True)
    )


def dod_age_days(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df.dod.dt.month == df.month_id, ["days_start", "days_end"]] = df.dod.dt.day

    return df[(df.month_id <= df.dod.dt.month) | (df.dod.isnull())].sort_values(
        by=["bene_id", "age_start"]
    )


def age_years(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create ages for non U1 ages
    """
    df = monthly_age_range(df)
    return df


def u1_transition(df: pd.DataFrame) -> pd.DataFrame:
    """
    Helper func for edge case of benes who are
    U1 but their dob is in the prior year
    """
    u1 = df[df.dob.dt.month >= df.month_id]
    over_1 = df[df.dob.dt.month < df.month_id]

    u1.reset_index(drop=True, inplace=True)
    over_1.reset_index(drop=True, inplace=True)

    over_1 = age_years(over_1)
    u1 = U1_ad.prev_yr_ages(u1)

    return pd.concat([over_1, u1], sort=False).sort_values(by=["bene_id", "month_id"])


def under_1(df: pd.DataFrame) -> pd.DataFrame:
    """Process U1 age groups"""
    transition = df[df.dob.dt.year == (df.year_id - 1)]
    transition = u1_transition(transition)

    u1 = U1_ad.create_ages(df[~df.bene_id.isin(transition.bene_id.unique())])

    return (
        pd.concat([transition, u1], sort=False)
        .sort_values(by=["bene_id", "month_id"])
        .reset_index(drop=True)
    )


def reader(cms_system: str, year: int, group: int) -> pd.DataFrame:
    path_list = []

    for output in ["demo", "elig"]:
        path_list.append(
            "FILEPATH"
        )

    fmt = "%Y-%m-%d"
    df_demo = pd.read_parquet("FILEPATH")
    df_demo = df_demo[["bene_id", "dob", "dod"]]
    df_demo["dob"] = pd.to_datetime(df_demo["dob"], format=fmt)
    df_demo["dod"] = pd.to_datetime(df_demo["dod"], format=fmt)

    df_elig = pd.read_parquet("FILEPATH")
    df_elig = df_elig[["bene_id", "year_id", "month_id"]]

    return pd.merge(df_elig, df_demo, on="bene_id")


def process(cms_system: str, year: int, group: int) -> pd.DataFrame:
    df = reader(cms_system, year, group)

    # drop all possible null benes
    df = df.loc[df.bene_id.notnull()]
    df = df.loc[~df.bene_id.astype(str).str.lower().isin(["nan", "none"])]

    u1 = df[df.dob.dt.year.isin([year, (year - 1)])].reset_index(drop=True)
    u1 = under_1(u1)

    over_1 = age_years(df[~df.bene_id.isin(u1.bene_id.unique())])

    return (
        pd.concat([u1, over_1]).sort_values(by=["bene_id", "month_id"]).reset_index(drop=True)
    )


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--cms_system", type=str, required=True, help="CMS system, max or mdcr"
    )
    arg_parser.add_argument("--year", type=int, required=True, help="year for the target file")
    arg_parser.add_argument("--group", type=int, required=True, help="group number")

    args = arg_parser.parse_args()
    df = process(args.cms_system, args.year, args.group)

    # negative ages are sometimes created in the over_1 df from
    # the process() func.
    df = df[~((df.dob.dt.year == df.year_id) & (df.month_id < df.dob.dt.month))]
    # remove all null DOBs
    df = df.loc[~df.dob.isnull()]
    df.drop(["dob", "dod"], axis=1, inplace=True)
    df = age_months(df)
    base = (
        "FILEPATH"
    )
    parquet_writer(
        df=df, path="FILEPATH"
    )

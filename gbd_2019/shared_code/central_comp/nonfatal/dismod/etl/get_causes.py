import os
from pathlib import Path

from db_queries import get_demographics
import pandas as pd

"""
Given a list of models used in fauxcorrect run, compute which causes
have complete demographic sets
"""


def read_file():
    this_dir = Path(os.path.abspath(os.path.dirname(__file__)))
    fname = Path("fauxcorrect_v17_models.csv")
    return pd.read_csv(this_dir / fname)


def calc_causes(df):
    # df containing min and max age group id per cause
    cause_ranges = df.groupby("cause_id").agg(
        {"cause_age_start": "min", "cause_age_end": "max"}
    )
    cause_ranges = cause_ranges[cause_ranges.notna().any(axis=1)]

    # df containing min and max age group id per model
    model_ranges = df[
        ["model_version_id", "sex_id", "cause_id", "age_start", "age_end"]
    ]
    # filter out causes that have missing models (ie only male models present)
    causes_w_missing_models = model_ranges[
        model_ranges.isna().any(axis=1)
    ].cause_id.unique()
    model_ranges = model_ranges[~model_ranges.cause_id.isin(causes_w_missing_models)]

    most_detailed_ages = get_demographics("cod", gbd_round_id=6)["age_group_id"]

    # create a list of most detailed ages for every row of df (for both dfs)
    model_ranges["ages"] = model_ranges.apply(
        lambda row: [
            a
            for a in most_detailed_ages
            if a >= row["age_start"] and a <= row["age_end"]
        ],
        axis=1,
    )
    cause_ranges["ages"] = cause_ranges.apply(
        lambda row: [
            a
            for a in most_detailed_ages
            if a >= row["cause_age_start"] and a <= row["cause_age_end"]
        ],
        axis=1,
    )

    # for each cause/sex combo, check to see if the union of all ages present
    # in models is a superset of cause age range
    result_list = []
    grps = model_ranges.groupby(["cause_id", "sex_id"])
    for (cause_id, sex_id), subdf in grps:
        try:
            relevant_ages = cause_ranges.loc[cause_id].ages
        except KeyError:
            print((
                f"Skipping {cause_id} because it doesn't have "
                "cause_age_start/end")
            )
            continue
        present_ages_list = subdf.ages.tolist()
        present_ages = set().union(*present_ages_list)
        passes = present_ages.issuperset(relevant_ages)
        result_list.append((cause_id, sex_id, passes))

    result_df = pd.DataFrame.from_records(
        result_list, columns=["cause_id", "sex_id", "passes"]
    )
    missing_some_demographics = set(
        result_df[~result_df.passes].cause_id.unique().tolist()
    )
    shock_model_version_type_id = 5
    is_shock = set(
        df[df.model_version_type_id == shock_model_version_type_id]
        .cause_id.unique()
        .tolist()
    )
    failing_causes = missing_some_demographics.union(is_shock)

    return (
        result_df[~result_df.cause_id.isin(failing_causes)].cause_id.unique().astype(int).tolist()
    )


def main():
    df = read_file()
    causes = calc_causes(df)
    return causes


if __name__ == "__main__":
    causes = main()

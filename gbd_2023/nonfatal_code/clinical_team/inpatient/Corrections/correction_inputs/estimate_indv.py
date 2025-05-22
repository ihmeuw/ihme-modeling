"""
Estimate individuals in the non-Marketscan correction factor inputs.
This is a helper script that each CF input script will call to get
from admissions to individual cases.
"""

from functools import reduce
from typing import Union

import numpy as np
import pandas as pd
from crosscutting_functions.clinical_metadata_utils.values import Estimates, legacy_cf_estimate_types
from crosscutting_functions.deduplication.dedup import ClinicalDedup
from crosscutting_functions.mapping import clinical_mapping

from inpatient.CorrectionsFactors.correction_inputs import cf_input_constants


def clean_bad_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    If a patient ID includes age changes > 1 year or multiple sexes we can't correctly
    deduplicate it.

    Args:
        df: Formatted single year df by source.

    Returns:
        Cleaned up df without bad ids.
    """
    # drop missing patient IDs
    df = df[(~df["patient_id"].isin([".", ""])) & (df["patient_id"].notnull())]
    df = df[df["age"].notnull()]

    # drop duplicates by patient id and sex
    bad_pid = df[["patient_id", "sex_id"]].drop_duplicates()
    # keep only ids that are duplicated i.e. that have multiple sex ids for them
    bad_pid = bad_pid[bad_pid.duplicated(subset=["patient_id"])]
    # drop enrollee IDs with 2 sexes associated
    df = df[~df["patient_id"].isin(bad_pid["patient_id"])]
    print("there were {} bad patient IDs due to multiple sexes".format(bad_pid.shape[0]))
    del bad_pid

    dfG = df.copy()
    dfG["age_min"] = dfG["age"]
    dfG["age_max"] = dfG["age"]
    # create age min and max by enrollee ID groups
    dfG = dfG.groupby(["patient_id"]).agg({"age_min": "min", "age_max": "max"}).reset_index()
    min_age_df = dfG[["patient_id", "age_min"]].copy()
    # find the difference beween min and max
    dfG["age_diff"] = dfG["age_max"] - dfG["age_min"]
    # drop where difference is greater than 1 year
    dfG = dfG[dfG["age_diff"] > 1]
    print("there were {} bad patient IDs due to large age differences".format(dfG.shape[0]))

    df = df[~df["patient_id"].isin(dfG["patient_id"])]

    # The difference in patient ages is causing problems with the counts
    # ie someone is 49 on their first visit and 50 on the second, but they're
    # admitted twice in a year for the same bundle id
    # so take only the patient's youngest age on record.
    df = df.merge(min_age_df, how="left", on="patient_id")
    df["age_diff"] = df["age"] - df["age_min"]
    print("age diffs are", df["age_diff"].value_counts(dropna=False))
    assert df["age_diff"].max() <= 1
    df = df.drop(["age", "age_diff"], axis=1)
    df = df.rename(columns={"age_min": "age"})
    del dfG

    # remove null ages and sexes values
    df = df[(df["age"].notnull()) & (df["sex_id"].notnull())]

    # Validate then remove sexes that need to be sex split
    if 1 not in df["sex_id"].unique():
        raise ValueError("1 is not in sex_id")
    if 2 not in df["sex_id"].unique():
        raise ValueError("2 is not in sex_id")
    df = df.loc[df["sex_id"].isin([1, 2])]

    return df


def deduplicate_df(
    df: pd.DataFrame, estimate_id: int, map_version: Union[str, int]
) -> pd.DataFrame:

    dedup = ClinicalDedup(
        enrollee_col="patient_id",
        service_start_col="adm_date",
        year_col="year_id",
        estimate_id=estimate_id,
        map_version=map_version,
    )

    all_bundles = df["bundle_id"].unique()
    all_bundles.sort()
    df_all = []
    for b in all_bundles:
        print(f"Deduping for bundle {b}")
        df_dedup = dedup.main(df=df.loc[df["bundle_id"] == b], create_backup=False)
        cases_sum = df_dedup.shape[0]

        col_name = legacy_cf_estimate_types[estimate_id]
        df_dedup[col_name] = 1
        df_dedup = (
            df_dedup.groupby(cf_input_constants.GROUP_COLS)
            .agg({col_name: "sum"})
            .reset_index()
        )

        # check sum of cases to ensure we're not losing beyond what's expected
        if df_dedup[col_name].sum() != cases_sum:
            raise ValueError("Some cases lost")

        df_all.append(df_dedup)

    df_all = pd.concat(df_all, ignore_index=True)

    return df_all


def compile_all_ests(df: pd.DataFrame, map_version: int) -> pd.DataFrame:
    """Dedup based on the estimate id needed and preps for the output df.

    Args:
        df: Inpatient data to deduplicate and compile together by estimate.
        map_version: Standard clinical map version, used to determine measure and duration.

    Returns:
        The input df de-duplicated along multiple estimate IDs and compiled together.
    """
    df = df.astype({"bundle_id": np.integer})
    df["year_id"] = df["year_start"]

    # reassign patient id so that it's always int
    # cf inputs don't include patient id in the final output
    # so no need to map patient id back in this pipeline
    all_patient_ids = df["patient_id"].unique()
    patient_id_dict = dict(zip(all_patient_ids, range(len(all_patient_ids))))
    df["patient_id"] = df["patient_id"].map(patient_id_dict)

    df_14 = deduplicate_df(df, Estimates.claims_primary_inp_claims, map_version)
    df_15 = deduplicate_df(df, Estimates.claims_primary_inp_indv, map_version)
    df_16 = deduplicate_df(df, Estimates.claims_any_inp_claims, map_version)
    df_17 = deduplicate_df(df, Estimates.claims_any_inp_indv, map_version)
    del df

    dfs = [df_14, df_15, df_16, df_17]
    df_merged = reduce(
        lambda left, right: pd.merge(
            left, right, on=cf_input_constants.GROUP_COLS, how="outer"
        ),
        dfs,
    )

    # remove rows where every value is NA
    case_cols = df_merged.columns[df_merged.columns.str.endswith("_cases")]
    col_sums = df_merged[case_cols].sum()
    df_merged = df_merged.dropna(axis=0, how="all", subset=case_cols)
    if (col_sums != df_merged[case_cols].sum()).all():
        raise ValueError("TODO - This validation seems like it can never fail?")

    return df_merged


def main(df: pd.DataFrame, map_version: int, clinical_age_group_set_id: int) -> pd.DataFrame:
    """Run all the functions defined in this script to go from inpatient admissions
    to inpatient individuals

    Args:
        df: pre-formatted df for CF input.
        map_version: clinical map version.
        clinical_age_group_set_id: clinical_age_group_set_id may vary by year.

    Returns:
        Clinical inpatient data that has been deduplicated per the requirement of each
        each estimate ID and compiled.
    """
    df = clean_bad_ids(df)

    if df.shape[0] == 0:
        print("This df is empty after cleaning bad ids.")
    else:
        # map to bundle
        df = clinical_mapping.map_to_gbd_cause(
            df,
            input_type="cause_code",
            output_type="bundle",
            retain_active_bundles=True,
            write_unmapped=False,
            truncate_cause_codes=True,
            extract_pri_dx=True,
            prod=True,
            map_version=map_version,
            groupby_output=False,
        )

        # apply restriction
        df = clinical_mapping.apply_restrictions(
            df,
            age_set="indv",
            cause_type="bundle",
            clinical_age_group_set_id=clinical_age_group_set_id,
            map_version=map_version,
            break_if_not_contig=False,
        )

        df = compile_all_ests(df, map_version)

    return df

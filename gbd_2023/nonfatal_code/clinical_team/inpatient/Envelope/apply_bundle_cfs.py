"""
Apply various correction factor models to draw-level bundle data and write to the file system.
"""
import os
import warnings

import numpy as np
import pandas as pd
from crosscutting_functions.clinical_constants.values import Estimates
from crosscutting_functions.clinical_metadata_utils.pipeline_wrappers import (
    InpatientWrappers,
)
from crosscutting_functions import general_purpose, legacy_pipeline, pipeline
from crosscutting_functions.mapping import clinical_mapping, clinical_mapping_db
from db_tools.ezfuncs import query

from inpatient.Clinical_Runs.utils.constants import RunDBSettings
from inpatient.CorrectionsFactors.modeling import haqi_prep
from crosscutting_functions import uncertainty


def apply_CFs(
    estimate_df: pd.DataFrame, correction_df: pd.DataFrame, run_id: int
) -> pd.DataFrame:
    """
    ASSUME - DF and CF are both for the same age_group_id and sex_id
    ASSUME - Draws are not ordered in either DF or CF

    Params:
        estimate_df (pd.DataFrame):
            The input clinical data at the population representative level
        correction_df (pd.DataFrame):
            The CF draws by age and sex. Can only have these 2 demographic
            values or the algo will break
        run_id (int):
            clinical run id
    """
    final_dfs = []

    mean0_df = estimate_df.copy()  # to concat along with corrected data

    assert (
        estimate_df["age_group_id"].unique().size == 1
        and estimate_df["sex_id"].unique().size == 1
    ), "There are multiple ages or sexes which will mean the merge won't work correctly"
    assert (
        correction_df["age_group_id"].unique().size == 1
        and correction_df["sex_id"].unique().size == 1
    ), "There are multiple ages or sexes which will mean the merge won't work correctly"

    # Pull draw column names from CFs and data. They don't have to be named identically but
    # they must be the exact same count
    cf_draw_columns = correction_df.filter(regex="^draw").columns.tolist()
    estimate_draw_columns = estimate_df.filter(regex="^draw").columns.tolist()
    if len(cf_draw_columns) != len(estimate_draw_columns):
        raise RuntimeError(
            "The correction factors and the inpatient data must have the "
            "exact same count of draw columns present."
        )

    cf_keepers = cf_draw_columns + ["bundle_id", "target_estimate_id"]
    correction_df = correction_df[cf_keepers]

    outdir = (FILEPATH
    )
    a = int(mean0_df["age_group_id"].iloc[0])
    y = int(mean0_df["year_start"].iloc[0])
    s = int(mean0_df["sex_id"].iloc[0])
    assert mean0_df["year_start"].unique().size == 1, "Should be parallelized by year"

    group_cols = ["bundle_id"]
    for bundle_id, group_df in estimate_df.groupby(group_cols):

        # Set up our correction factors and get relavent info
        CF = correction_df.loc[correction_df.bundle_id == bundle_id, cf_draw_columns].values
        n_estimates = len(group_df)
        n_CFs = len(CF)

        # Take our estimates and
        # repeat them for each correction factor. For each correction factor tile them
        # for each estimate. Then multiply them.
        DRAWS = group_df[estimate_draw_columns].values
        corrected = np.repeat(DRAWS, n_CFs, axis=0) * np.tile(CF, (n_estimates, 1))

        estimate_cols = estimate_draw_columns

        # Construct the new DF -

        index = np.repeat(group_df.index.values, n_CFs)
        cf_applied_estimates_df = pd.DataFrame(corrected, columns=estimate_cols, index=index)

        final = pd.merge(
            cf_applied_estimates_df,
            group_df.drop(columns=estimate_cols),
            left_index=True,
            right_index=True,
        )
        target_estimate_ids = correction_df.loc[
            correction_df.bundle_id == bundle_id, "target_estimate_id"
        ].values
        final["target_estimate_id"] = np.tile(target_estimate_ids, n_estimates)

        final_estimates = final["target_estimate_id"].unique()
        if Estimates.inp_primary_unadj in final_estimates:
            raise ValueError(
                "There are still estimate 1 and 6 in the data post cf application."
            )

        # drop target_estimate_id after we overwrite estimate_id with new targets
        final["estimate_id"] = final["target_estimate_id"]
        final = final.drop(["target_estimate_id"], axis=1)

        # There may be unsquared data. It may be due
        # to a bug in this function that allows `final` to be smaller than `mean0_df`
        # ie data is lost when the CFs are missing a value
        if len(final) == 0:
            final.to_csv(FILEPATH,
                index=False,
            )
        else:
            for est in final["estimate_id"].unique():
                fest = final[final["estimate_id"] == est]
                if len(fest) != len(mean0_df[(mean0_df["bundle_id"] == bundle_id)]):
                    fest.to_csv(FILEPATH, index=False)

        final_dfs.append(final)

    concat_final = pd.concat([mean0_df] + final_dfs, sort=False).reset_index(drop=True)
    if concat_final["estimate_id"].isnull().sum() != 0:
        raise ValueError("There are rows with null estimate_id post cf appication")

    return concat_final


def write_draws(
    df: pd.DataFrame,
    run_id: int,
    age_group_id: int,
    sex_id: int,
    year: int,
    drop_draws_after_write: bool = True,
) -> pd.DataFrame:
    """
    Write the file with draws of all estimate types (where available) to the run
    """
    write_path = (FILEPATH.format(
            run=run_id, age=age_group_id, sex=sex_id, year=year
        )
    )
    general_purpose.write_hosp_file(df, write_path, backup=False)

    if drop_draws_after_write:
        draw_cols = df.filter(regex="^draw_").columns.tolist()
        df = df.drop(draw_cols, axis=1)

    return df


def merge_measure(df: pd.DataFrame, map_version: int) -> pd.DataFrame:
    """Merge bundle level measures onto the data."""

    bundle_measure_df = clinical_mapping_db.get_bundle_measure(map_version=map_version).rename(
        columns={"bundle_measure": "measure"}
    )
    df = df.merge(bundle_measure_df, how="left", on=["bundle_id"], validate="m:1")

    assert df.measure.isnull().sum() == 0, "There shouldn't be any null measures"

    df["measure"] = df["measure"].astype(str)
    return df


def align_uncertainty(df: pd.DataFrame) -> pd.DataFrame:
    """
    We're using different forms of uncertainty, depending on estimate type and
    data source so set the values we'd expect and test them
    """
    # When mean_raw is zero, then sample_size should not be null.
    assert (
        df.loc[df["mean"] == 0, "sample_size"].notnull().all()
    ), "Imputed zeros are missing sample_size in some rows."

    return df


def apply_haqi_corrections(df: pd.DataFrame, run_id: int) -> pd.DataFrame:
    """
    merge the haqi correction (averaged over 5 years) onto the hospital data
    """

    df = haqi_prep.merge_haqi(df, run_id)
    df = haqi_prep.apply_haqi(df)

    return df


def verify_cf_draws(df: pd.DataFrame, age_group_id: int, sex_id: int, run_id: int) -> None:
    """Verify a specific CF draws file that all appropriate
    bundle age group sex combos have target estimates.
    Throw warning if there are bundles with fewer than 3 target estimates.

    Args:
        df (pd.DataFrame): CF draws file read in
        age_group_id (int): GBD age_group_id of the given draws file
        sex_id (int): GBD sex_id of the given draws file
        run_id (int): run_id of this run
    """
    # check if all bundles have 3 target estimates, warning if there is
    cf_bun_est = pd.DataFrame(df.groupby(["bundle_id"]).size().reset_index()).rename(
        columns={0: "est_count"}
    )
    if not cf_bun_est.loc[cf_bun_est["est_count"] != 3].empty:
        problem_bundles = cf_bun_est.loc[cf_bun_est["est_count"] != 3]["bundle_id"].unique()
        warnings.warn(
            f"There are bundles with fewer than 3 target estimates: {problem_bundles}"
            "This may cause problems in squareness check. "
            "Confirm estimates in DB."
        )

    iw = InpatientWrappers(run_id, RunDBSettings.iw_profile)
    run_metadata = iw.pull_run_metadata()
    map_version = run_metadata["map_version"][0]
    # estimates 2,3,4,7,8,9
    cf_estimates = (
        Estimates.inp_primary_cf1_modeled,
        Estimates.inp_primary_cf2_modeled,
        Estimates.inp_primary_cf3_modeled,
        Estimates.inp_primary_live_births_cf1_modeled,
        Estimates.inp_primary_live_births_cf2_modeled,
        Estimates.inp_primary_live_births_cf3_modeled,
    )
    clinical_bundle = query(QUERY,
        conn_def="epi",
    )
    clinical_bundle["age_group_id"] = age_group_id
    clinical_bundle["sex_id"] = sex_id

    # grab clinical age group set and then apply restrictions
    p = pd.read_pickle(FILEPATH)
    proper_clinical_bundle = clinical_mapping.apply_restrictions(
        df=clinical_bundle,
        age_set="age_group_id",
        cause_type="bundle",
        clinical_age_group_set_id=p.clinical_age_group_set_id,
        map_version=map_version,
    )
    # keep relevant cf draws columns
    df = df[["bundle_id", "age_group_id", "sex_id", "target_estimate_id"]].drop_duplicates()

    merge_df = proper_clinical_bundle.merge(
        df, how="left", on=["bundle_id", "age_group_id", "sex_id"], validate="1:m"
    )

    if merge_df["target_estimate_id"].isnull().any():
        raise ValueError(
            f"Verify CF draws failed with age_group {age_group_id} and sex {sex_id}"
        )


def clean_after_write(df: pd.DataFrame) -> pd.DataFrame:
    """
    data structure and type is mostly there, make some little changes
    """
    int_cols = ["nid", "bundle_id"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], downcast="integer", errors="raise")

    # lose years
    df = df.drop(["years"], axis=1)

    # drop original pop col
    df = df.drop(["population"], axis=1)

    # drop the flag col
    if "zero_flag" in df.columns:
        df = df.drop(["zero_flag"], axis=1)

    return df


def replace_zeros_in_squared_df(df: pd.DataFrame) -> pd.DataFrame:
    """Replace all zeros in post-squared df by drawing from invECDF.
    Require population to be in df

    Args:
        df (pd.DataFrame): df prior to fill UI/mean/median
    """
    # use GBD pop as default sample size
    df["sample_size"] = df["population"]

    pre_draw = len(df)
    draw_cols = [col for col in df.columns if col.startswith("draw_")]

    # rows where all draws are 0
    df_zero = df.loc[df[draw_cols].sum(axis=1) == 0].reset_index(drop=True)
    df_nonzero = df.loc[df[draw_cols].sum(axis=1) != 0].reset_index(drop=True)

    # add a flag that identifies rows where all draws are 0
    # so we can force their mean and median back to 0 after add_ui()
    df_nonzero["zero_flag"] = 0
    df_zero["zero_flag"] = 1

    # with all draws being zero we can assume the mean is 0
    df_zero["mean"] = 0
    # replace all zeros among draw_cols with new draws
    # mean col will be dropped here
    df_zero = uncertainty.draws_from_invECDF(
        df=df_zero[list(set(df_zero.columns) - set(draw_cols))], draw_cols=draw_cols
    )

    if set(df_nonzero.columns) != set(df_zero.columns):
        raise RuntimeError("Column names don't match!")

    df = pd.concat([df_nonzero, df_zero], ignore_index=True)

    if len(df) != pre_draw:
        raise RuntimeError("Generating draws somehow added/removed rows")

    zeros = df.loc[df[draw_cols].sum(axis=1) == 0].reset_index(drop=True)
    if not zeros.empty:
        raise RuntimeError("There are still rows where draws are all zeros")

    return df


def replace_zero_row_lower_mean_median(df: pd.DataFrame) -> pd.DataFrame:
    """Replace the flagged rows' lower, mean and median with zero while
    keeping their non-zero upper. Require "zero_flag" column in the df

    Args:
        df (pd.DataFrame): df with UI/mean/median filled
    """
    df.loc[df["zero_flag"] == 1, "mean"] = 0
    df.loc[df["zero_flag"] == 1, "median_CI_team_only"] = 0
    df.loc[df["zero_flag"] == 1, "lower"] = 0

    return df


def apply_bundle_cfs_main(
    df: pd.DataFrame,
    age_group_id: int,
    sex_id: int,
    year: int,
    run_id: int,
    full_cover_stat: str, 
    bin_years: bool,
    draws: int,
) -> None:
    """The main function for the apply_bundle_cfs module.

    Args:
        df: Post-sqaured dataset with draws
        age_group_id: age_group_id that we are processing
        sex_id: sex_id that we are processing
        year: year_start that we are processing
        run_id: the clinical run_id
        full_cover_stat
        draws: The number of draws for the inp run
    """
    # pull run_metadata from ddl
    iw = InpatientWrappers(run_id, RunDBSettings.iw_profile)
    run_metadata = iw.pull_run_metadata()
    cf_version_set_id = run_metadata["cf_version_set_id"][0]
    map_version = run_metadata["map_version"][0]

    path = (FILEPATH
    )

    CF_path = os.path.expanduser(
        "{path}/{age}_{sex}.csv".format(path=path, age=age_group_id, sex=sex_id)
    )
    cf_df = pd.read_csv(CF_path)

    verify_cf_draws(cf_df.copy(), age_group_id, sex_id, run_id)

    cf_df = pipeline.downsample_draws(df=cf_df, draws=draws, draw_start=1)

    # resample the rows that are zero across the board post-squaring
    df = replace_zeros_in_squared_df(df)

    df = apply_CFs(df, cf_df, run_id)

    df = uncertainty.add_ui(df)

    # ensure cases == 0 for rows where draws were zero across the board originally
    df = replace_zero_row_lower_mean_median(df)

    df = write_draws(df, run_id=run_id, age_group_id=age_group_id, sex_id=sex_id, year=year)

    df = clean_after_write(df)

    # merge measures on using icg measures
    df = merge_measure(df, map_version=map_version)

    # set nulls
    df = align_uncertainty(df)

    # apply the 5 year inj corrections
    df = legacy_pipeline.apply_inj_corrections(
        df=df, run_id=run_id, bin_years=bin_years, iw_profile="clinical"
    )

    # apply e code proportion cutoff, removing rows under cutoff
    df = legacy_pipeline.remove_injuries_under_cutoff(
        df=df, run_id=run_id, bin_years=bin_years, iw_profile="clinical"
    )

    # final write to /share
    write_path = (FILEPATH.format(
            run=run_id, age=age_group_id, sex=sex_id, year=year
        )
    )
    df.to_csv(write_path, index=False)

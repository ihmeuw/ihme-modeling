"""
Apply final corrections to 5 year bundle data and write to drive
"""
import os
import sys
import warnings
import pandas as pd
import numpy as np
from collections import namedtuple
from getpass import getuser

from db_queries import get_population, get_covariate_estimates, get_cause_metadata

from clinical_info.Functions import hosp_prep
from clinical_info.Mapping import clinical_mapping
from clinical_info.Corrections import haqi_prep
from clinical_info.Database.bundle_relationships import relationship_methods


def apply_CFs(estimate_df, correction_df, run_id, full_cover_stat):
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
        full_cover_stat (str):
            aggregation statistic for the full coverage data, the envelope data
            will create upper/lower/mean/median but the full cover method is
            a bit more restrictive and can only do 1 or the other. Acceptable
            values are 'mean' or 'median'
    """
    final_dfs = []

    mean0_df = estimate_df.copy()  # to concat along with corrected data

    # just in case
    assert (
        estimate_df["age_group_id"].unique().size == 1
        and estimate_df["sex_id"].unique().size == 1
    ), "There are multiple ages or sexes which will mean the merge won't work correctly"
    assert (
        correction_df["age_group_id"].unique().size == 1
        and correction_df["sex_id"].unique().size == 1
    ), "There are multiple ages or sexes which will mean the merge won't work correctly"

    # The number of draws could differ between estimates and corrections, so use draws they have in common
    draw_columns = correction_df.filter(regex="^draw").columns.tolist()

    keepers = draw_columns + ["bundle_id", "cf_type"]
    correction_df = correction_df[keepers]

    outdir = f"FILEPATH"
    a = int(mean0_df["age_group_id"].iloc[0])
    y = int(mean0_df["year_start"].iloc[0])
    s = int(mean0_df["sex_id"].iloc[0])
    assert mean0_df["year_start"].unique().size == 1, "Should be parallelized by year"

    group_cols = ["bundle_id", "use_draws"]
    for names, group_df in estimate_df.groupby(group_cols):
        bundle_id, use_draws = names

        # Set up our correction factors and get relavent info
        CF = correction_df.loc[
            correction_df.bundle_id == bundle_id, draw_columns
        ].values
        n_estimates = len(group_df)
        n_CFs = len(CF)

        # The algorithm is the same whether we use 1000 draws or a single mean. Take our estimates and
        # repeat them for each correction factor. For each correction factor tile them
        # for each estimate. Then multiply them.
        # In the case of draws, we are multiplying two vectors element-wise to produce a new vector
        # In the case of a mean, we are multiplying a vector of means with a vector of averaged correction factors
        # Q: What is the difference between repeat and tile?
        # A:    tile(<a,b,c>, 2) = <a, a, b, b, c, c>
        #    repeate(<a,b,c>, 2) = <a, b, c, a, b, c> Notice that this gives us all combinations.
        if use_draws:
            DRAWS = group_df[draw_columns].values
            corrected = np.repeat(DRAWS, n_CFs, axis=0) * np.tile(CF, (n_estimates, 1))

            estimate_cols = draw_columns
        else:
            MEAN = group_df["mean"].values
            n_draws = CF.shape[1]
            if full_cover_stat == "mean":
                CF_MEAN = CF.sum(axis=1) / n_draws
                corrected = np.repeat(MEAN, n_CFs, axis=0) * np.tile(
                    CF_MEAN, n_estimates
                )
            elif full_cover_stat == "median":
                CF_MEDIAN = np.median(CF, axis=1)
                if np.isnan(CF_MEDIAN).all():
                    # print("Creating an empty array to mimic 'mean' behavior")
                    CF_MEDIAN = np.array([])
                corrected = np.repeat(MEAN, n_CFs, axis=0) * np.tile(
                    CF_MEDIAN, n_estimates
                )
            else:
                assert False, "this wont work"

            estimate_cols = ["mean"]

        # Construct the new DF
        index = np.repeat(group_df.index.values, n_CFs)
        cf_applied_estimates_df = pd.DataFrame(
            corrected, columns=estimate_cols, index=index
        )

        final = pd.merge(
            cf_applied_estimates_df,
            group_df.drop(columns=estimate_cols),
            left_index=True,
            right_index=True,
        )
        cf_types = correction_df.loc[
            correction_df.bundle_id == bundle_id, "cf_type"
        ].values
        final["cf_type"] = np.tile(cf_types, n_estimates)

        # fix the incorrect estimate_ids
        cf_dict = {
            "cf1_1": 2,
            "cf2_1": 3,
            "cf3_1": 4,
            "cf1_6": 7,
            "cf2_6": 8,
            "cf3_6": 9,
        }
        final["estimate_id"] = final["cf_type"] + "_" + final["estimate_id"].astype(str)
        final["estimate_id"].replace(cf_dict, inplace=True)
        final["estimate_id"] = pd.to_numeric(
            final["estimate_id"], errors="raise", downcast="integer"
        )
        final_e = final["estimate_id"].unique()
        assert 1 not in final_e and 6 not in final_e

        if len(final) == 0:
            final.to_csv(
                f"FILEPATH", index=False,
            )
        else:
            for est in final["estimate_id"].unique():
                fest = final[final["estimate_id"] == est]
                if len(fest) != len(
                    mean0_df[
                        (mean0_df["bundle_id"] == bundle_id)
                        & (mean0_df["use_draws"] == use_draws)
                    ]
                ):
                    fest.to_csv(f"FILEPATH", index=False)

        final_dfs.append(final)

    return pd.concat([mean0_df] + final_dfs, sort=False).reset_index(drop=True)


def add_UI(df):
    df["use_draws"] = df["use_draws"].astype(bool)

    ui_cols = ["upper", "lower", "median_CI_team_only"]
    for ui in ui_cols:
        if ui not in df.columns:
            df[ui] = np.nan

    if df["use_draws"].sum() == 0:
        print("There is no draw data, returning df object")
        return df

    draw_columns = df.filter(regex="^draw").columns.tolist()
    draws_df = df.loc[df["use_draws"], draw_columns].values

    df.loc[df["use_draws"], "mean"] = df.loc[df["use_draws"], draw_columns].mean(axis=1)

    df["median_CI_team_only"] = np.nan
    df.loc[df["use_draws"], "median_CI_team_only"] = df.loc[
        df["use_draws"], draw_columns
    ].median(axis=1)

    df.loc[df["use_draws"], "lower"] = np.percentile(draws_df, 2.5, axis=1)
    df.loc[df["use_draws"], "upper"] = np.percentile(draws_df, 97.5, axis=1)

    return df


def write_draws(df, run_id, age_group_id, sex_id, year, drop_draws_after_write=True):
    """
    Write the file with draws of all estimate types (where available) to the run
    """
    write_path = "FILEPATH"
    hosp_prep.write_hosp_file(df, write_path, backup=False)

    if drop_draws_after_write:
        draw_cols = df.filter(regex="^draw_").columns.tolist()
        df.drop(draw_cols, axis=1, inplace=True)

    return df


def merge_measure(df, parent_inj, drop_mult_measures=True):
    cm = clinical_mapping.get_clinical_process_data("cause_code_icg", prod=True)
    cm = cm[["icg_id", "icg_measure"]].drop_duplicates()

    bun = clinical_mapping.get_clinical_process_data("icg_bundle", prod=True)
    measures = bun.merge(cm, how="left", on="icg_id")
    measures = measures[["bundle_id", "icg_measure"]].drop_duplicates()
    if drop_mult_measures:
        doubles = measures.bundle_id[measures.bundle_id.duplicated(keep=False)].unique()
        warnings.warn(
            "We will be dropping distinct measures for bundle ids {}".format(doubles)
        )
        measures.drop_duplicates(subset=["bundle_id"], inplace=True)
    measures.rename(columns={"icg_measure": "measure"}, inplace=True)

    pre = df.shape[0]
    df = df.merge(measures, how="left", on="bundle_id")
    assert pre == df.shape[0], "Row counts have changed, not good"

    # parent_inj = [264, 269, 270, 272, 275, 276]
    parent_inj = parent_inj["output_bundle_id"].unique().tolist()

    df.loc[df.bundle_id.isin(parent_inj), "measure"] = "inc"
    assert df.measure.isnull().sum() == 0, "There shouldn't be any null measures"

    df["measure"] = df["measure"].astype(str)
    return df


def align_uncertainty(df):
    """
    We're using different forms of uncertainty, depending on estimate type and
    data source so set the values we'd expect and test them
    """
    # When mean_raw is zero, then sample_size should not be null.
    assert (
        df.loc[df["mean"] == 0, "sample_size"].notnull().all()
    ), "Imputed zeros are missing sample_size in some rows."

    # Make upper and lower Null when sample size is not null
    est_types = [2, 3, 4]

    df.loc[
        (df["sample_size"].notnull()) & (df["estimate_id"].isin(est_types)),
        ["upper", "lower"],
    ] = np.nan

    return df


def apply_haqi_corrections(df, run_id):
    """
    merge the haqi correction (averaged over 5 years) onto the hospital data
    """

    df = haqi_prep.merge_haqi(df, run_id)
    df = haqi_prep.apply_haqi(df)

    return df


def clean_after_write(df):
    int_cols = ["nid", "bundle_id"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], downcast="integer", errors="raise")

    df.drop(["cf_type", "years"], axis=1, inplace=True)

    return df


def identify_correct_sample_size(df, full_coverage_sources):
    """
    We have 3 distinct types of sample sizes currently
    population: gbd population for UTLA data
    maternal_sample_size: ifd * asfr * gbd population for maternal causes
    sample_size: inpatient admissions for rows where mean == 0 NOTE: this is
        calculated from our cause fractions- so these values are only available
        currently for sources which use the envelope (ie not UTLA)
    
    THE ORDER IS VERY IMPORTANT
    UTLA pop is used, except for maternal bundles then live births are used
    inp admits are used for mean0==0, except for maternal bundles
    If the order is changed then sample_size values will change
    """
    df.rename(columns={"sample_size": "inp_admits"}, inplace=True)

    # gen new ss column
    df["sample_size"] = np.nan

    # use gbd pop for full coverage
    df.loc[df["source"].isin(full_coverage_sources), "sample_size"] = df.loc[
        df["source"].isin(full_coverage_sources), "population"
    ]

    # use inp admits for mean == 0
    df.loc[df["mean"] == 0, "sample_size"] = df.loc[df["mean"] == 0, "inp_admits"]

    # use maternal for maternal estimate
    df.loc[df["estimate_id"].isin([6, 7, 8, 9]), "sample_size"] = df.loc[
        df["estimate_id"].isin([6, 7, 8, 9]), "maternal_sample_size"
    ]

    # drop original sample cols
    df.drop(["population", "maternal_sample_size", "inp_admits"], axis=1, inplace=True)

    return df


def apply_bundle_cfs_main(
    df,
    age_group_id,
    sex_id,
    year,
    run_id,
    gbd_round_id,
    decomp_step,
    full_coverage_sources,
    full_cover_stat,
):
    """
    Put everything above together into 1 function that will be called into the other bundle uncertainty script
    """
    CF_path = os.path.expanduser("FILEPATH")
    cf_df = pd.read_csv(CF_path)

    # run data through the pipe
    # df = estimate_df.copy()
    parent_method = relationship_methods.ParentInjuries(run_id=run_id)
    df = parent_method.append_parent_bundle(df)

    df["use_draws"] = df["use_draws"].astype(bool)
    df = apply_CFs(df, cf_df, run_id, full_cover_stat)

    df = add_UI(df)

    # can't sum UI vals, gotta sum draws
    draw_cols = pd.Series(df.filter(regex="^draw_").columns.tolist())
    if draw_cols.tolist():
        for col in draw_cols.sample(7):
            hosp_prep.check_parent_injuries(
                df[df["use_draws"]], col_to_sum=col, run_id=run_id, verbose=True
            )

    df = write_draws(
        df, run_id=run_id, age_group_id=age_group_id, sex_id=sex_id, year=year
    )

    df = clean_after_write(df)

    # unify sample size
    df = identify_correct_sample_size(df, full_coverage_sources)

    # merge measures on using icg measures
    df = merge_measure(df, parent_method.bundle_relationship)

    # set nulls
    df = align_uncertainty(df)

    # apply the 5 year inj corrections
    df = hosp_prep.apply_inj_corrections(df, run_id)

    # apply e code proportion cutoff, removing rows under cutoff
    df = hosp_prep.remove_injuries_under_cutoff(df, run_id)

    # append and apply the haqi correction
    df = apply_haqi_corrections(df, run_id)

    # final write to FILEPATH
    write_path = "FILEPATH"
    df.to_csv(write_path, index=False)

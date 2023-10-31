"""
A set of functions that prepare inpatient hospital data for the correction
factor/envelope uncertainty data
There are 5 rough steps
1) Drop data
2) Split into covered and uncovered dataframes
    2.1) create "mean" from the covered data
3) Create cause fractions for the uncovered data
4) Apply age and sex restrictions
5) Split the envelope draws into smaller files to increase runtime
    Note: only needed when a new env is created OR new hospital
    data locations are included

Note: output is 2 dataframes, one with covered sources, one with uncovered
(ie sources that use the envelope) sources
"""

import platform
import sys
import getpass
import warnings
import pandas as pd
import numpy as np
import ipdb

# load our functions
from clinical_info.Mapping import clinical_mapping as cm
from clinical_info.Functions import hosp_prep, gbd_hosp_prep
from clinical_info.Envelope import redistribute_poisoning_drug as rpd


def split_sources(df, gbd_round_id, decomp_step):
    """
    NOTE USER wanted us to stop using the envelope for the UTLA data.
    The assumption here is that the population is fully covered, so we don't
    need to use the envelope

    This will be passed to PopEstimates to create population level rates
    """
    # Make list of sources that we don't want to use envelope.  This should be
    # the source in the 'source' column of the dataframe
    full_coverage_sources = hosp_prep.full_coverage_sources()

    if pd.Series(full_coverage_sources).isin(df.source.unique()).any():
        # make dataframe with sources
        full_coverage_df = df[df.source.isin(full_coverage_sources)].copy()

    else:
        # make empty dataframe so that it exists and wont break anything
        full_coverage_df = pd.DataFrame(columns=list(df.columns))

    # drop these sources from dataframe for now
    df = df[~df.source.isin(full_coverage_sources)].copy()

    return df, full_coverage_df


def apply_restricts(df, full_coverage_df, clinical_age_group_set_id):

    df = cm.apply_restrictions(
        df,
        age_set="age_group_id",
        cause_type="icg",
        clinical_age_group_set_id=clinical_age_group_set_id,
    )
    full_coverage_df = cm.apply_restrictions(
        full_coverage_df,
        age_set="age_group_id",
        cause_type="icg",
        clinical_age_group_set_id=clinical_age_group_set_id,
    )

    # drop the restricted values
    df = df[df["cause_fraction"].notnull()]

    full_coverage_df = full_coverage_df[full_coverage_df["val"].notnull()]

    return df, full_coverage_df


def write_env_demo(df, run_id):
    path = "FILEPATH"
    cols = ["location_id", "age_group_id", "year_id", "sex_id"]
    df[cols].drop_duplicates().to_csv(path + "hosp_demographics.csv", index=False)


def prep_for_env_main(
    df,
    env_path,
    run_id,
    gbd_round_id,
    decomp_step,
    clinical_age_group_set_id,
    new_env_or_data=True,
    write=True,
    create_hosp_denom=True,
    drop_data=True,
    fix_norway_subnat=False,
):
    """
    run everything, returns 2 DataFrames
    """
    # data is already dropped when doing age_sex splitting so this should not usually be needed after that
    if drop_data:
        df = hosp_prep.drop_data(df, verbose=False)

    # redistribute the poisoning drug baby seq
    df = rpd.redistribute_poison_drug(
        df=df, clinical_age_group_set_id=clinical_age_group_set_id
    )

    if not df.age_group_id.notnull().all():
        warnings.warn(
            """Shouldn't be any null age_group_id, there could be
        unsplit ages in the data, or maybe an age_start/age_end changed"""
        )

    print("Splitting covered and uncovered sources...")
    df, full_coverage_df = split_sources(
        df, gbd_round_id=gbd_round_id, decomp_step=decomp_step
    )

    # df.rename(columns={'year_id': 'year_start'}, inplace=True)
    print("Creating Cause Fractions...")

    df = hosp_prep.create_cause_fraction(
        df,
        run_id=run_id,
        write_cause_fraction=True,
        create_hosp_denom=create_hosp_denom,
        store_diagnostics=True,
        tol=1e-6,
    )

    print("Applying Restrictions...")
    df, full_coverage_df = apply_restricts(
        df, full_coverage_df, clinical_age_group_set_id=clinical_age_group_set_id
    )

    # write demographic info for the envelope process
    write_env_demo(df, run_id)

    if write:
        base = "FILEPATH"

        print("Writing the df file...")
        file_path = "FILEPATH"
        hosp_prep.write_hosp_file(df, file_path, backup=False)

        print("Writing the full_coverage_df file...")
        file_path_cover = "FILEPATH"
        hosp_prep.write_hosp_file(full_coverage_df, file_path_cover, backup=False)

    print("df columns are {}".format(df.columns))
    print("full cover df columns are {}".format(full_coverage_df.columns))
    return df, full_coverage_df

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

import warnings

import pandas as pd
from crosscutting_functions import general_purpose, legacy_pipeline
from crosscutting_functions.mapping import clinical_mapping


def split_sources(df):
    """

    This will be passed to PopEstimates to create population level rates
    """
    # Make list of sources that we don't want to use envelope.  This should be
    # the source in the 'source' column of the dataframe
    full_coverage_sources = legacy_pipeline.full_coverage_sources()

    if pd.Series(full_coverage_sources).isin(df.source.unique()).any():
        # make dataframe with sources
        full_coverage_df = df[df.source.isin(full_coverage_sources)].copy()

    else:
        # make empty dataframe so that it exists and wont break anything
        full_coverage_df = pd.DataFrame(columns=list(df.columns))

    # drop these sources from dataframe for now
    df = df[~df.source.isin(full_coverage_sources)].copy()

    return df, full_coverage_df


def apply_restricts(df, clinical_age_group_set_id, map_version, uses_envelope: bool):
    """

    """

    df = clinical_mapping.apply_restrictions(
        df,
        age_set="age_group_id",
        cause_type="icg",
        clinical_age_group_set_id=clinical_age_group_set_id,
        map_version=map_version,
    )

    # drop the restricted values
    if uses_envelope:
        df = df[df["cause_fraction"].notnull()]
    else:
        df = df[df["val"].notnull()]

    return df


def write_env_demo(df, run_id):
    path = FILEPATH
    cols = ["location_id", "age_group_id", "year_id", "sex_id"]
    df[cols].drop_duplicates().to_csv(FILEPATH, index=False)


def prep_for_env_main(
    df,
    env_path,
    run_id,
    clinical_age_group_set_id,
    map_version,
    new_env_or_data=True,
    write=True,
    drop_data=True,
    fix_norway_subnat=False,
):
    """
    run everything, returns 2 DataFrames
    """
    if drop_data:
        from clinical_info.Clinical_Runs.utils.constants import InpRunSettings

        df = legacy_pipeline.drop_data(
            df, verbose=False, gbd_start_year=InpRunSettings.GBD_START_YEAR
        )

    if not df.age_group_id.notnull().all():
        warnings.warn(
            """Shouldn't be any null age_group_id, there could be
        unsplit ages in the data, or maybe an age_start/age_end changed"""
        )

    print("Splitting covered and uncovered sources...")
    df, full_coverage_df = split_sources(df)

    if not df.empty:  # process envelope data if it's present
        print("Creating Cause Fractions...")
        df = legacy_pipeline.create_cause_fraction(
            df,
            run_id=run_id,
            write_cause_fraction=True,
            tol=1e-6,
        )
        df = apply_restricts(
            df,
            clinical_age_group_set_id=clinical_age_group_set_id,
            map_version=map_version,
            uses_envelope=True,
        )

        # write demographic info for the envelope process
        write_env_demo(df, run_id)

    if not full_coverage_df.empty:  # process full coverage (gbd pop) data if present
        full_coverage_df = apply_restricts(
            full_coverage_df,
            clinical_age_group_set_id=clinical_age_group_set_id,
            map_version=map_version,
            uses_envelope=False,
        )

    if df.empty and full_coverage_df.empty:
        raise ValueError("Both envelope and full coverage data are empty something is wrong.")

    if write:
        base = FILEPATH.format(run_id)

        if not df.empty:
            print("Writing the envelope df file...")
            file_path = FILEPATH.format(base)
            general_purpose.write_hosp_file(df, file_path, backup=False)

        if not full_coverage_df.empty:
            print("Writing the full_coverage_df file...")
            file_path_cover = FILEPATH.format(base)
            general_purpose.write_hosp_file(full_coverage_df, file_path_cover, backup=False)

    print("df columns are {}".format(df.columns))
    print("full cover df columns are {}".format(full_coverage_df.columns))
    return df, full_coverage_df

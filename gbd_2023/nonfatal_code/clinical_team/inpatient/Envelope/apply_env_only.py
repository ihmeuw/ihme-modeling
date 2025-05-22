"""
Function that creates cause fractions and applies just the envelope NOT the envelope * CF uncertainty
"""
import warnings

import pandas as pd
from crosscutting_functions import legacy_pipeline

# load our functions
from crosscutting_functions.mapping import clinical_mapping

from inpatient.AgeSexSplitting import under1_adjustment


def apply_envelope_only(
    df,
    env_path,
    run_id,
    clinical_age_group_set_id,
    map_version,
    return_only_inj=False,
    apply_age_sex_restrictions=True,
    want_to_drop_data=True,
    apply_env_subroutine=False,
    use_cached_pop=False,
):
    """
    Function that converts hospital data into rates.  Takes hospital data in
    count space, at the baby Sequelae level, computes cause fractions, attaches
    the hospital utilization envelope, and multiplies the envelope and the cause
    fractions.  Data that represents fully covered populations are not made into
    cause fractions, nor is the envelope applied.  Instead, their counts are
    divided by population. Returns a DataFrame that is in Rate spaces at the
    Baby Sequelae level.

    Arguments:
        df (DataFrame):
            contains hospital inpatient primary diagnosis that
            has been mapped to Baby Sequelae
        env_path (str):
            filepath to the envelope you want to apply
        run_id (int):
            Identifies which clinical inpatient run we're using
        use_cached_pop (bool):
            How does this interact with gbd round and sample size in get_ss?
            If it's true are the former 2 ignored? I think so
        clinical_age_group_set_id (int):
            Our 'good' age groups, the ages we use in final data have changed
            so this arg tells the function which population to pull and which
            set of age restrictions to use
        return_only_inj (bool):
            switch that will make it so this function only returns injuries
            data. should always be off, it's just there for
            testing/convienience.
        apply_age_sex_restrictions (bool): switch that if True, will apply
            age and sex restrictions as determined by cause_set_id=9.  Defaults
            to true.  Useful if you want to look at data all the data that was
            present in the source.
        want_to_drop_data (bool): If True will run drop_data(verbose=False)
    """
    ###############################################
    # DROP DATA
    ###############################################

    if want_to_drop_data:
        from inpatient.Clinical_Runs.utils.constants import InpRunSettings

        df = legacy_pipeline.drop_data(
            df, verbose=False, gbd_start_year=InpRunSettings.GBD_START_YEAR
        )

    ###############################################
    # MAP AGE_GROUP_ID ONTO DATA DEPRECATED 7/13/2017
    # WE NOW ONLY USE AGE GROUP ID
    ###############################################

    if not df.age_group_id.notnull().all():
        warnings.warn(
            """Shouldn't be any null age_group_id, there could be
        unsplit ages in the data, or maybe an age_start/age_end changed"""
        )

    #########################################################
    # Get Denominator aka sample_size for sources where we don't want to use
    # envelope
    #########################################################

    # Make list of sources that we don't want to use envelope.  This should be
    # the source in the 'source' column of the dataframe
    full_coverage_sources = legacy_pipeline.full_coverage_sources()

    if pd.Series(full_coverage_sources).isin(df.source.unique()).any():
        # make dataframe with sources
        full_coverage_df = df[df.source.isin(full_coverage_sources)].copy()

        # get denominator (aka poplation)
        full_coverage_df = legacy_pipeline.get_sample_size(
            full_coverage_df,
            use_cached_pop=use_cached_pop,
            run_id=run_id,
            clinical_age_group_set_id=clinical_age_group_set_id,
        )

        full_coverage_df.rename(columns={"population": "sample_size"}, inplace=True)


        # make product
        full_coverage_df["product"] = full_coverage_df.val / full_coverage_df.sample_size

        # adjust the under 1 counts here. If the clinical age set does not
        # contain more than 1 U1 group then it won't adjust
        full_coverage_df = under1_adjustment.adjust_under1_data(
            full_coverage_df,
            col_to_adjust="product",
            conversion="count_to_rate",
            clinical_age_group_set_id=clinical_age_group_set_id,
        )

    else:
        # make empty dataframe so that it exists and wont break anything
        full_coverage_df = pd.DataFrame(columns=list(df.columns) + ["product"])

    # drop these sources from dataframe for now
    df = df[~df.source.isin(full_coverage_sources)].copy()

    #########################################
    # CREATE CAUSE FRACTIONS
    #########################################
    if not apply_env_subroutine:
        df = legacy_pipeline.create_cause_fraction(df, run_id=run_id, tol=1e-6)

    ###############################################
    # APPLY CAUSE RESTRICTIONS
    ###############################################

    if apply_age_sex_restrictions:
        df = clinical_mapping.apply_restrictions(
            df,
            age_set="age_group_id",
            cause_type="icg",
            prod=False,
            clinical_age_group_set_id=clinical_age_group_set_id,
            map_version=map_version,
        )

        if full_coverage_df.shape[0] > 0:
            full_coverage_df = clinical_mapping.apply_restrictions(
                full_coverage_df,
                age_set="age_group_id",
                cause_type="icg",
                prod=False,
                clinical_age_group_set_id=clinical_age_group_set_id,
                map_version=map_version,
            )
        # drop the restricted values
        df = df[df["cause_fraction"].notnull()]
        full_coverage_df = full_coverage_df[full_coverage_df["product"].notnull()]

    #########################################
    # APPLY ENVELOPE
    #########################################
    # read envelope
    if env_path[-3:] == "csv":
        env_df = pd.read_csv(env_path)
    else:
        env_df = pd.read_hdf(env_path, key="df")
    # Row identifying columns in the envelope
    demography = ["location_id", "year_start", "year_end", "age_group_id", "sex_id"]

    ############################################
    # MERGE ENVELOPE onto data
    ############################################
    pre_shape = df.shape[0]
    df = df.merge(env_df, how="left", on=demography, validate="m:1")
    assert pre_shape == df.shape[0], "The merge duplicated rows unexpectedly"

    # "apply the envelope"
    df["product"] = df["cause_fraction"] * df["mean"]
    df["upper_product"] = df["cause_fraction"] * df["upper"]
    df["lower_product"] = df["cause_fraction"] * df["lower"]

    # catch rows where the env couldn't be applied
    null_means = len(df.loc[df["mean"].isnull()])
    if null_means > 0:
        print(
            f"There are {null_means} rows where 'product' is null, "
            "probably related to an age_group, sex, location, or "
            "year not present in the envelope"
        )
    ############################################
    # RE-ATTACH data that has sample size
    ############################################

    if full_coverage_df.shape[0] > 0:
        df = pd.concat([df, full_coverage_df], sort=False).reset_index(drop=True)
        # NOTE now there's going be a column "sample_size" and "cases" that
        # is null for every source except the fully covered ones
    else:
        print(
            "Note: The Full coverage df did not have any observations, we will not "
            "concat the data together"
        )

    # Drop columns we don't need anymore:
    #   cause fraction: finished using it, was used to make product
    #   mean: finished using it, was used to make product
    #   upper: finished using, it was used ot make product_upper
    #   lower: finished using, it was used ot make product_lower
    #   val, numerator, denomantor: no longer in count space
    #
    #   NON FATAL_CAUSE_NAME IS LEFT TO SIGNIFY THAT THIS DATA IS STILL AT THE
    #   ICG_ID LEVEL
    if apply_env_subroutine:
        df.drop(["cause_fraction", "mean", "upper", "lower"], axis=1, inplace=True)
        df.rename(
            columns={
                "product": "mean",
                "lower_product": "lower",
                "upper_product": "upper",
            },
            inplace=True,
        )
    else:
        df.drop(
            [
                "cause_fraction",
                "mean",
                "upper",
                "lower",
                "val",
                "numerator",
                "denominator",
            ],
            axis=1,
            inplace=True,
        )
    return df

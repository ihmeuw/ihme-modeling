"""
Set of functions to create or adjust the maternal denominators
"""

import datetime
import getpass
import warnings
import pandas as pd
import numpy as np
import os
import time
import functools
import glob
from db_tools.ezfuncs import query
from db_queries import get_population, get_cause_metadata, get_location_metadata

# load our functions
from clinical_info.Functions import hosp_prep, gbd_hosp_prep, correction_factor_utils
from clinical_info.Mapping import clinical_mapping
from clinical_info.Functions import cached_pop_tools


# functions for IFD ASFR denom


def run_shared_funcs(gbd_round_id, decomp_step, clinical_age_group_set_id, run_id):
    """
    get all the central inputs we'll need. Population and asfr and ifd covariates
    """
    warnings.warn("\nYears are hard coded to start at 1988 and end at 2023\n")
    years = list(np.arange(1988, 2023, 1))

    locs = (
        get_location_metadata(location_set_id=35, gbd_round_id=gbd_round_id)[
            "location_id"
        ]
        .unique()
        .tolist()
    )
    ages = (
        hosp_prep.get_hospital_age_groups(
            clinical_age_group_set_id=clinical_age_group_set_id
        )["age_group_id"]
        .unique()
        .tolist()
    )

    # get pop
    pop = cached_pop_tools.get_cached_pop(run_id, drop_duplicates=True)
    pop = pop[(pop["sex_id"] == 2)]

    # GET ASFR and IFD
    # has age/location/year
    warnings.warn(
        "We're pulling shared data with gbd round id {} and decomp step {}".format(
            gbd_round_id, decomp_step
        )
    )

    asfr = gbd_hosp_prep.get_covar_for_clinical(
        covariate_name_short="ASFR",
        run_id=run_id,
        location_id=locs,
        age_group_id=ages,
        year_id=years,
    )
    ifd = gbd_hosp_prep.get_covar_for_clinical(
        covariate_name_short="IFD_coverage_prop", run_id=run_id
    )

    asfr = asfr[
        (asfr.location_id.isin(locs))
        & (asfr.age_group_id.isin(ages))
        & (asfr.year_id.isin(years))
    ]
    ifd = ifd[(ifd.location_id.isin(locs))]

    assert (
        ifd.shape[0] > 0 and asfr.shape[0] > 0 and pop.shape[0] > 0
    ), "One of these three is empty"

    return pop, asfr, ifd


def merge_pop_asfr_ifd(pop, asfr, ifd):
    """
    central inputs will be multiplied together to get ifd*asfr*population
    to do this we first need to merge the data together into the same df
    """
    # first merge asfr and ifd, NOTE ifd has no age related information
    new_denom = asfr.merge(ifd, how="outer", on=["location_id", "sex_id", "year_id"])
    # drop data before 1987, our earliest hosp year is 1988
    new_denom = new_denom[new_denom.year_id > 1987]
    # keep only female data
    new_denom = new_denom[new_denom.sex_id == 2]
    print(ifd.shape, asfr.shape, new_denom.shape)

    pai = pop.merge(
        new_denom, how="outer", on=["location_id", "sex_id", "age_group_id", "year_id"]
    )
    print(new_denom.shape, pai.shape)

    # create the new denominator
    pai["ifd_asfr_denom"] = pai["population"] * pai["asfr_mean"] * pai["ifd_mean"]

    return pai


def clean_shared_output(pop, asfr, ifd):
    """
    prep the central inputs to work with our process
    """
    pop.drop("pop_run_id", axis=1, inplace=True)
    # pop.rename(columns={'year_id': 'year_start'}, inplace=True)

    keeps = ["location_id", "sex_id", "age_group_id", "year_id", "mean_value"]
    asfr = asfr[keeps].copy()
    asfr.rename(columns={"mean_value": "asfr_mean"}, inplace=True)

    ifd = ifd[keeps].copy()
    ifd.rename(columns={"mean_value": "ifd_mean"}, inplace=True)
    # IFD doesn't return data with age or sex specified
    ifd.drop("age_group_id", axis=1, inplace=True)
    ifd["sex_id"] = 2

    return pop, asfr, ifd


def create_maternal_rate(mat_res):
    """
    create the unadjusted maternal rate
    """
    # create maternal denom in rate space
    mat_res["ifd_asfr_denom_rate"] = mat_res["ifd_asfr_denom"] / mat_res["population"]
    mat_res.drop(["population"], axis=1, inplace=True)

    return mat_res


def write_maternal_denom(
    denom_type, run_id, gbd_round_id, decomp_step, clinical_age_group_set_id
):
    """
    Write the maternal denominator data

    Parameters:
        denom_type: 'ifd_asfr' or 'bundle1010', but currently bundle1010 is broken
        run_id: clinical runs!
        gbd_round_id: CentralComp standardized to signify GBD year
        decomp_step: same as gbd_round_id
        clinical_age_group_set_id: internal clinical age group sets to identify which age groups
                                   to pull from shared/central tools
    """

    print("Creating the maternal denominator file in /../other/inp/denom/")
    # define good maternal ages
    good_age_group_ids = [7, 8, 9, 10, 11, 12, 13, 14, 15]

    if denom_type == "ifd_asfr":
        # get the three estimates from central Dbs
        pop, asfr, ifd = run_shared_funcs(
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step,
            run_id=run_id,
            clinical_age_group_set_id=clinical_age_group_set_id,
        )
        pop, asfr, ifd = clean_shared_output(pop, asfr, ifd)

        # multiply them together to get our new denom which is IFD * ASFR * POP
        mat_df = merge_pop_asfr_ifd(pop, asfr, ifd)
        # keep only good ages
        warnings.warn(
            f"\nMat denoms are being created with age groups {good_age_group_ids}"
        )
        mat_df = mat_df[mat_df.age_group_id.isin(good_age_group_ids)]

        mat_df = create_maternal_rate(mat_df)
        mat_df.rename(
            columns={
                "ifd_asfr_denom_rate": "mean_raw",
                "ifd_asfr_denom": "sample_size",
            },
            inplace=True,
        )

        # can't use null data and there seem to be regional null ifd/asfr vals
        prerows = len(mat_df)
        mat_df = mat_df[mat_df["sample_size"].notnull()]
        diff = prerows - len(mat_df)
        print(
            f"{diff} rows lost due to null sample sizes. This probably isn't an issue"
        )

        mat_df.columns = mat_df.columns.astype(str)
        mat_df.to_hdf(
            "FILEPATH".format(run_id), key="df", mode="w",
        )

    if denom_type == "bundle1010":
        assert (
            False
        ), "This needs to be rebuilt to run in individual years before we can run it"

        # select just the total_maternal bundle
        mat_df = df[df.bundle_id == 1010].copy()

        # keep only allowed ages and sexes
        mat_df = mat_df[mat_df.age_group_id.isin(good_age_group_ids)].copy()
        mat_df = mat_df[mat_df.sex_id == 2].copy()

        if mat_df.shape[0] == 0:
            return

        # NOTE sample size is dropped here, and we make a new one in the
        # following code
        mat_df = mat_df[
            [
                "location_id",
                "year_start",
                "year_end",
                "age_group_id",
                "sex_id",
                "mean_raw",
                "mean_incidence",
                "mean_prevalence",
                "mean_indvcf",
            ]
        ].copy()

        bounds = mat_df.filter(regex="^upper|^lower").columns
        for uncertainty in bounds:
            mat_df[uncertainty] = np.nan

        # PREP FOR POP
        # we don't have years that we can merge on pop to yet, because
        # we aggregated to year bands
        mat_df["year_id"] = mat_df.year_start + 2  # makes 2000,2005,2010

        # create age/year/location lists to use for pulling population
        age_list = list(mat_df.age_group_id.unique())
        loc_list = list(mat_df.location_id.unique())
        year_list = list(mat_df.year_id.unique())

        pop = cached_pop_tools.get_cached_pop(run_id, drop_duplicates=True)

        pop.drop(["pop_run_id"], axis=1, inplace=True)

        demography = ["location_id", "year_id", "age_group_id", "sex_id"]

        pre_shape = mat_df.shape[0]  # store for before comparison
        # then merge population onto the hospital data

        # attach pop info to df
        mat_df = mat_df.merge(pop, how="left", on=demography)
        assert pre_shape == mat_df.shape[0], "number of rows don't " "match after merge"

        mat_df["sample_size"] = mat_df.population * mat_df.mean_raw

        # DROP intermediate columns
        mat_df.drop(["population", "year_id"], axis=1, inplace=True)

        # print("before writing denoms", mat_df.info())
        mat_df.sex_id = mat_df.sex_id.astype(int)
        mat_df.location_id = mat_df.location_id.astype(int)

        mat_df.to_hdf(
            "FILEPATH".format(run_id), key="df", mode="w",
        )


def select_maternal_data(df, gbd_round_id):
    """
    Function that filters out non maternal data. Meant to be ran at the start
    of this process.  If we are only interested in adjusting the maternal denom,
    then we don't need non maternal data.

    Parameters:
        df: Pandas DataFrame
            Must have 'icg_id' as a column
    """

    assert "icg_id" in df.columns, "'icg_id' must be a column."

    # get causes
    causes = get_cause_metadata(cause_set_id=9, gbd_round_id=gbd_round_id)

    causes.drop_duplicates(inplace=True)

    # create condiational mask that selects maternal causes
    condition = causes.path_to_top_parent.str.contains("366")

    # subset just causes that meet the condition sdf
    maternal_causes = causes[condition]

    # make list of maternal causes
    maternal_list = list(maternal_causes["cause_id"].unique())

    # get bundle to cause map
    bundle_cause = query("QUERY", conn_def="CONN")

    # we need to go from ICG id to bundle to cause, so first merge bundle onto data
    icg_bundle = clinical_mapping.get_clinical_process_data("icg_bundle", prod=True)
    mat_icg = bundle_cause.merge(icg_bundle, how="left", on=["bundle_id"])
    # keep just icgs mapping to maternal causes
    mat_icg = mat_icg[mat_icg.cause_id.isin(maternal_list)]
    # drop the denominator bundle
    mat_icg = mat_icg[mat_icg["bundle_id"] != 1010]
    pre_icgs = mat_icg["icg_id"].unique().size
    assert pre_icgs == mat_icg["icg_name"].unique().size, "Why don't these match?"
    # drop duplicates caused by the bundle_id merge
    mat_icg = mat_icg[["icg_id", "icg_name"]].drop_duplicates()
    assert pre_icgs == mat_icg["icg_name"].unique().size, "Why don't these match?"

    # keep only maternal ICGs in df
    df = df[df["icg_id"].isin(mat_icg["icg_id"])]

    return df


def adjust_maternal_denom(df, denom_type, run_id):
    """
    Function that adjusts the maternal bundles by dividing each of them by
    bundle 1010 or IFD ASFR

    Parameters:
        df: Pandas DataFrame
    """

    # drop sample_size, UTLAs already had it, but we need it for
    # everything, so we have to drop it.
    if "sample_size" in df.columns:
        df.drop("sample_size", axis=1, inplace=True)
    if "mean_inj" in df.columns:
        df.drop("mean_inj", axis=1, inplace=True)

    # keep only allowed ages and sexes
    good_age_group_ids = [7, 8, 9, 10, 11, 12, 13, 14, 15]
    df = df[df.age_group_id.isin(good_age_group_ids)].copy()
    df = df[df.sex_id == 2].copy()

    # read in maternal denoms, this is needed when our process is
    # parallelized
    denom_path = "FILEPATH".format(run_id)
    warnings.warn(
        """

                  ENSURE THAT THE DENOMINATORS FILE IS UP TO DATE.
                  the file was last edited at {}

                  """.format(
            time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(denom_path))
            )
        )
    )
    denom = pd.read_hdf(denom_path, key="df")

    # rename denominator columns before merging
    denom_cols = sorted(denom.filter(regex="^mean").columns)
    # list comprehension to make new col names
    new_denom_cols = [x + "_denominator" for x in denom_cols]
    denom.rename(columns=dict(list(zip(denom_cols, new_denom_cols))), inplace=True)

    # merge on denominator
    pre = df.shape[0]
    # set cols to merge onto df
    merge_on = ["location_id", "year_id", "age_group_id", "sex_id"]

    denom["age_group_id"] = pd.to_numeric(denom.age_group_id)
    df = df.merge(denom, how="left", on=merge_on)
    assert pre == df.shape[0], "shape should not have changed " "during merge"

    # todo -- dropping square data?
    df = df[(df["mean_raw"] > 0) | (df["mean_raw_denominator"].notnull())]

    assert df.mean_raw_denominator.isnull().sum() == 0, (
        "shouldn't be " "any null values in this column"
    )

    # regex to find the columns that start with "mean" and DONT end with
    # "tor", ie "denominator"
    num_cols = sorted(df.filter(regex="^mean.*[^(tor)]$").columns)

    if denom_type == "bundle1010":
        assert Fales, "This code must be rebuilt to fit the run_id structure"
        # num cols and denom cols should be same length
        assert len(num_cols) == len(
            new_denom_cols
        ), "the denom cols and num are different lengths, gonna break the division {} and {}".format(
            num_cols, new_denom_cols
        )

        # make sure the col names line up, ie that "mean_incidence" will be
        # divided by "mean_incidence_denominator"
        for i in np.arange(0, len(new_denom_cols), 1):
            print(num_cols[i], new_denom_cols[i], new_denom_cols[i][:-12])
            assert num_cols[i] == new_denom_cols[i][:-12]

        # divide each bundle value by maternal denom to get the adjusted rate
        for i in np.arange(0, len(new_denom_cols), 1):
            df[num_cols[i]] = df[num_cols[i]] / df[new_denom_cols[i]]

    # for ifd asfr method we divide mean raw by adjusted denom then multiply by our scalars
    # to get CFs, technically I think we could do this to each upper and lower value
    if denom_type == "ifd_asfr":
        # divide mean raw by mean raw denom
        df["mean_raw"] = df["mean_raw"] / df["mean_raw_denominator"]

    # drop the denominator columns
    df.drop(new_denom_cols, axis=1, inplace=True)

    # the current upper and lower cols are not methodologically correct
    # fill them with NaNs
    bounds = df.filter(regex="^upper|^lower").columns
    for uncertainty in bounds:
        df[uncertainty] = np.nan

    # can't divide by zero
    df = df[df["sample_size"] != 0]
    # RETURN ONLY THE MATERNAL DATA

    return df


def rename_raw_cols(df):
    df.rename(
        columns={"mean": "mean_raw", "lower": "lower_raw", "upper": "upper_raw"},
        inplace=True,
    )
    return df


def drop_extraneous_cols(df):
    # drop all the extra cols
    cols = df.columns
    to_drop = df.filter(regex="prevalence$|incidence$|indvcf$").columns.tolist()
    to_drop = to_drop + ["val", "numerator", "denominator"]
    for col in to_drop:
        if col in cols:
            df.drop(col, axis=1, inplace=True)

    return df


def prep_maternal_main(
    df,
    run_id,
    gbd_round_id,
    decomp_step,
    clinical_age_group_set_id,
    cf_model_type,
    bundle_level_cfs,
    write_denom=True,
    write=False,
    denom_type="ifd_asfr",
):

    # if write the maternal denoms
    if write_denom:
        # write the actual denominator file
        write_maternal_denom(
            denom_type=denom_type,
            run_id=run_id,
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step,
            clinical_age_group_set_id=clinical_age_group_set_id,
        )

    df = select_maternal_data(
        df=df, gbd_round_id=gbd_round_id
    )  # subset just maternal causes

    df = drop_extraneous_cols(df)
    df = rename_raw_cols(df)
    df = adjust_maternal_denom(df, denom_type=denom_type, run_id=run_id)

    if bundle_level_cfs:
        pass
    else:
        df = correction_factor_utils.apply_corrections(df, run_id, cf_model_type)

    # data types constantly changing, probably from merges
    cols_to_numer = ["icg_id", "location_id", "sex_id", "year_id", "age_group_id"]
    for col in cols_to_numer:
        df[col] = pd.to_numeric(df[col], errors="raise")

    df.drop(["ifd_mean", "asfr_mean"], axis=1, inplace=True)

    if write:
        file_path = "FILEPATH".format(run_id)
        print("Writing the maternal df file to {}...".format(file_path))
        hosp_prep.write_hosp_file(df, file_path, backup=True)

    return df

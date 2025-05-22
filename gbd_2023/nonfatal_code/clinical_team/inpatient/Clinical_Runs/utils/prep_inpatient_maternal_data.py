"""Set of functions to create or adjust the inpatient maternal denominators"""
import warnings
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
from crosscutting_functions.clinical_constants.values import Estimates
from crosscutting_functions.clinical_metadata_utils.api.pipeline_wrappers import (
    InpatientWrappers,
)
from crosscutting_functions import demographic, legacy_pipeline
from crosscutting_functions.get-cached-population import cached_pop_tools
from crosscutting_functions.maternal import get_maternal_bundles
from crosscutting_functions.pipeline import get_release_id
from db_queries import get_location_metadata

from inpatient.Clinical_Runs.utils.constants import RunDBSettings
from inpatient.Envelope import apply_bundle_cfs as abc
from crosscutting_functions import uncertainty

MATERNAL_AGE_GROUPS = [7, 8, 9, 10, 11, 12, 13, 14, 15]
MATERNAL_ESTS_DICT = {
    Estimates.inp_primary_unadj: Estimates.inp_primary_live_births_unadj,
    Estimates.inp_primary_cf1_modeled: Estimates.inp_primary_live_births_cf1_modeled,
    Estimates.inp_primary_cf2_modeled: Estimates.inp_primary_live_births_cf2_modeled,
    Estimates.inp_primary_cf3_modeled: Estimates.inp_primary_live_births_cf3_modeled,
}
MATERNAL_ESTIMATES = list(MATERNAL_ESTS_DICT.values())


def _run_shared_funcs(clinical_age_group_set_id, run_id):
    """
    get all the central inputs we'll need. Population and asfr and ifd covariates
    """
    warnings.warn("\nYears are hard coded to start at 1988 and end at 2023\n")
    years = list(np.arange(1988, 2023, 1))

    locs = (
        get_location_metadata(location_set_id=35, release_id=get_release_id(run_id))[
            "location_id"
        ]
        .unique()
        .tolist()
    )
    ages = (
        demographic.get_hospital_age_groups(
            clinical_age_group_set_id=clinical_age_group_set_id
        )["age_group_id"]
        .unique()
        .tolist()
    )

    pop = cached_pop_tools.get_cached_pop(run_id, drop_duplicates=True)
    pop = pop[(pop["sex_id"] == 2)]

    # GET ASFR and IFD
    # has age/location/year
    warnings.warn(
        "We're pulling shared data with release id {}".format(get_release_id(run_id))
    )

    asfr = legacy_pipeline.get_covar_for_clinical(
        covariate_name_short="ASFR",
        run_id=run_id,
        iw_profile="clinical",
        location_id=locs,
        age_group_id=ages,
        year_id=years,
    )
    ifd = legacy_pipeline.get_covar_for_clinical(
        covariate_name_short="IFD_coverage_prop", run_id=run_id, iw_profile="clinical"
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


def _merge_pop_asfr_ifd(
    pop: pd.DataFrame, asfr: pd.DataFrame, ifd: pd.DataFrame
) -> pd.DataFrame:
    """
    central inputs will be multiplied together to get ifd*asfr*population
    to do this we first need to merge the data together into the same df
    """
    # first merge asfr and ifd
    new_denom = asfr.merge(
        ifd, how="outer", on=["location_id", "sex_id", "year_id"], validate="m:1"
    )
    # drop data before 1987, our earliest hosp year is 1988
    new_denom = new_denom[new_denom.year_id > 1987]
    # keep only female data
    new_denom = new_denom[new_denom.sex_id == 2]

    pai = pop.merge(
        new_denom,
        how="outer",
        on=["location_id", "sex_id", "age_group_id", "year_id"],
        validate="1:1",
    )

    # create the new denominator
    pai["ifd_asfr_denom"] = pai["population"] * pai["asfr_mean"] * pai["ifd_mean"]

    return pai


def _clean_shared_output(
    pop: pd.DataFrame, asfr: pd.DataFrame, ifd: pd.DataFrame
) -> pd.DataFrame:
    """Prep the central inputs to work with our process."""
    pop.drop("pop_run_id", axis=1, inplace=True)

    keeps = ["location_id", "sex_id", "age_group_id", "year_id", "mean_value"]
    asfr = asfr[keeps].copy()
    asfr.rename(columns={"mean_value": "asfr_mean"}, inplace=True)

    ifd = ifd[keeps].copy()
    ifd.rename(columns={"mean_value": "ifd_mean"}, inplace=True)
    # IFD doesn't return data with age or sex specified
    ifd = ifd.drop("age_group_id", axis=1)
    ifd["sex_id"] = 2

    return pop, asfr, ifd


def _create_maternal_rate(mat_res: pd.DataFrame) -> pd.DataFrame:
    """Create the unadjusted maternal rate."""
    # create maternal denom in rate space
    mat_res["ifd_asfr_denom_rate"] = mat_res["ifd_asfr_denom"] / mat_res["population"]
    mat_res = mat_res.drop(["population"], axis=1)

    return mat_res


def _bin_maternal_denoms(mat_df: pd.DataFrame) -> pd.DataFrame:
    """Convert the maternal denominator data from single-year rates of live births to
    aggregated 5 year rates."""
    # Re-calculate the live birth rate using 5 year bins.
    mat_df = demographic.year_binner(df=mat_df)
    # rate to count space
    mat_df["live_births"] = mat_df["sample_size"] * mat_df["mean_raw"]
    # sum denominators and numerators in 5 year bins
    mat_df = (
        mat_df.groupby(["age_group_id", "sex_id", "location_id", "year_start", "year_end"])[
            "sample_size", "live_births"
        ]
        .sum()
        .reset_index()
    )
    # re-calculate live birth rate to use when adjusting data in 5 year bins
    mat_df["mean_raw"] = mat_df["live_births"] / mat_df["sample_size"]

    return mat_df


def write_maternal_denom(
    denom_type: str, run_id: int, clinical_age_group_set_id: int, bin_years: bool
) -> None:
    """
    Write the Inpatient maternal denominator data.
    Updated September 2019 to remove the need of a hospital data input, now we just
    pull a bunch of centrally identified ages and locations and then they'll be
    dropped when you left-merge denoms. Benefit is that this is now a standalone function,
    no need to load hospital data!

    Args:
        denom_type: 'ifd_asfr' or 'bundle1010', but currently bundle1010 is broken
        run_id: Standard clinical run_id.
        clinical_age_group_set_id: internal clinical age group sets to identify which age
                                   groups to pull from shared/central tools
        bin_years: Whether or not the bin the maternal denominators into 5 year groups. This
                   option must align with the same self.bin_years attribute of the inpatient
                   processing pipeline

    Raises:
        NotImplementedError: If the bundle1010 option is chosen.
    """
    # Pull good maternal ages
    good_age_group_ids = [7, 8, 9, 10, 11, 12, 13, 14, 15]

    if denom_type == "ifd_asfr":
        # get the three estimates from central Dbs
        pop, asfr, ifd = _run_shared_funcs(
            run_id=run_id,
            clinical_age_group_set_id=clinical_age_group_set_id,
        )
        pop, asfr, ifd = _clean_shared_output(pop, asfr, ifd)

        # multiply them together to get our new denom which is IFD * ASFR * POP
        mat_df = _merge_pop_asfr_ifd(pop, asfr, ifd)
        # keep only good ages
        warnings.warn(f"\nMat denoms are being created with age groups {good_age_group_ids}")
        mat_df = mat_df[mat_df.age_group_id.isin(good_age_group_ids)]

        mat_df = _create_maternal_rate(mat_df)
        mat_df.rename(
            columns={
                "ifd_asfr_denom_rate": "mean_raw",
                "ifd_asfr_denom": "sample_size",
                "year_id": "year_start",
            },
            inplace=True,
        )
        mat_df["year_end"] = mat_df["year_start"]

        # can't use null data and there seem to be regional null ifd/asfr vals
        prerows = len(mat_df)
        mat_df = mat_df[mat_df["sample_size"].notnull()]
        diff = prerows - len(mat_df)
        print(f"{diff} rows lost due to null sample sizes. This probably isn't an issue")

        mat_df.columns = mat_df.columns.astype(str)

        if bin_years:
            mat_df = _bin_maternal_denoms(mat_df)

        mat_df.to_hdf(FILEPATH,
            key="df",
            mode="w",
        )

    if denom_type == "bundle1010":
        raise NotImplementedError(
            "This needs to be rebuilt to run in individual years before we can run it"
        )


def _load_mat_denoms(run_id: int) -> pd.DataFrame:
    """Load in the maternal denom file for adjusting maternal ICGs"""
    denom_path = (FILEPATH
    )
    mat_denom = pd.read_hdf(denom_path, key="df")
    return mat_denom


def _clean_mat_denoms(mat_denom: pd.DataFrame) -> pd.DataFrame:
    """Drop some unneeded columns and rename other cols"""
    denom_cols = mat_denom.columns.tolist()

    drops = [d for d in denom_cols if d in ["asfr_mean", "ifd_mean"]]
    if drops:
        mat_denom = mat_denom.drop(drops, axis=1)
    # clarify exactly which are the maternal columns
    mat_denom = mat_denom.rename(
        columns={
            "sample_size": "maternal_sample_size",
            "mean_raw": "maternal_mean",
        }
    )
    return mat_denom


def _merge_mat_denoms(maternal_data: pd.DataFrame, mat_denom: pd.DataFrame) -> pd.DataFrame:
    """Attach maternal denom data to maternal subset of inpatient data"""
    merge_cols = ["age_group_id", "location_id", "sex_id", "year_start", "year_end"]
    maternal_data = maternal_data.merge(mat_denom, how="left", on=merge_cols, validate="m:1")
    return maternal_data


def _clean_mat_cols(maternal_data: pd.DataFrame, run_id: int) -> pd.DataFrame:
    """Rename the maternal_sample_size column to just sample size and drop a maternal mean
    column."""
    maternal_data = maternal_data.rename(columns={"maternal_sample_size": "sample_size"})
    maternal_data = maternal_data.drop(
        [
            "maternal_mean",
            "years",
            "live_births",
            "population",
            "median_CI_team_only",
            "source",
        ],
        axis=1,
    )

    maternal_data["source_type_id"] = 10
    maternal_data["diagnosis_id"] = 1
    maternal_data["run_id"] = run_id
    return maternal_data


def _remove_non_maternal_ages(maternal_data: pd.DataFrame) -> pd.DataFrame:
    """Retain only the 9 age groups that maternal modelers expect."""
    maternal_data = maternal_data[maternal_data.age_group_id.isin(MATERNAL_AGE_GROUPS)]
    return maternal_data


def adjust_mat_denom(run_id: int, draw_cols: List[str], cf_agg_stat: str) -> pd.DataFrame:
    """Live births are the population of interest for maternal causes.
    Adjust them accordingly."""

    iw = InpatientWrappers(run_id, RunDBSettings.iw_profile)
    run_metadata = iw.pull_run_metadata()

    # make a copy of df with just maternal data
    maternal_bundles = get_maternal_bundles(
        map_version=run_metadata["map_version"].iloc[0], run_id=run_id
    )

    maternal_data = []
    draw_files = Path(FILEPATH
    ).glob(pattern="*.H5")
    for file in draw_files:
        split_file = file.stem.split("_")
        age_group = int(split_file[0])
        sex_id = int(split_file[1])
        if age_group not in MATERNAL_AGE_GROUPS or sex_id != 2:
            continue
        df = pd.read_hdf(file)
        maternal_subset = df[df["bundle_id"].isin(maternal_bundles)]
        maternal_subset = _remove_non_maternal_ages(maternal_subset)
        maternal_data.append(maternal_subset)

    maternal_data = pd.concat(maternal_data, ignore_index=True, sort=False)

    ui_cols_to_replace = ["sample_size", "mean", "upper", "lower", "median_CI_team_only"]
    maternal_data = maternal_data.drop(ui_cols_to_replace, axis=1)

    # pull in the maternal denoms and set good col names
    mat_denom = _load_mat_denoms(run_id)
    mat_denom = _clean_mat_denoms(mat_denom)
    # merge on maternal denoms
    maternal_data = _merge_mat_denoms(maternal_data=maternal_data, mat_denom=mat_denom)
    if maternal_data["maternal_mean"].isnull().any():
        raise RuntimeError("The merge appears to have failed. Please review the data.")

    for draw in draw_cols:
        # divide mean rate by maternal rate from denom
        # This is mathematically the same as dividing the draw count by sample size and
        # then recalculating the draw rate.
        maternal_data[draw] = maternal_data[draw] / maternal_data["maternal_mean"]

    # set estimate_id for maternal adjusted inp data
    maternal_data["estimate_id"] = maternal_data["estimate_id"].map(MATERNAL_ESTS_DICT)
    estimate_diff = set(MATERNAL_ESTIMATES).symmetric_difference(
        maternal_data["estimate_id"].unique()
    )
    if estimate_diff:
        raise ValueError(
            f"We expect maternal_data to contain only maternal estimates. {estimate_diff}"
        )

    # calculate UI and use cf agg stat to determine whether to replace mean with median
    maternal_data = uncertainty.add_ui(df=maternal_data, drop_draw_cols=True)

    # replace the flagged rows' lower mean and median with 0
    maternal_data = abc.replace_zero_row_lower_mean_median(df=maternal_data)

    if cf_agg_stat == "median":
        maternal_data["mean"] = maternal_data["median_CI_team_only"]

    # drop the flag col
    maternal_data = maternal_data.drop(["zero_flag"], axis=1)

    maternal_data = _clean_mat_cols(maternal_data=maternal_data, run_id=run_id)

    return maternal_data

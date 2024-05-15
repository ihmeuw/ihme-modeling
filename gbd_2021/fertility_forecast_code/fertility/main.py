r"""Orchestrator of fertility pipeline.
"""
import logging
from typing import Union

import pandas as pd
import xarray as xr
from fhs_lib_database_interface.lib.query.age import get_ages
from fhs_lib_database_interface.lib.query.location import get_location_set
from fhs_lib_file_interface.lib.file_interface import FBDPath
from fhs_lib_file_interface.lib.versioning import Versions
from fhs_lib_file_interface.lib.xarray_wrapper import save_xr
from fhs_lib_year_range_manager.lib.year_range import YearRange

from fhs_pipeline_fertility.lib import stage_1, stage_2, stage_3
from fhs_pipeline_fertility.lib.constants import (
    COHORT_AGE_END,
    COHORT_AGE_START,
    FERTILE_AGE_IDS,
    FIVE_YEAR_AGE_GROUP_SIZE,
    MODELED_FERTILE_AGE_IDS,
    OLD_TERMINAL_AGES,
    PRONATAL_THRESHOLD,
    RAMP_YEARS,
    REFERENCE_SCENARIO_COORD,
    YOUNG_TERMINAL_AGES,
    DimensionConstants,
    StageConstants,
)
from fhs_pipeline_fertility.lib.input_transform import convert_period_to_cohort_space

LOGGER = logging.getLogger(__name__)


def fertility_pipeline(
    versions: Versions,
    years: YearRange,
    draws: int,
    gbd_round_id: int,
    ensemble: bool,
    pronatal_bump: Union[float, None],
) -> None:
    """Run the fertility pipeline.

    Fertility pipeline contains the following stages:

    1.) starting with past ASFR, compute CCFX from age 15 through 49.
    2.) forecast CCF50 (summed over 15-49) based on (1).
    3.) map CCF50 back to ASFR.

    4.) If pronatal_policy, add bump to each location/year ASFR according
        to how its TFR is distributed amongst the ages.

    Args:
        versions (Versions): All relevant input/output versions.
        years (YearRange): past_start:forecast_start:forecast_end.
        draws (int): The number of draws to generate.
        gbd_round_id (int): The numeric ID of GBD round.
        ensemble (bool): whether to make an ensemble out of 2/3/4-covariate
            forecasts for CCF in (2).
        pronatal_bump (Union[float, None]): if pronatal, this value will be
            distributed amongst single year ASFR for every future
            location/year.
    """
    # first collect some metadata needed to execute the stages
    asfr_past_version = versions["past"]["asfr"]

    # we fit on nations data and predict to all locations,
    # and we constantly switch between 5- and 1-year age groups.
    # so we need to start with some location/age metadata here.
    locations_df = get_location_set(gbd_round_id=gbd_round_id)
    ages_df = get_ages().query(f"{DimensionConstants.AGE_GROUP_ID} in {FERTILE_AGE_IDS}")[
        ["age_group_id", "age_group_years_start", "age_group_years_end"]
    ]

    # for modeled forecast, we only want age group ids 8-14
    modeled_ages_df = ages_df.query(
        f"{DimensionConstants.AGE_GROUP_ID} in {MODELED_FERTILE_AGE_IDS}"
    )

    # stage 1
    ccfx, last_past_year, last_cohort = stage_1.get_past_asfr_into_ccfx(
        asfr_past_version, years, gbd_round_id, draws, locations_df
    )

    # done with stage 1, save data first.
    ccf_past_data_path = FBDPath(
        gbd_round_id=gbd_round_id,
        past_or_future="past",
        stage="ccf",
        version=versions["future"]["asfr"],
    )

    save_xr(
        ccfx,
        ccf_past_data_path / "ccfx.nc",
        metric="rate",
        space="identity",
        versions=str(versions),
        ensemble_covs=str(StageConstants.ENSEMBLE_COV_COUNTS),
    )

    # stage 2
    if ensemble:
        ccf_future, fit_summary = stage_2.ensemble_forecast_ccf(
            ccfx,
            last_cohort,
            versions,
            gbd_round_id,
            years,
            draws,
            locations_df,
            modeled_ages_df,
        )
    else:
        ccf_future, fit_summary = stage_2.forecast_ccf(
            ccfx,
            last_cohort,
            versions,
            gbd_round_id,
            years,
            draws,
            locations_df,
            modeled_ages_df,
        )

    if type(fit_summary) is list:  # means we ran an ensemble model
        coefficients = pd.concat([tup[0] for tup in fit_summary])
    else:  # just a two-tuple, where we only need the first element.
        coefficients = fit_summary[0]

    # stage 3
    asfr_future = stage_3.forecast_asfr_from_ccf(
        ccfx,
        ccf_future,
        last_cohort,
        gbd_round_id,
        versions,
        locations_df,
        modeled_ages_df,
        years,
    )

    # add the pronatal policy boost if so desired.
    if pronatal_bump:
        asfr_future = add_pronatal_policy(
            asfr_future, pronatal_bump, tfr_threshold=PRONATAL_THRESHOLD, ramp_years=RAMP_YEARS
        )

    # done with computations.  Now save the results.
    save_data_path = FBDPath(
        gbd_round_id=gbd_round_id,
        past_or_future="future",
        stage="asfr",
        version=versions["future"]["asfr"],
    )

    # save future files with a scenario dim
    future_scenario = versions["future"].get_version_metadata("asfr").scenario

    if future_scenario is None:  # then defaults to reference
        future_scenario = REFERENCE_SCENARIO_COORD

    save_xr(
        asfr_future.expand_dims(dim={DimensionConstants.SCENARIO: [future_scenario]}),
        save_data_path / "asfr_single_year.nc",
        metric="rate",
        space="identity",
        versions=str(versions),
        ensemble_covs=str(StageConstants.ENSEMBLE_COV_COUNTS),
    )

    coefficients.to_csv(save_data_path / "coefficients.csv")

    asfr_5yr = make_five_year_asfr_from_single_year(asfr_future, ages_df)

    save_xr(
        asfr_5yr.expand_dims(dim={DimensionConstants.SCENARIO: [future_scenario]}),
        save_data_path / "asfr.nc",
        metric="rate",
        space="identity",
        versions=str(versions),
        ensemble_covs=str(StageConstants.ENSEMBLE_COV_COUNTS),
    )

    tfr = asfr_future.sum(DimensionConstants.AGE)

    tfr_data_path = FBDPath(
        gbd_round_id=gbd_round_id,
        past_or_future="future",
        stage="tfr",
        version=versions["future"]["asfr"],
    )

    save_xr(
        tfr.expand_dims(dim={DimensionConstants.SCENARIO: [future_scenario]}),
        tfr_data_path / "tfr.nc",
        metric="rate",
        space="identity",
        versions=str(versions),
        ensemble_covs=str(StageConstants.ENSEMBLE_COV_COUNTS),
    )

    # ccf50 requires past/future cohort asfrs because ccf spans many years
    asfr_future_cohort = convert_period_to_cohort_space(da=asfr_future)
    asfr_past_cohort = xr.concat(
        [ccfx.sel(age=COHORT_AGE_START), ccfx.diff(DimensionConstants.AGE)],
        dim=DimensionConstants.AGE,
    )
    asfr_cohort = asfr_past_cohort.combine_first(asfr_future_cohort)
    ccf = asfr_cohort.sel(age=range(COHORT_AGE_START, COHORT_AGE_END + 1)).sum(
        DimensionConstants.AGE
    )

    ccf_data_path = FBDPath(
        gbd_round_id=gbd_round_id,
        past_or_future="future",
        stage="ccf",
        version=versions["future"]["asfr"],
    )

    save_xr(
        ccf.expand_dims(dim={DimensionConstants.SCENARIO: [future_scenario]}),
        ccf_data_path / "ccf.nc",
        metric="rate",
        space="identity",
        versions=str(versions),
        ensemble_covs=str(StageConstants.ENSEMBLE_COV_COUNTS),
    )

    return


def make_five_year_asfr_from_single_year(
    asfr_future: xr.DataArray, ages_df: pd.DataFrame
) -> xr.DataArray:
    """Make 5-year asfr from single-year asfr by taking the mean.

    The asfr of a five-year age group is the mean of the single-year
    asfrs within that group.

    Args:
        asfr_future (xr.DataArray): forecasted single-year asfr.
        ages_df (pd.DataFrame): age-related metadata.

    Returns:
        (xr.DataArray): five-year asfr.
    """
    asfr_5yr_list = []
    for age_start in range(
        min(YOUNG_TERMINAL_AGES), max(OLD_TERMINAL_AGES) + 1, FIVE_YEAR_AGE_GROUP_SIZE
    ):
        ages = range(age_start, age_start + FIVE_YEAR_AGE_GROUP_SIZE)
        # just take the mean over the single years for 5-year asfr
        asfr_5yr = asfr_future.sel(age=ages).mean(DimensionConstants.AGE)
        asfr_5yr[DimensionConstants.AGE_GROUP_ID] = int(
            ages_df.query(f"age_group_years_start == {age_start}")["age_group_id"]
        )
        asfr_5yr_list.append(asfr_5yr)
    asfr_5yr = xr.concat(asfr_5yr_list, dim=DimensionConstants.AGE_GROUP_ID)

    return asfr_5yr


def add_pronatal_policy(
    asfr_single_year: xr.DataArray, pronatal_bump: float, tfr_threshold: float, ramp_years: int
) -> xr.DataArray:
    """Implement pronatal policy by adding boost to TFR.

    The boost is ramped over from 0 to pronatal_bump over
    ram_years.

    Go through every location/year in the future and distribute
    the value amongst ages by the current distribution.

    Args:
        asfr_single_year (xr.DataArray): forecasted single-year
            asfr.
        pronatal_bump (float): if pronatal, this value will be
            distributed amongst single year ASFR for every future
            location/year.the final TFR bump expected from policy.
        tfr_threhold (float): threhold of TFR below which the
            pronatal policy is triggered.
        ramp_years (int): number of years over which the pronatal
            boost is linearly ramped up.

    Returns:
        (xr.DataArray): post pro-natal policy worldwide asfr.
    """
    for location_id in asfr_single_year[DimensionConstants.LOCATION_ID]:
        policy_already_implemented = False  # starting assumption
        policy_start_year = None  # starting assumption

        for year_id in asfr_single_year[DimensionConstants.YEAR_ID]:
            asfr = asfr_single_year.sel(location_id=location_id, year_id=year_id).mean(
                DimensionConstants.DRAW
            )
            tfr = float(asfr.sum(DimensionConstants.AGE))  # tfr = sum over asfr ages
            # starting year of policy is the first future year below threshold
            if tfr < tfr_threshold and not policy_already_implemented:
                policy_already_implemented = True
                policy_start_year = year_id

            if policy_already_implemented:  # meaning this year gets a boost
                if year_id - policy_start_year >= ramp_years:  # already at full boost
                    bump = pronatal_bump
                else:  # during ramp up
                    slope = (year_id - policy_start_year) / ramp_years  # ramp up slope
                    bump = pronatal_bump * slope  # the total tfr bump for this year

                asfr_dist = asfr / tfr  # prob. distribution of asfr over ages
                asfr_bump = bump * asfr_dist  # distributes boost over ages
                # now simply add boost to this location/year
                asfr_single_year.loc[dict(location_id=location_id, year_id=year_id)] = (
                    asfr_single_year.sel(location_id=location_id, year_id=year_id) + asfr_bump
                )

    return asfr_single_year

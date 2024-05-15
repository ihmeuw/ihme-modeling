"""Fertility stage 1: read in of past asfr into ccfx."""
import logging

import numpy as np
import pandas as pd
import xarray as xr
from fhs_lib_file_interface.lib.file_interface import FBDPath
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr
from fhs_lib_year_range_manager.lib.year_range import YearRange

from fhs_pipeline_fertility.lib.constants import (
    COHORT_AGE_END,
    COHORT_AGE_START,
    DimensionConstants,
    StageConstants,
)
from fhs_pipeline_fertility.lib.input_transform import convert_period_to_cohort_space

LOGGER = logging.getLogger(__name__)


def get_past_asfr_into_ccfx(
    asfr_past_version: str,
    years: YearRange,
    gbd_round_id: int,
    draws: int,
    locations_df: pd.DataFrame,
) -> xr.DataArray:
    """Read in past asfr data and convert to ccfx (cohort spce).

    The returned object has cohort_id instead of year_id.  For the forecast
    model, we only use ages from COHORT_AGE_START through COHORT_AGE_END.

    Args:
        asfr_past_version (str): past asfr version.
        years (YearRange): past_start:forecast_start:forecast_end.
        gbd_round_id (int): gbd round id.
        draws (int): number of draws to keep.
        locations_df (pd.DataFrame): all location metadata.

    Returns:
        tuple(xr.DataArray | int) Fertility data in cohort space (indexed by
        cohort birth year ``cohort_id``):
        * ``ccfX``: cumulative cohort fertility up to
          ``DimensionConstants.AGE``=``X``.
        and
        * ``last_past_year``: inferred from past data.  Sets cohort ranges.
        * ``last_complete_cohort``: the last cohort that has age up to
            COHORT_AGE_END.
    """
    LOGGER.info("Stage 1: converting past asfr into ccfx")

    # collect all national/subnationals
    loc_ids = locations_df[(locations_df["level"] >= 3)].location_id.values

    # now we infer what past years we have from the past asfr data.
    path = FBDPath(
        gbd_round_id=gbd_round_id,
        past_or_future="past",
        stage="asfr",
        version=asfr_past_version,
    )

    # past data for some reason labels "age" as "age group id"
    asfr_past = (
        open_xr(path / "asfr.nc")
        .sel(
            location_id=loc_ids,
            draw=range(draws),
            year_id=years.past_years,
            age_group_id=range(COHORT_AGE_START, COHORT_AGE_END + 1),
        )
        .rename({DimensionConstants.AGE_GROUP_ID: DimensionConstants.AGE})
    )

    # now that we're done reading in asfr, do some transformation to get ccfx.
    past_years = asfr_past[DimensionConstants.YEAR_ID].values.tolist()
    last_past_year = max(past_years)  # last past year inferred from data.

    asfr_past_cohort = convert_period_to_cohort_space(da=asfr_past)

    valid_cohorts = range(
        min(past_years) - COHORT_AGE_START, max(past_years) - COHORT_AGE_START + 1
    )

    asfr_past_cohort = asfr_past_cohort.sel(cohort_id=valid_cohorts)

    # ccfx has cumulative fertility by age, for all past complete/incomplete
    # cohorts.  That means we start seeing NaNs in first complete cohort.
    ccfx = asfr_past_cohort.cumsum(DimensionConstants.AGE).where(np.isfinite(asfr_past_cohort))
    # after cumsum, change name of dim so its meaning is more clear
    ccfx = ccfx.sortby(DimensionConstants.AGE)
    ccfx.name = StageConstants.CCF

    last_complete_cohort = last_past_year - COHORT_AGE_END

    LOGGER.info("Done completing the past cohorts...")

    return ccfx, last_past_year, last_complete_cohort

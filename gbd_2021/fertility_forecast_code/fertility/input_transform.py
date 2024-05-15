"""Helper I/O functions for the fertility pipeline."""
import logging
from typing import List, Union

import xarray as xr
from fhs_lib_file_interface.lib.file_interface import FBDPath
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr

from fhs_pipeline_fertility.lib.constants import PAST_MET_NEED_YEAR_START, DimensionConstants

LOGGER = logging.getLogger(__name__)


# this func is used for stage 2 of fertility: CCF forecast.
# also used in stage 3 step 1
def get_covariate_data(
    stage: str,
    future_version: str,
    past_version: str,
    year_ids: List[int],
    gbd_round_id: int,
    location_ids: List[int],
    age_group_ids: Union[int, List[int]],
    scenario: int,
    draws: int,
) -> xr.DataArray:
    r"""Extract age-specific covariate data in period space.

    Args:
        stage (str): the stage (education, met_need, etc.) of the covariate.
        future_version (str): version of future covariate.
        past_version (str): version of past covariate.
        year_ids (List[int]): all year_ids needed from past + future.
        gbd_round_id (int): gbd round id.
        location_ids (List[int]): the location ids to pull data for.
        age_group_ids (Union[int, List[int]]): age group ids for ccf covariate.
        scenario (int): the future scenario to pull.
        draws (int): the number of draws to return.

    Returns:
        (xr.DataArray): age-specific covariate data
            in period space (indexed by ``year_id`` and ``age_group_id``).
    """
    da = _load_fhs_covariate_data(
        stage=stage,
        version=future_version,
        gbd_round_id=gbd_round_id,
        draws=draws,
        past_or_future="future",
        age_group_ids=age_group_ids,
        scenario=scenario,
        location_ids=location_ids,
    )

    da.name = stage  # the name of the covariate will be handy

    # assume the past has no scenario (None)
    past_da = _load_fhs_covariate_data(
        stage=stage,
        version=past_version,
        gbd_round_id=gbd_round_id,
        draws=draws,
        past_or_future="past",
        age_group_ids=age_group_ids,
        scenario=None,
        location_ids=location_ids,
    )

    da = da.combine_first(past_da)  # future year_ids overwrite if overlap

    if type(age_group_ids) is int:
        da = da.drop_vars(DimensionConstants.AGE_GROUP_ID)

    # some covariates (met_need) may not have data going as far back as
    # the other covariates (education),
    # so we remove the extra early past years to align all past covariates
    if min(year_ids) < PAST_MET_NEED_YEAR_START:
        year_ids = range(PAST_MET_NEED_YEAR_START, max(year_ids) + 1)

    da = da.sel(year_id=year_ids)  # errors if we have missing years

    return da


def _load_fhs_covariate_data(
    stage: str,
    version: str,
    gbd_round_id: int,
    past_or_future: str,
    age_group_ids: Union[int, List[int]],
    location_ids: Union[int, List[int]],
    draws: int,
    scenario: Union[int, None],
) -> xr.DataArray:
    """Load covariate data for a given stage.

    This is really just a wrapper to standardize sex/scenario-related
    processing we apply to read-in covariate data for fertility.

    1.) if there is a sex_id dimension, only data for females is returned.
    2.) pull only chosen scenario from object.

    ..todo:: Replace with a centralized/core function?

    Args:
        stage (str): stage of data to load.
        version (str): past version name.
        gbd_round_id (int): GBD round id.
        past_or_future (str): either "past" or "future" (period space).
        age_group_ids (Union[int, List[int]]): age group ids to filter for.
        location_ids (Union[int, List[int]]): location ids to filter for.
        draws (int): number of draws to pull.
        scenario (int): scenario to return. Ignored if
            there is no scenario dimension in the data or ``scenario=None``.

    Returns:
        (xr.DataArray): requested data.
    """
    path = FBDPath(
        gbd_round_id=gbd_round_id, past_or_future=past_or_future, stage=stage, version=version
    )

    da = open_xr(path / f"{stage}.nc").sel(
        location_id=location_ids, age_group_id=age_group_ids, draw=range(draws)
    )

    if DimensionConstants.SCENARIO in da.dims and scenario is not None:
        da = da.sel(scenario=scenario, drop=True)
    # all of fertilty modeling is for female sex. cleaner to do all the needed
    # downstream data cleaning and manipulation if the sex dimension is dropped
    if DimensionConstants.SEX_ID in da.dims:
        if DimensionConstants.FEMALE_SEX_ID in da[DimensionConstants.SEX_ID].values:
            da = da.sel(sex_id=DimensionConstants.FEMALE_SEX_ID, drop=True)
        else:
            da = da.sel(sex_id=da[DimensionConstants.SEX_ID].values[0], drop=True)

    return da


def convert_period_to_cohort_space(da: xr.DataArray) -> xr.DataArray:
    """Convert single age year data from period space to cohort space.

    Change index dim ``year_id`` to ``cohort_id`` defined by cohort birth
    year as``year_id``-``age``.

    Args:
        da (xr.DataArray): data in period space by single age year ``age``
            and year of data ``year_id``.

    Returns:
        (xr.DataArray): data in cohort space by single age year ``age``
            and year of birth ``cohort_id``
    """
    das = []

    for age in da[DimensionConstants.AGE].values:
        da_sub = da.sel(age=age, drop=True)
        # define cohort_id = year_id - age
        da_sub[DimensionConstants.YEAR_ID] = da_sub[DimensionConstants.YEAR_ID] - age
        da_sub = da_sub.rename({DimensionConstants.YEAR_ID: DimensionConstants.COHORT_ID})
        # add back in age dim
        da_sub[DimensionConstants.AGE] = age
        das.append(da_sub)

    return xr.concat(das, dim=DimensionConstants.AGE)

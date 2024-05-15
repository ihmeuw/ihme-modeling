"""Utility/DB functions for the scalars pipeline.
"""

from typing import Any, List, Optional, Union

import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_database_interface.lib.constants import (
    AgeConstants,
    DimensionConstants,
    LocationConstants,
    SexConstants,
)
from fhs_lib_file_interface.lib.query import mediation
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec, VersionMetadata
from fhs_lib_file_interface.lib.xarray_wrapper import save_xr_scenario
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_pipeline_scalars.lib.forecasting_db import demographic_coords

logger = get_logger()


def product_of_mediation(
    acause: str, risk: str, mediators: List[str], gbd_round_id: int
) -> Union[xr.DataArray, int]:
    r"""Return :math:`\prod_{i} (1 - MF_{jic})`.

    :math:`j` is the risk whose adjusted PAF to acause c we're considering,
    and :math:`i` is the mediator.

    That means this method takes in only 1 risk, and a list of multiple
    mediators, and performs a product over all the :math:`(1 - MF_{jic})`'s.

    "metab_bmi" impacts "cvd_ihd" via "metab_sbp".  Say sbp is the only
    mediator of bmi.  So if PAF_{ihd/bmi} is 0.9 and MF_{sbp/bmi} is 0.6,
    then the adjusted PAF_{ihd/bmi} is 0.9 * (1 - 0.6) = 0.36,
    because 0.54 of the 0.9 comes from sbp.

    The mediation factor for (acause, risk) is provided in a flat file.

    Args:
        acause (str): analytical cause.
        risk (str): risk related to acause.
        mediators (list[str]): the risks that could potentially sit between risk and the acause
            in attribution.  Usually these are just all the risks that are paired with acause,
            and then we filter through the mediation file for the ones that matter.
        gbd_round_id (int): gbd round id.

    Returns:
        mediation_products: Either the number 1 (float), if the loaded data does not describe
            the given acause and rei, or else the mediation matrix, as an xarray DataArray, if
            it does.
    """
    logger.info(
        "Computing product of mediation of risk {} via mediators {} "
        "on acause {}".format(risk, mediators, acause)
    )

    med_risks = list(mediators)  # just making a copy for later .remove()
    # NOTE the list of mediators can include "risk" itself.  Here we remove
    # risk from the list of mediators
    if risk in med_risks:
        med_risks.remove(risk)

    med = mediation.get_mediation_matrix(gbd_round_id)

    # only apply mediation when mediation exists
    if acause in med["acause"] and risk in med["rei"]:
        mediators = list(set(med["med"].values) & set(med_risks))
        if mediators:
            mediation_factors = med.sel(acause=acause, rei=risk, med=mediators)
            return (1 - mediation_factors).prod("med")
        else:
            return 1  # default, if no mediation exists
    else:
        return 1  # default, if no mediation exists


def conditionally_triggered_transformations(
    da: xr.DataArray, gbd_round_id: int, years: YearRange
) -> xr.DataArray:
    """Encode dataarray transformations that are conditionally triggered.

    Any of the following conditions will trigger a transformation:

    1.)
        da only has sex_id = 3 (both sexes).  In this case, expand to
        sex_id = 1, 2.
    2.)
        da onyl has age_group_id = 22 (all ages).  In this case, expand to
        all age_group_ids of current gbd_round
    3.)
        da has different years than expected.  In this case, filter/interpolate
        for just DEMOGRAPHY_INDICES[YEAR_ID].
    4.)
        da has only location_id = 1 (all locations).  In this case, we replace
        location_id=1 with the latest gbd location ids.
    5.)
        da has a point dimension of "quantile".  Simply remove.

    Args:
        da (xr.DataArray): may or may not become transformed.
        gbd_round_id (int): gbd round id.
        years (YearRange): [past_start, forecast_start, forecast_end] years.

    Returns:
        (xr.DataArray): transformed datarray, or not.
    """
    if (
        DimensionConstants.SEX_ID in da.dims
        and len(da[DimensionConstants.SEX_ID]) == 1
        and da[DimensionConstants.SEX_ID] == SexConstants.BOTH_SEX_ID
    ):
        # some vaccine SEVs could be modeled this way
        da = _expand_point_dim_to_all(da, gbd_round_id, years, dim=DimensionConstants.SEX_ID)

    if (
        DimensionConstants.AGE_GROUP_ID in da.dims
        and len(da[DimensionConstants.AGE_GROUP_ID]) == 1
        and da[DimensionConstants.AGE_GROUP_ID] == AgeConstants.ALL_AGE_ID
    ):
        # if da has a few age groups but not all, no transformation here
        da = _expand_point_dim_to_all(
            da, gbd_round_id, years, dim=DimensionConstants.AGE_GROUP_ID
        )

    if (
        DimensionConstants.LOCATION_ID in da.dims
        and len(da[DimensionConstants.LOCATION_ID]) == 1
        and da[DimensionConstants.LOCATION_ID] == LocationConstants.GLOBAL_LOCATION_ID
    ):
        da = _expand_point_dim_to_all(
            da, gbd_round_id, years, dim=DimensionConstants.LOCATION_ID
        )

    # some times we're provided with not enough or too many years.
    # here we first construct a superset, then crop out unwanted years.
    if DimensionConstants.YEAR_ID in da.dims and set(
        da[DimensionConstants.YEAR_ID].values
    ) != set(demographic_coords(gbd_round_id, years)[DimensionConstants.YEAR_ID]):
        missing_years = list(
            set(demographic_coords(gbd_round_id, years)[DimensionConstants.YEAR_ID])
            - set(da[DimensionConstants.YEAR_ID].values)
        )
        if missing_years:
            da = _fill_missing_years_with_mean(da, DimensionConstants.YEAR_ID, missing_years)
        # Now I filter for only the ones I need
        da = da.loc[
            {
                DimensionConstants.YEAR_ID: demographic_coords(gbd_round_id, years)[
                    DimensionConstants.YEAR_ID
                ]
            }
        ]

    # we don't need the the quantile point coordinate
    if DimensionConstants.QUANTILE in da.dims and len(da[DimensionConstants.QUANTILE]) == 1:
        da = da.drop_vars(DimensionConstants.QUANTILE)

    return da


def _fill_missing_years_with_mean(
    da: xr.DataArray, year_dim: str, missing_years: List[int]
) -> xr.DataArray:
    """Fill missing years with the mean of other years.

    Args:
        da (xr.DataArray): should contain the year_dim dimension.
        year_dim (str): str name of the year dimension.
        missing_years (list): list of years that are missing.

    Returns:
        (xr.DataArray): dataarray with missing years filled in.
    """
    year_coords = xr.DataArray([np.nan] * len(missing_years), [(year_dim, missing_years)])
    mean_vals = da.mean(year_dim)  # NOTE just fill with mean values
    fill_da = mean_vals.combine_first(year_coords)
    da = da.combine_first(fill_da)
    return da.sortby(year_dim)


def _expand_point_dim_to_all(
    da: xr.DataArray, gbd_round_id: int, years: YearRange, dim: str
) -> xr.DataArray:
    """Expand point dim to full set of coordinates.

    Sometimes input data has only age_group_id = 22 (all ages), or sex_id = 3 (both sexes).
    In that case, we'll need to convert it to the full age group dim, or sex_id = 1,2.  Such
    information is stored in self._expected_dims

    Args:
        da (xr.DataArray): da to expand.
        gbd_round_id (int): gbd round id.
        years (YearRange): [past_start, forecast_start, forecast_end] years.
        dim (str): Point dimension of da to expand on.  Typically this occurs with
            age_group_id = 2 (all ages) or sex_id = 3.

    Returns:
        (xr.Dataarray): has the dim coordinates prescribed by self.gbd_round_id
    """
    if dim not in da.dims:
        raise ValueError("{} is not an index dimension".format(dim))
    if len(da[dim]) != 1:
        raise ValueError("Dim {} length not equal to 1".format(dim))

    # first I need the proper coordinates
    coords = demographic_coords(gbd_round_id, years)[dim]
    # then make keyword argument dict
    dimension_kwarg = {dim: coords}

    # expand
    out = expand_dimensions(da.drop_vars(dim).squeeze(), **dimension_kwarg)

    return out


def data_value_check(da: xr.DataArray) -> None:
    """Check sensibility of dataarray values.

    Currently, these are checked:

    1.) No NaN/Inf
    2.) No negative values

    Args:
        da (xr.DataArray): data in dataarray format.
    """
    if not np.isfinite(da).all():
        raise ValueError("Array contains non-finite values")
    if not (da >= 0.0).all():
        raise ValueError("Array contains values < 0")


def save_paf(
    paf: Union[xr.DataArray, xr.Dataset],
    gbd_round_id: int,
    past_or_future: str,
    version: str,
    acause: str,
    cluster_risk: Optional[str] = None,
    file_suffix: str = "",
    metric: str = DimensionConstants.PERCENT_METRIC,
    space: str = DimensionConstants.IDENTITY_SPACE_NAME,
    **kwargs: Any,
) -> None:
    """Save mediated PAF at cause level.

    Args:
        paf (Union[xr.DataArray, xr.Dataset]): DataArray or Dataset of PAF.
        gbd_round_id (int): gbd round id.
        past_or_future (str): 'past' or 'future'.
        version (str): version, dated.
        acause (str): analytical cause.
        cluster_risk (Optional[str]): If none, it will be just risk.
        file_suffix (str): File name suffix to attach onto output file name, should include a
            leading underscore if desired.  Default is an empty string.
        metric (str): Metric name of the output xarray, defaults to
            `DimensionConstants.PERCENT_METRIC`
        space (str): Space name of the output xarray, defaults to
            `DimensionConstants.IDENTITY_SPACE_NAME`
        kwargs (Any): Additional keyword arguments to pass into save_xr call
    """
    if past_or_future == "past":
        if DimensionConstants.SCENARIO in paf.dims:
            # past data should be scenarioless; all its scenarios are identical, so we can just
            # grab the first one
            paf = paf.sel(scenario=paf["scenario"].values[0], drop=True)

    file_attributes = kwargs
    version_metadata = VersionMetadata.make(
        data_source=gbd_round_id,
        epoch=past_or_future,
        stage="paf",
        version=version,
    )

    if cluster_risk:
        output_file_spec = FHSFileSpec(
            version_metadata=version_metadata,
            sub_path=("risk_acause_specific",),
            filename=f"{acause}_{cluster_risk}{file_suffix}.nc",
        )
        file_attributes = dict(file_attributes, risk=cluster_risk)
    else:
        output_file_spec = FHSFileSpec(
            version_metadata=version_metadata,
            filename=f"{acause}{file_suffix}.nc",
        )

    save_xr_scenario(
        xr_obj=paf,
        file_spec=output_file_spec,
        metric=metric,
        space=space,
        acause=acause,
        version=version,
        gbd_round_id=gbd_round_id,
        **file_attributes,
    )

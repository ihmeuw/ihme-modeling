r"""For each cause, compute a daly from the sum of yld and yll.

.. math::

    \text{DALY} = \text{YLD} + \text{YLL}

Parallelized by cause.

Example call for future only:

.. code:: bash

    fhs_pipeline_dalys_console parallelize-by-cause \
        --gbd-round-id 89 \
        --versions FILEPATH \
        -v FILEPATH \
        --draws 500 \
        --years 1776:1864:1927 \

Example call for future & past:

.. code:: bash

    fhs_pipeline_dalys_console parallelize-by-cause \
        --gbd-round-id 89 \
        --versions FILEPATH \
        -v FILEPATH \
        --draws 500 \
        --years 1776:1864:1927 \
        --past include
"""  # noqa: D208
from typing import Tuple, Union

import xarray as xr
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_data_transformation.lib.validate import assert_coords_same
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec
from fhs_lib_file_interface.lib.versioning import Versions
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario, save_xr_scenario
from fhs_lib_year_range_manager.lib import YearRange
from tiny_structured_logger.lib import fhs_logging

logger = fhs_logging.get_logger()


def fill_missing_coords(
    yld_da: xr.DataArray, yll_da: xr.DataArray, dim: str
) -> Tuple[xr.DataArray, xr.DataArray]:
    """Check if there are missing coordinates between two data arrays.

    Fills any missing coordinates in the respective DAs with 0.

    Args:
        yld_da (xr.DataArray): YLD data to check for missing coords.
        yll_da (xr.DataArray): YLL data to check for missing coords.
        dim (str): Dimension to check for missing coords.

    Returns:
        Tuple(xr.DataArray, xr.DataArray): yld_da and yll_da with missing coords filled with 0
    """
    yld_coord_values = tuple(yld_da[dim].values)
    yll_coord_values = tuple(yll_da[dim].values)
    yll_missing_coords = list(set(yld_coord_values) - set(yll_coord_values))
    yld_missing_coords = list(set(yll_coord_values) - set(yld_coord_values))

    if yll_missing_coords:
        logger.warning(f"{dim}:{yll_missing_coords} are missing from YLLs")
        yll_da = expand_dimensions(yll_da, **{dim: yll_missing_coords}, fill_value=0)

    if yld_missing_coords:
        logger.warning(f"{dim}:{yld_missing_coords} are missing from YLDs")
        yld_da = expand_dimensions(yld_da, **{dim: yld_missing_coords}, fill_value=0)

    return yld_da, yll_da


def _get_years_in_slice(years: YearRange, past_or_future: str) -> YearRange:
    if past_or_future == "future":
        years_in_slice = years.forecast_years
    elif past_or_future == "past":
        years_in_slice = years.past_years
    else:
        raise RuntimeError("past_or_future must be `past` or `future`")

    return years_in_slice


def _read_and_resample_stage(
    stage: str,
    versions: Versions,
    gbd_round_id: int,
    draws: int,
    years: YearRange,
    past_or_future: str,
    acause: str,
) -> Union[xr.DataArray, int]:
    years_in_slice = _get_years_in_slice(years, past_or_future)

    logger.info(f"acause: {acause} years: {years_in_slice}")

    stage_file_metadata = versions.get(past_or_future, stage).default_data_source(gbd_round_id)

    try:
        da = open_xr_scenario(
            file_spec=FHSFileSpec(
                version_metadata=stage_file_metadata, filename=f"{acause}.nc"
            )
        ).sel(year_id=years_in_slice)

        da = resample(da, draws)
    except OSError:
        logger.warning("{} does not have YLDs".format(acause))
        da = 0
    return da


def one_cause_main(
    versions: Versions,
    gbd_round_id: int,
    draws: int,
    years: YearRange,
    past_or_future: str,
    acause: str,
) -> None:
    """Compute a daly from the yld and yll at the cause level.

    Args:
        versions (Versions): A Versions object that keeps track of all the versions and their
            respective data directories.
        gbd_round_id (int): What gbd_round_id that yld, yll and daly are saved under.
        draws (int): How many draws to save for the daly output.
        years (str): years for calculation. Will use either the past or future portion of the
            year range depending on the value of past_or_future.
        past_or_future (str): whether calculating past or future values. Must be "past" or
            "future".
        acause (str): cause to calculate dalys for.

    Raises:
        RuntimeError: if `past_or_future` is not "past" or "future"
        ValueError: if the `daly` DA doesn't have YLLs or YLDs
    """
    logger.info("Entering `one_cause_main` function.")

    yld = _read_and_resample_stage(
        "yld", versions, gbd_round_id, draws, years, past_or_future, acause
    )
    yll = _read_and_resample_stage(
        "yll", versions, gbd_round_id, draws, years, past_or_future, acause
    )

    if isinstance(yld, xr.DataArray) and "acause" not in yld.dims:  # type: ignore
        yld = yld.expand_dims(acause=[acause])  # type: ignore
    if isinstance(yll, xr.DataArray) and "acause" not in yll.dims:  # type: ignore
        yll = yll.expand_dims(acause=[acause])  # type: ignore

    if isinstance(yll, xr.DataArray) and isinstance(yld, xr.DataArray):
        yld, yll = fill_missing_coords(yld, yll, dim="age_group_id")
        yld, yll = fill_missing_coords(yld, yll, dim="sex_id")
        yld, yll = fill_missing_coords(yld, yll, dim="location_id")

        assert_coords_same(yld, yll)

    daly = yld + yll

    if not isinstance(daly, xr.DataArray):
        err_msg = f"{acause} is missing both YLDs and YLLs"
        logger.error(err_msg)
        raise ValueError(err_msg)

    daly_file_metadata = versions.get(past_or_future, "daly").default_data_source(gbd_round_id)

    save_xr_scenario(
        xr_obj=daly,
        file_spec=FHSFileSpec(version_metadata=daly_file_metadata, filename=f"{acause}.nc"),
        metric="rate",
        space="identity",
    )

    logger.info("Leaving `one_cause_main` function. DONE")
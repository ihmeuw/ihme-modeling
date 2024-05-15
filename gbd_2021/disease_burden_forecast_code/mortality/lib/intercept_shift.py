from typing import Callable, List, Union

import xarray as xr
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec, VersionMetadata
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib import fhs_logging

DEMOGRAPHIC_DIMS = ["age_group_id", "sex_id", "location_id"]

logger = fhs_logging.get_logger()


def open_xr_as_dataarray(file: FHSFileSpec, data_var: str = "value") -> xr.DataArray:
    """Open an xarray file and return a DataArray (not Dataset): filter to data_var."""
    data = open_xr_scenario(file)
    if isinstance(data, xr.Dataset):
        data = data[data_var]
    return data


def select_coords_by_dataarray(
    data: xr.DataArray, selective_da: xr.DataArray, dims: List[str]
) -> xr.DataArray:
    """Filter some dims of data so that it has the same coords as selective_da.

    Only affects the dims mentioned in "dims". On these dims, the `data` array will have its
    coordinates narrowed so that they match the same dim in selective_da.
    """
    selector = {dim: selective_da[dim].values for dim in dims}
    try:
        return data.sel(**selector)
    except KeyError as err:
        raise IndexError(err)


def intercept_shift_draws(
    preds: xr.DataArray,
    acause: str,
    past_version: Union[str, VersionMetadata],
    gbd_round_id: int,
    years: YearRange,
    draws: int,
    shift_function: Callable,
) -> xr.DataArray:
    """Load past data and use it to apply an intercept shift to preds at the draw level.

    Args:
        preds (xr.DataArray): The raw predictions to intercept shift.
        acause (str): The short name of the cause to intercept shift predictions of.
        past_version (str): The "past", i.e., GBD, data for past years to
        gbd_round_id (int): The numeric ID of the relevant GBD round.
        years (YearRange): The forecasting time series year range.
        draws (int): The number of draws to resample both GBD data and raw predictions
        shift_function (Callable): The function for actually executing the intercept shift,
            must take 3 arguments: 1) ``preds``, 2) ``past_data``, which is read in based on
            ``past_version`` and other parameters, where ``preds`` and ``past_data`` have
            their draws resampled to the same number of draws: ``draws``, 3) ``years.past_end``

    Notes:
        Preconditions: Expects past data to *include* the same coordinate dimensions for age,
            sex, and location, but for year  they should have one overlapping year. GBD-past
            and modeled-past will be resampled to the given number of draws.

    Returns:
        xr.DataArray: The intercept-shifted predictions.

    Raises:
        IndexError: If coordinates do not match up between modeled data and GBD data.
    """
    modeled_data = resample(preds, draws)

    if isinstance(past_version, VersionMetadata):
        # The next line is a hint to the type checker
        past_version = past_version  # type: VersionMetadata
    else:
        past_version = VersionMetadata.make(
            data_source=gbd_round_id,
            epoch="past",
            stage="death",
            version=past_version,
            root_dir="int",
        )

    past_file = FHSFileSpec(past_version, f"{acause}_hat.nc")

    raw_past_data = open_xr_as_dataarray(past_file)

    # Align DataArrays by throwing away known unimportant dimensions.
    modeled_data = eliminate_dims(modeled_data, ["acause", "rei", "rei_id", "cause_id"])
    raw_past_data = eliminate_dims(raw_past_data, ["acause", "rei_id", "cause_id"])

    # Some causes have more age groups/sex groups in the past data.
    # Make sure past data have the same coordinates as the modeled data.
    raw_past_data = select_coords_by_dataarray(raw_past_data, modeled_data, DEMOGRAPHIC_DIMS)
    past_data = resample(raw_past_data, draws)

    shifted = shift_function(
        modeled_data=modeled_data,
        past_data=past_data,
        past_end_year_id=years.past_end,
    )

    return shifted


def eliminate_dims(data: xr.DataArray, dims_to_eliminate: List[str]) -> xr.DataArray:
    """Drop dims and coords of data, for each named dim."""
    for dim in dims_to_eliminate:
        data = eliminate_dim(data, dim)
    return data


def eliminate_dim(data: xr.DataArray, dim: str) -> xr.DataArray:
    """Drop the given dim and its coords, provided it is single-valued.

    Drops whatever is present, and doesn't crash if the dim is missing, or the coord is
    missing, or both. Crashes if dim is present with more than one coord.

    Handles cases where the dim is present as a "point-coord" or as an ordinary dimensional
    coord with a single value.
    """
    if dim in data.dims:
        return data.squeeze(dim, drop=True)
    if dim in data.coords:
        return data.drop_vars(dim)
    return data

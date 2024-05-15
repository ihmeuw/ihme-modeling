from typing import Iterable, Optional

import numpy as np
import pandas as pd
import xarray as xr
from fhs_lib_database_interface.lib.constants import DimensionConstants
from fhs_lib_file_interface.lib.pandas_wrapper import write_csv
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec
from fhs_lib_file_interface.lib.versioning import Versions

from fhs_lib_genem.lib.constants import FileSystemConstants

OMEGA_DIM = "omega"


def get_omega_weights(min: float, max: float, step: float) -> Iterable[float]:
    """Return the list of weights between ``min`` and ``max``, incrementing by ``step``."""
    return np.arange(min, max, step)


def root_mean_square_error(
    predicted: xr.DataArray,
    observed: xr.DataArray,
    dims: Optional[Iterable[str]] = None,
) -> xr.DataArray:
    """Dimensions-specific root-mean-square-error.

    Args:
        predicted (xr.DataArray): predicted values.
        observed (xr.DataArray): observed values.
        dims (Optional[Iterable[str]]): list of dims to compute rms for.

    Returns:
        (xr.DataArray): root-mean-square error, dims-specific.
    """
    dims = dims or []

    squared_error = (predicted - observed) ** 2
    other_dims = [d for d in squared_error.dims if d not in dims]
    return np.sqrt(squared_error.mean(dim=other_dims))


def calculate_predictive_validity(
    forecast: xr.DataArray,
    holdouts: xr.DataArray,
    omega: float,
) -> xr.DataArray:
    """Calculate the RMSE between ``forecast`` and ``holdouts`` across location & sex."""
    # Take the mean over draw if forecast or holdouts data has them
    if DimensionConstants.DRAW in forecast.dims:
        forecast_mean = forecast.mean(DimensionConstants.DRAW)
    else:
        forecast_mean = forecast

    if DimensionConstants.DRAW in holdouts.dims:
        holdouts_mean = holdouts.mean(DimensionConstants.DRAW)
    else:
        holdouts_mean = holdouts

    # Calculate RMSE
    pv_data = root_mean_square_error(
        predicted=forecast_mean.sel(scenario=0, drop=True),
        observed=holdouts_mean,
        dims=[DimensionConstants.LOCATION_ID, DimensionConstants.SEX_ID],
    )

    # Tag the data with a hard-coded attribute & return it
    pv_data[OMEGA_DIM] = omega
    return pv_data


def finalize_pv_data(pv_list: Iterable[xr.DataArray], entity: str) -> pd.DataFrame:
    """Convert a list of PV xarrays into a pandas dataframe, and take the mean over sexes."""
    pv_xr = xr.concat(pv_list, dim=OMEGA_DIM)

    # mean over sexes (if it's present)
    if DimensionConstants.SEX_ID in pv_xr.dims:
        pv_xr = pv_xr.mean([DimensionConstants.SEX_ID])

    pv_xr["entity"] = entity
    return pv_xr.to_dataframe(name="rmse").reset_index()


def save_predictive_validity(
    file_name: str,
    gbd_round_id: int,
    model_name: str,
    pv_df: pd.DataFrame,
    stage: str,
    subfolder: str,
    versions: Versions,
) -> None:
    """Write a predictive validity dataframe to disk."""
    # Define the output file spec
    pv_file_spec = FHSFileSpec(
        version_metadata=versions.get(past_or_future="future", stage=stage),
        sub_path=(
            FileSystemConstants.PV_FOLDER,
            model_name,
            subfolder,
        ),
        filename=f"{file_name}_pv.csv",
    )

    # Write the dataframe (note that the pv output directory is "{out_version}_pv")
    write_csv(df=pv_df, file_spec=pv_file_spec, sep=",", na_rep=".", index=False)

"""Calculates Life Expectancy.
"""

import gc
from typing import Callable, List, Optional, Tuple

import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_database_interface.lib.constants import DimensionConstants
# This refers to the lexmodel file that is also included in this directory,
# which also includes its own set of imports
from fhs_lib_demographic_calculation.lib import lexmodel as model
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec, VersionMetadata
from fhs_lib_file_interface.lib.versioning import validate_versions_scenarios
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario, save_xr_scenario
from tiny_structured_logger.lib.fhs_logging import get_logger

logger = get_logger()

MODELS = [mxx for mxx in dir(model) if mxx.startswith("gbd")]


def main(
    gbd_round_id: int,
    draws: int,
    input_version: str,
    past_or_future: str,
    output_scenario: int | None,
    output_version: str,
    lx_function: Callable,
    chunk_size: int = 100,
    suffix: Optional[str] = None,
) -> None:
    """Entry point for lex for outside calls.

    If ``draw`` dim exists, chunk over draws.  Else if ``scenario`` exists,
    chunk over scenarios.

    Args:
        gbd_round_id (int): GBD Round used for input and output versions.
        draws (int): number of draws to keep, if draw dimension exists.
        input_version (str): The version of input to use. Include the :scenario_id notation to
            run in single-scenario mode
        past_or_future (str): "past" or "future".
        output_scenario (int | None): Scenario ID to use for output.
        output_version (str): Name to create the output data under. Include the :scenario_id
            notation to run in single-scenario mode.
        lx_function (Callable): The function to call to calculate life expectancy. Should be
            one of the models, all of which are model.gbd* in model.py.
        chunk_size (int): number of draw chunks to work on within each iteration.
        suffix (Optional[str]): If not None, include this suffix as part of each filename

    Raises:
        ValueError: If the mx data has negatives or infinite values.
    """
    input_version_metadata: VersionMetadata = (
        VersionMetadata.parse_version(input_version, default_stage="death")
        .default_data_source(gbd_round_id)
        .with_epoch(past_or_future)
    )
    output_version_metadata: VersionMetadata = (
        VersionMetadata.parse_version(output_version, default_stage="life_expectancy")
        .default_data_source(gbd_round_id)
        .with_epoch(past_or_future)
    )

    validate_versions_scenarios(
        versions=[input_version_metadata, output_version_metadata],
        output_scenario=output_scenario,
        output_epoch_stages=[(past_or_future, "death"), (past_or_future, "life_expectancy")],
    )

    mx_file_spec = FHSFileSpec(version_metadata=input_version_metadata, filename="FILEPATH")
    mx = open_xr_scenario(mx_file_spec)

    if type(mx) == xr.Dataset:
        logger.info(f"Input mx to lex code is a {xr.Dataset}")
        mx = mx[DimensionConstants.VALUE]

    if float(mx.min()) < 0:
        raise ValueError("Negatives in mx")

    if ~np.isfinite(mx).all():
        raise ValueError("Non-finites in mx")

    mx_no_point, point_coords = model.without_point_coordinates(mx)

    del mx
    gc.collect()

    # Because we operate across age group id in all the calculations so
    # this will be much faster.
    reordered = list(mx_no_point.dims)
    reordered.remove(DimensionConstants.AGE_GROUP_ID)
    reordered.append(DimensionConstants.AGE_GROUP_ID)
    mx_no_point = mx_no_point.transpose(*reordered)

    mx_no_point, chunk_dim = _set_chunk_dim(mx_no_point, draws)

    if chunk_dim is None:
        ds = lx_function(mx_no_point)
    else:  # compute over chunks along either draw or scenario dim
        dim_size = len(mx_no_point[chunk_dim])
        logger.info(f"Chunking over {chunk_dim} dim over {dim_size} coords")

        chunk_da_list: List[xr.DataArray] = []
        for start_idx in range(0, dim_size, chunk_size):
            end_idx = (
                start_idx + chunk_size if start_idx + chunk_size <= dim_size else dim_size
            )
            mx_small = mx_no_point.sel(
                {chunk_dim: mx_no_point[chunk_dim].values[start_idx:end_idx]}
            )
            ds_small = lx_function(mx_small)
            chunk_da_list.append(ds_small)

        # Concatenate all the small dataarrays
        ds = xr.concat(chunk_da_list, dim=chunk_dim)

    del mx_no_point
    gc.collect()

    ds_point = ds.assign_coords(**point_coords)

    del ds
    gc.collect()

    suffix = f"_{suffix}" if suffix else ""
    lex_file = FHSFileSpec(
        version_metadata=output_version_metadata, filename=f"FILEPATH"
    )
    save_xr_scenario(
        ds_point.ex,
        lex_file,
        metric="number",
        space="identity",
        mx_source=str(mx_file_spec),
        model=str(lx_function.__name__),
    )

    dataset_file = FHSFileSpec(
        version_metadata=output_version_metadata, filename=f"FILEPATH"
    )

    # ds contains mx, ax, lx, nLx, and ex.
    save_xr_scenario(
        ds_point,
        dataset_file,
        metric="number",
        space="identity",
        mx_source=str(mx_file_spec),
        model=str(lx_function.__name__),
    )

    logger.info(f"wrote {dataset_file}")


def _set_chunk_dim(da: xr.DataArray, draws: int) -> Tuple[xr.DataArray, Optional[str]]:
    """Set chunk dimension.

    If "draw" in da.dims, resample and chunk along draws.
    Otherwise, set chunk_dim to be "scenario".
    If neither "draw" nor "scenario" exists, then there's no dim to chunk over.

    Args:
        da (xr.DataArray): input data array.
        draws (int): number of draws to resample.

    Returns:
        Tuple[xr.DataArray, str]:
            xr.DataArray: either resampled input da, or just the input da.
            str: dimension to chunk over.  Could be None.
    """
    if DimensionConstants.DRAW in da.dims:  # if draw dim exists, chunk over draws
        da = resample(da, draws)
        chunk_dim = DimensionConstants.DRAW
    elif DimensionConstants.SCENARIO in da.dims:
        chunk_dim = DimensionConstants.SCENARIO
    else:
        chunk_dim = None

    return da, chunk_dim
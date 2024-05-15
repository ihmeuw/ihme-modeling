"""This module contains utilities for preparing the model input.
"""

import gc

import xarray as xr
from fhs_lib_database_interface.lib.constants import DimensionConstants, ScenarioConstants
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_lib_model.lib.constants import GKModelConstants

logger = get_logger()


def data_var_merge(dataset: xr.Dataset, collapse: bool = False) -> xr.DataArray:
    """Merges the data variables of the given dataset into one dataarray.

    Adds dims to each data variable of the dataset such that they are all consistent in
    shape, i.e., dimensions and coordinates. Also adds a new single-coordinate dim
    ``"cov"``, i.e., "covariate" to each variable, where the coordinate is the name of the
    variable. Then all of the data variables are concatenated over this new dimension, such
    that each one corresponds to a coordinate on the ``"cov"`` dimension.

    Args:
        dataset (xr.Dataset): The dataset to convert into a dataarray.
        collapse (bool): Whether to collapse the dataarray into a model-able array.

    Returns:
        xr.DataArray: The dataarray containing each data variable of the original dataset
            as a dimension.
    """
    final_das = []
    exclude_dims = [DimensionConstants.REGION_ID, DimensionConstants.SUPER_REGION_ID]
    col_vars = [v for v in dataset.data_vars.keys() if v not in exclude_dims]

    for var in col_vars:
        logger.debug("Adding dims to data var", bindings=dict(var=var))

        ex_da = dataset[var].copy()
        das_to_expand_by = list(dataset.data_vars.keys())
        das_to_expand_by.remove(var)
        for oda_key in das_to_expand_by:
            ex_da, _ = xr.broadcast(ex_da, dataset[oda_key].copy())

        ex_da = ex_da.expand_dims({GKModelConstants.COVARIATE_DIM: [ex_da.name]})

        final_das.append(ex_da)

    del ex_da
    gc.collect()

    final_array: xr.DataArray = xr.concat(final_das, dim="cov")
    del final_das
    gc.collect()

    if collapse:
        logger.debug("Collapsing dataarray into model-able array")
        final_array = _modable_array(final_array)

    return final_array


def _modable_array(dataarray: xr.DataArray) -> xr.DataArray:
    """Collapse an array to its model-able dimensions.

    Collapse an array to its model-able dimensions collapsing on the draw dimnesion (i.e.,
    taking the mean of the draws) and any other dimensions that aren't in the designated
    model-able dimensions, defined in ``GKModelConstants.MODELABLE_DIMS``. If the scenario
    dimension exists, then only use the reference/default scenario.

    Args:
        dataarray (xr.DataArray): The array to collapse into a "model-lable" array as
            required by the GK Modeling interface.

    Returns:
        xr.DataArray: The collapsed model-able array.
    """
    # Only keep the reference scenario if scenario is a dimension.
    if DimensionConstants.SCENARIO in dataarray.dims:
        scenario_slice = {
            DimensionConstants.SCENARIO: ScenarioConstants.REFERENCE_SCENARIO_COORD
        }
        logger.debug("Slicing to reference scenario", bindings=scenario_slice)
        dataarray = dataarray.sel(**scenario_slice)

    # Take the mean across all coordinates of any dimension that isn't a designated
    # model-able dimension.
    for dim in dataarray.dims:
        if dim not in GKModelConstants.MODELABLE_DIMS:
            logger.debug("Collapsing non-modelable dim into mean", bindings=dict(dim=dim))
            dataarray = dataarray.mean(dim)

    # Reorder the dimensions of the collapsed array so they conform to the order as they
    # appear in the list of designated model-able dimensions.
    reorder = [d for d in GKModelConstants.MODELABLE_DIMS if d in dataarray.dims]
    dataarray = dataarray.transpose(*reorder)
    return dataarray

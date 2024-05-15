"""This module contains utilities for processing the model output into a more usable form.
"""

from typing import Any, Dict, List, Union

import numpy as np
import xarray as xr
from fhs_lib_database_interface.lib.constants import DimensionConstants
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_lib_model.lib.constants import GKModelConstants

logger = get_logger()


def region_to_location(array: np.ndarray, region_map: List[int]) -> np.ndarray:
    """Converts a region axis of an array into an axis of detailed locations.

    Given an array of coefficient values  and regions transforms the array from
    region-age-time-covariate-draw to location-age-time-covariate-draw.

    Args:
        array (np.ndarray): 5D array with first axis to be converted from regions to detailed
            locations.
        region_map (List[int]): A list of region indices -- one index for each location on the
            new location axis.

    Returns:
        np.ndarray: 5d array with axis for locations instead of for regions.
    """
    _, age_dim_size, time_dim_size, covariate_dim_size, draw_dim_size = array.shape
    location_dim_size = len(region_map)

    # Initialize an empty array with the desired shape.
    location_array = np.zeros(
        (
            location_dim_size,
            age_dim_size,
            time_dim_size,
            covariate_dim_size,
            draw_dim_size,
        )
    )

    # For each location, assign the coefficient to it that corresponds to the region it is in.
    for i in range(len(region_map)):
        location_array[i, :, :, :, :] = array[region_map[i], :, :, :, :]

    return location_array


def transform_parameter_array(array: np.ndarray, constraints: np.ndarray) -> np.ndarray:
    """Transforms an array of coefficients based on the corresponding parameter's constraints.

    Applies the associated constraints to the coefficients of each covariate for the given
    parameter.

    Args:
        array (np.ndarray): The mutli-dimensional Numpy array of parameter coefficients to
            transform. Must have the following 5 dimensions in that order: location, age, time
            (i.e., years), covariate, and draw.
        constraints (np.array): Array of integers with value 0, -1, or 1 corresponding to the
            constraint of each covariate.

    Returns:
        np.ndarray: The 5-dimensional array of coefficients that have been transformed
            according to the given constraints.
    """
    arr_trans = np.copy(array)
    for i in range(len(constraints)):
        if constraints[i] == -1:
            arr_trans[:, :, :, i, :] = -1 * np.exp(arr_trans[:, :, :, i, :])
        if constraints[i] == 1:
            arr_trans[:, :, :, i, :] = np.exp(arr_trans[:, :, :, i, :])

    return arr_trans


def np_to_xr(
    array: np.ndarray,
    demog_coords: Dict[str, Union[int, str]],
    covariates: List[str],
) -> xr.DataArray:
    """Convert a numpy array from TMB to an xarray for use in forecasting.

    Any single coordinate dimensions will be removed.

    Args:
        array (np.ndarray): The Numpy multi-dimension array to convert into an Xarray
            DataArray.
        demog_coords (Dict[str, Union[int, str]]): Mapping of demographic dimensions to their
            corresponding coordinates.
        covariates: coordinates for covariate dim.

    Returns:
        xr.DataArray: The model output that has been converted to Xarray DataArray form.
    """
    all_coords: Dict[str, Any] = demog_coords.copy()
    all_coords[GKModelConstants.COVARIATE_DIM] = covariates

    drop_axes = [x for x in range(3) if array.shape[x] == 1]
    drop_dims = [GKModelConstants.OUTPUT_DIMS[x] for x in range(3) if array.shape[x] == 1]
    dims = [
        GKModelConstants.OUTPUT_DIMS[i]
        for i in range(len(GKModelConstants.OUTPUT_DIMS))
        if i not in drop_axes
    ]

    for dim in drop_dims:
        all_coords.pop(dim)

    all_coords[DimensionConstants.DRAW] = np.arange(array.shape[-1])

    squeezed_array = array.copy()
    if len(drop_axes):
        logger.debug(
            "Dropping dimensions from output",
            bindings=dict(drop_dims=drop_dims, drop_axes=drop_axes),
        )
        squeezed_array = squeezed_array.mean(tuple(drop_axes))

    return xr.DataArray(squeezed_array, dims=dims, coords=all_coords)

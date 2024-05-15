"""Functions related to validating inputs, and outputs of nonfatal pipeline."""

from typing import List

import xarray as xr
from tiny_structured_logger.lib.fhs_logging import get_logger

logger = get_logger()


def assert_covariates_scenarios(cov_data_list: List[xr.DataArray]) -> None:
    """Check that all covariates have the same scenario coordinates.

    Args:
        cov_data_list (list[xr.DataArray]): Past and forecast data for each covariate, i.e.
            independent variable.

    Raises:
        ValueError: If the covariates do not have consistent scenario coords.
    """
    first_cov = cov_data_list[0]

    if "scenario" in first_cov.dims:
        first_scenarios = set(first_cov["scenario"].values)
        for next_cov in cov_data_list[1:]:
            next_scenarios = set(next_cov["scenario"].values)
            if first_scenarios.symmetric_difference(next_scenarios):
                raise ValueError(
                    f"Covariates have inconsistent scenario coords, e.g. "
                    f"{first_cov.name} and {next_cov.name}"
                )

    else:
        for next_cov in cov_data_list[1:]:
            if "scenario" in next_cov.dims:
                raise ValueError(
                    f"{first_cov.name} doesn't have a scenario dimension, but {next_cov.name} "
                    "does. If any covariates have a scenario, they all need to."
                )

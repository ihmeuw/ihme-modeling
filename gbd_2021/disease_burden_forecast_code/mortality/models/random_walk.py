"""This module contains the Random Walk model.
"""

from typing import Optional, Union

import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib.dimension_transformation import (
    expand_dimensions,
    remove_dims,
)
from fhs_lib_data_transformation.lib.processing import strip_single_coord_dims
from fhs_lib_database_interface.lib.constants import DimensionConstants
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_lib_model.lib.constants import ModelConstants

logger = get_logger()


class RandomWalk:
    """Runs an independent random walk model for every time series in the dataset."""

    def __init__(
        self,
        dataset: Union[xr.DataArray, xr.Dataset],
        years: YearRange,
        draws: int,
        y: str = ModelConstants.DEFAULT_DEPENDENT_VAR,
        seed: int | None = None,
    ) -> None:
        """Initialize arguments and set state.

        Args:
            dataset: (xr.Dataset | xr.DataArray): Data to forecast. If ``xr.Dataset`` then
                ``y`` paramter must be specified as the variable to forecast.
            years (YearRange): The forecasting time series.
            draws (int): Number of draws to make for predictions.
            y (str): If using a ``xr.Dataset`` the dependent variable to apply analysis to.
            seed (Optional[int]): An optional seed to set for numpy's random number generation
        """
        self.rng = np.random.default_rng(seed=seed)

        self.y = y
        self.years = years
        self.draws = draws
        self._init_dataset(dataset)

        # Declare the sigma instance field that will hold the model fit values.
        self.sigma: Optional[xr.DataArray] = None

    def fit(self) -> None:
        """Fit the model.

        Here, fitting the model corrsponds to calculating the standard deviation of the normal
        distribution used to generate the random walk.
        """
        diff = self.dataset[self.y].diff(DimensionConstants.YEAR_ID)
        self.sigma = diff.std(DimensionConstants.YEAR_ID)

    def predict(self) -> xr.DataArray:
        """Generate predictions based on model fit.

        Produces projections into future years using the model fit.

        Notes:
            * Must be run after the ``RandomWalk.fit()`` method.

        Returns:
            xr.DataArray: Contains past data and future projections.
        """
        location_ids = self.dataset[self.y].location_id.values
        age_group_ids = self.dataset[self.y].age_group_id.values
        sex_ids = self.dataset[self.y].sex_id.values
        location_forecast_list = []
        for location_id in location_ids:
            age_forecast_list = []
            for age_group_id in age_group_ids:
                sex_forecast_list = []
                for sex_id in sex_ids:
                    sex_forecast = self._predict_demog_slice(
                        location_id=location_id,
                        age_group_id=age_group_id,
                        sex_id=sex_id,
                    )
                    sex_forecast_list.append(sex_forecast)
                age_forecast_list.append(
                    xr.concat(sex_forecast_list, dim=DimensionConstants.SEX_ID)
                )
            location_forecast_list.append(
                xr.concat(age_forecast_list, dim=DimensionConstants.AGE_GROUP_ID)
            )
        forecast = xr.concat(location_forecast_list, dim=DimensionConstants.LOCATION_ID)
        past = self.dataset.y

        past = expand_dimensions(past, draw=range(self.draws))
        past = strip_single_coord_dims(past)

        full_time_series: xr.DataArray = xr.concat(
            [past, forecast], dim=DimensionConstants.YEAR_ID
        )

        return full_time_series

    def _init_dataset(self, dataset: Union[xr.DataArray, xr.Dataset]) -> None:
        """Initialize the dataset of observed/past data."""
        if isinstance(dataset, xr.DataArray):
            logger.debug("DataArray given, converting to Dataset")
            self.dataset = xr.Dataset({self.y: dataset})
        else:
            self.dataset = dataset

        if self.y not in list(self.dataset.data_vars.keys()):
            err_msg = "``y`` was not found in the input dataset"
            logger.error(err_msg)
            raise ValueError(err_msg)

        expected_dims = [
            DimensionConstants.YEAR_ID,
            DimensionConstants.LOCATION_ID,
            DimensionConstants.AGE_GROUP_ID,
            DimensionConstants.SEX_ID,
        ]
        for dim in self.dataset.dims:
            if dim not in expected_dims:
                self.dataset = remove_dims(xr_obj=self.dataset, dims_to_remove=[str(dim)])

    def _predict_demog_slice(
        self, location_id: int, age_group_id: int, sex_id: int
    ) -> xr.DataArray:
        """Generate predictions based on model fit on a single demographic slice."""
        demographic_slice_coords = {
            DimensionConstants.LOCATION_ID: location_id,
            DimensionConstants.AGE_GROUP_ID: age_group_id,
            DimensionConstants.SEX_ID: sex_id,
        }

        past_slice = self.dataset.loc[demographic_slice_coords][self.y].values
        if len(past_slice.shape) != 1:
            err_msg = "Demographic slice must be 1-Dimensional (just year/time)"
            logger.error(err_msg)
            raise RuntimeError(err_msg)

        sigma_slice = self.sigma.loc[demographic_slice_coords].values

        distribution = self.rng.normal(
            loc=0,
            scale=sigma_slice,
            size=(self.draws, self.years.forecast_end - self.years.past_end),
        )
        forecast_slice = past_slice[-1] + np.cumsum(distribution, axis=1)

        forecast_slice_dataarray = xr.DataArray(
            forecast_slice,
            coords=[list(range(self.draws)), self.years.forecast_years],
            dims=[DimensionConstants.DRAW, DimensionConstants.YEAR_ID],
        )
        forecast_slice_dataarray.coords[DimensionConstants.LOCATION_ID] = location_id
        forecast_slice_dataarray.coords[DimensionConstants.AGE_GROUP_ID] = age_group_id
        forecast_slice_dataarray.coords[DimensionConstants.SEX_ID] = sex_id

        return forecast_slice_dataarray

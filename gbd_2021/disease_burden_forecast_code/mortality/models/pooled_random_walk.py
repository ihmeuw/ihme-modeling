"""This module contains tools creating a collection of correlated Random Walk projections.
"""

import itertools as it
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_database_interface.lib.constants import DimensionConstants
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_lib_model.lib.constants import ModelConstants, PooledRandomWalkConstants
from fhs_lib_model.lib.random_walk.random_walk import RandomWalk

logger = get_logger()


class PooledRandomWalk(RandomWalk):
    """Creates a collection of correlated Random Walk projections.

    Runs a random walk model on the data while pulling strength across multiple time series.
    Within the model structure argument define a key dims which defines which dimensions to use
    specific ar parameter values for as a list. An empty list, which is the default will use
    the same parameters for all time series. The time dimension should always be ``"year_id"``.
    If you specify ``"region_id"`` or ``"super_region_id"`` the dataset parameter must be an
    ``xr.Dataset`` with the modeled value as ``"y"`` and another key with ``"region_id"`` or
    `"super_region_id"`` which has a ``"location_id"`` dimension and a value for each location.
    """

    def __init__(
        self,
        dataset: Union[xr.DataArray, xr.Dataset],
        years: YearRange,
        draws: int,
        y: str = ModelConstants.DEFAULT_DEPENDENT_VAR,
        dims: Optional[List[str]] = None,
        seed: int | None = None,
    ) -> None:
        """Initialize arguments and set state.

        Args:
            dataset: (xr.Dataset | xr.DataArray): Data to forecast. If ``xr.Dataset`` then
                ``y`` paramter must be specified as the variable to forecast.
            years (YearRange): The forecasting time series.
            draws (int): Number of draws to make for predictions.
            y (str): If using a ``xr.Dataset`` the dependent variable to apply analysis to.
            dims (Optional[List[str]]): List of demographics dimensions to pool random walk
                estimates over. Defaults to an empty list. **NOTE:** ``super_region_id``
                pooling takes higher precedence than ``region_id`` pooling, so if both are
                given in dims, then only ``super_region_id`` pooling will be done.
            seed (Optional[int]): An optional seed to set for numpy's random number generation
        """
        super().__init__(dataset, years, draws, y, seed)

        self._init_dims(dims)

        # Declare the instance field that holds the projections of, ``y``, i.e., the dependent
        # variable (after the ``fit()`` and ``predict`` methods have been called).
        self.y_hat_data: Optional[xr.DataArray] = None

    def fit(self) -> None:
        """Fit the pooled random walk model."""
        if len(self.dims) >= 1:
            self._pooled_fit()
        else:
            logger.warning("Projecting -- no pooled fit", bindings=dict(pooled_fit=False))
            super().fit()

    def predict(self) -> xr.DataArray:
        """Generate predictions based on model fits.

        Returns:
            xr.DataArray: The random walk projections.
        """
        if len(self.dims) > 0:
            self._pooled_predict()
            return self.y_hat_data
        else:
            logger.warning("Projecting -- no pooled fit", bindings=dict(pooled_fit=False))
            self.y_hat_data = super().predict()
            return self.y_hat_data

    def _init_dims(self, dims: Optional[str]) -> None:
        """Initializes the ``dims`` instance field."""
        self.dims = dims if dims is not None else []

        unexpected_dims = set(self.dims) - (  # list(
            set(self.dataset.dims) | set(self.dataset.data_vars)
        )
        if unexpected_dims:
            err_msg = f"Some of the dims to pool over are not included: {unexpected_dims}"
            logger.error(err_msg, bindings=dict(unexpected_dims=unexpected_dims))
            raise IndexError(err_msg)

        # super-region pooling takes higher precedence than region-pooling.
        if DimensionConstants.SUPER_REGION_ID in self.dims:
            logger.debug(
                (
                    "'super_region_id' dim present, 'region_id' and 'location_id' dims will "
                    "be removed if also present"
                )
            )
            self.dims = [
                d
                for d in self.dims
                if d not in (DimensionConstants.REGION_ID, DimensionConstants.LOCATION_ID)
            ]
        elif DimensionConstants.REGION_ID in self.dims:
            # region pooling takes higher precedence than detailed-location-pooling.
            if DimensionConstants.SUPER_REGION_ID in self.dims:
                logger.debug(
                    (
                        "'region_id' dim present, 'location_id' dim will be removed if also "
                        "present"
                    )
                )
                self.dims = [d for d in self.dims if d != DimensionConstants.LOCATION_ID]

    def _pooled_fit(self) -> None:
        """This method handles pooled Random Walk model fitting."""
        # Make a copy of the input data to be pre-processed before using it in the model.
        y_data = self.dataset[self.y].copy()
        dims = [d for d in self.dims]

        # If ``"super_region_id"`` is a dim to pool over, update coordinates of the
        # ``location_id`` dim of the input dependent variable data to the values of the
        # ``super_region_id`` data variable from the input dataset.
        if DimensionConstants.SUPER_REGION_ID in dims:
            y_data, dims = self._pre_pool_fit(
                y_data=y_data,
                dims=dims,
                aggregate_location_dim=DimensionConstants.SUPER_REGION_ID,
            )
        # super-region pooling takes higher precedence than region-pooling, but if
        # ``"region_id"`` is a dim to pool over and ``"super_region_id"`` is not, then do the
        # same thing with ``"region_id"`` as is done above with ``"super_region_id"``.
        elif DimensionConstants.REGION_ID in dims:
            y_data, dims = self._pre_pool_fit(
                y_data=y_data,
                dims=dims,
                aggregate_location_dim=DimensionConstants.REGION_ID,
            )

        # Create the sigma place holder dataarray to be filled in with fit-parameter values.
        dim_coord_dict = {d: list(y_data[d].values) for d in dims}
        value_array = np.ones([len(x) for x in dim_coord_dict.values()])

        self.sigma = xr.DataArray(
            value_array,
            dims=dims,
            coords=dim_coord_dict,
            name=PooledRandomWalkConstants.SIGMA,
        )

        year_dict = {DimensionConstants.YEAR_ID: list(self.years.past_years)}
        dim_coord_sets = [np.unique(y_data[d].values) for d in dims]

        results = []
        for slice_coords in it.product(*dim_coord_sets):
            result = self._fit_work(
                dims=dims,
                year_dict=year_dict,
                y_data=y_data,
                slice_coords=slice_coords,
            )
            results.append(result)

        for result in results:
            coord_slice_dict = result[0]
            self.sigma.loc[coord_slice_dict] = result[1]

        if DimensionConstants.SUPER_REGION_ID in self.dims:
            self._post_pool_fit(aggregate_location_dim=DimensionConstants.SUPER_REGION_ID)
        elif DimensionConstants.REGION_ID in self.dims:
            self._post_pool_fit(aggregate_location_dim=DimensionConstants.REGION_ID)

    def _pre_pool_fit(
        self,
        y_data: xr.DataArray,
        dims: List[str],
        aggregate_location_dim: str,
    ) -> Tuple[xr.DataArray, List[str]]:
        """Prepare inputs for pooling."""
        y_data = y_data.assign_coords(location_id=self.dataset[aggregate_location_dim].values)
        dims = [
            d if d != aggregate_location_dim else DimensionConstants.LOCATION_ID for d in dims
        ]
        return y_data, dims

    def _post_pool_fit(self, aggregate_location_dim: str) -> None:
        """Cleanup model-fit after pooling."""
        logger.info(
            (
                f"Switching dim='location_id' back from {aggregate_location_dim} coords to"
                f" 'location_id' coords"
            )
        )
        location_ids = self.dataset[aggregate_location_dim].location_id.values
        self.sigma = self.sigma.assign_coords(location_id=location_ids)
        self.dataset = self.dataset.assign_coords(location_id=location_ids)

    def _pooled_predict(self) -> None:
        """This method handles pooled Random Walk model projection."""
        # Set place holder dataarray for projections -- with appropriate dim/coord shape.
        self.y_hat_data = expand_dimensions(
            xr.ones_like(
                self.dataset[self.y].sel(
                    **{DimensionConstants.YEAR_ID: self.years.past_end}, drop=True
                )
            ),
            **{DimensionConstants.YEAR_ID: self.years.years},
            draw=list(range(self.draws)),
        )

        y_observed_data = self.dataset[self.y]
        dims = list(self.sigma.coords.indexes.keys())
        dim_coord_sets = [self.sigma[d].values for d in self.sigma.dims]

        results = []
        for slice_coords in it.product(*dim_coord_sets):
            result = self._predict_work(
                dims=dims,
                y_observed_data=y_observed_data,
                y_hat_data=self.y_hat_data,
                sigma=self.sigma,
                years=self.years,
                slice_coords=slice_coords,
            )
            results.append(result)

        for result in results:
            coord_slice_dict = result[0]
            self.y_hat_data.loc[coord_slice_dict] = result[1]

    @staticmethod
    def _fit_work(
        dims: List[str],
        year_dict: Dict[str, List[int]],
        y_data: xr.DataArray,
        slice_coords: Tuple[int],
    ) -> Tuple[Dict[str, int], np.ndarray]:
        """Calculate sigma based on past time series variation."""
        coord_slice_dict = {dims[i]: slice_coords[i] for i in range(len(dims))}

        y_data_slice = y_data.loc[dict(**coord_slice_dict, **year_dict)]
        diff = y_data_slice.diff(DimensionConstants.YEAR_ID)
        sigma = np.std(diff.values)

        return coord_slice_dict, sigma

    def _predict_work(
        self,
        dims: List[str],
        y_observed_data: xr.DataArray,
        y_hat_data: xr.DataArray,
        sigma: xr.DataArray,
        years: YearRange,
        slice_coords: Tuple[int],
    ) -> Tuple[Dict[str, int], xr.DataArray]:
        """Generate predictions for a specific age, sex, location."""
        coord_slice_dict = {dims[i]: slice_coords[i] for i in range(len(slice_coords))}

        y_hat_slice = y_hat_data.loc[coord_slice_dict]
        y_observed_slice = y_observed_data.loc[coord_slice_dict]
        sigma_slice = sigma.loc[coord_slice_dict]

        # NaNs produced by upstream R process can be < 0, which causes error -- we want to
        # replace these with Numpy NaNs.
        if np.isnan(sigma_slice.values):
            logger.warning("Replacing R NaNs with Numpy NaNs")
            sigma_slice.values = np.nan

        # Populate past year data with observed data but copied to include fake draws.
        past_year_dict = {DimensionConstants.YEAR_ID: years.past_years}

        past_shape = y_hat_slice.loc[past_year_dict]
        y_hat_past = y_observed_slice.sel(**past_year_dict) * y_hat_slice.sel(**past_year_dict)
        y_hat_slice.loc[past_year_dict] = y_hat_past.transpose(*past_shape.coords.dims)

        # Populate future year data with projections.
        forecast_year_dict = {DimensionConstants.YEAR_ID: years.forecast_years}

        forecast_shape = y_hat_slice.sel(**forecast_year_dict)
        distribution = xr.DataArray(
            self.rng.normal(
                loc=0,
                scale=sigma_slice.values,
                size=forecast_shape.shape,
            ),
            coords=forecast_shape.coords,
            dims=forecast_shape.dims,
        )

        y_hat_forecast = y_observed_slice.sel(
            **{DimensionConstants.YEAR_ID: years.past_end}, drop=True
        ) + distribution.cumsum(DimensionConstants.YEAR_ID)
        y_hat_slice.loc[forecast_year_dict] = y_hat_forecast.transpose(
            *forecast_shape.coords.dims
        )

        return coord_slice_dict, y_hat_slice

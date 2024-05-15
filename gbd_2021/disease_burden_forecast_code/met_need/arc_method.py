"""Module with functions for making forecast scenarios."""

from typing import Any, Callable, Iterable, List, Optional, Type, Union

import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_data_transformation.lib.statistic import (
    Quantiles,
    weighted_mean_with_extra_dim,
    weighted_quantile_with_extra_dim,
)
from fhs_lib_data_transformation.lib.truncate import truncate_dataarray
from fhs_lib_data_transformation.lib.validate import assert_coords_same
from fhs_lib_database_interface.lib.constants import DimensionConstants, ScenarioConstants
from fhs_lib_database_interface.lib.query import location
from fhs_lib_file_interface.lib import xarray_wrapper
from fhs_lib_file_interface.lib.version_metadata import FHSDirSpec
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_lib_model.lib import predictive_validity_metrics as pv_metrics
from fhs_lib_model.lib.constants import ArcMethodConstants
from fhs_lib_model.lib.model_protocol import ModelProtocol

logger = get_logger()


class StatisticSpec:
    """A type representing a choice of statistical summary, with its attendant data.

    Can compute a weighted or an unweighted form. See MeanStatistic and QuantileStatistic.
    """

    def weighted_statistic(
        self, data: xr.DataArray, stat_dims: List[str], weights: xr.DataArray, extra_dim: str
    ) -> xr.DataArray:
        """Take a weighted summary statistic on annual_diff."""
        pass

    def unweighted_statistic(self, data: xr.DataArray, stat_dims: List[str]) -> xr.DataArray:
        """Take a unweighted statistic on annual_diff."""
        pass


class MeanStatistic(StatisticSpec):
    """A stat "take the mean of some things." Takes no args."""

    def weighted_statistic(
        self, data: xr.DataArray, stat_dims: List[str], weights: xr.DataArray, extra_dim: str
    ) -> xr.DataArray:
        """Take a weighted mean on `data`."""
        return weighted_mean_with_extra_dim(data, stat_dims, weights, extra_dim)

    def unweighted_statistic(self, data: xr.DataArray, stat_dims: List[str]) -> xr.DataArray:
        """Take an unweighted mean on `dat`a`, over the dimenstions stat_dims."""
        return data.mean(stat_dims)


class QuantileStatistic(StatisticSpec):
    """The intention of "take some quantiles from the data"."""

    def __init__(self, quantiles: Union[float, Iterable[float]]) -> None:
        """Args are the quantile fractions.

        E.g. QuantileStatistic([0.1, 0.9]) represents
        the desire to take the 10th percentile and 90th percentile. You may also
        pass a single number, as in QuantileStatistic(0.5) for a single quantile,
        the median in that case.
        """
        if not (isinstance(quantiles, float) or is_iterable_of(float, quantiles)):
            raise ValueError("Arg to QuantileStatistic must either float or list of floats")

        self.quantiles = quantiles

    def weighted_statistic(
        self, data: xr.DataArray, stat_dims: List[str], weights: xr.DataArray, extra_dim: str
    ) -> xr.DataArray:
        """Take a weighted set of quantiles on `data`."""
        return weighted_quantile_with_extra_dim(
            data, self.quantiles, stat_dims, weights, extra_dim
        )

    def unweighted_statistic(self, data: xr.DataArray, stat_dims: List[str]) -> xr.DataArray:
        """Take an unweighted set of quantiles on `data`."""
        return data.quantile(q=self.quantiles, dim=stat_dims)


class ArcMethod(ModelProtocol):
    """Instances of this class represent an arc_method model.

    Can be fit and used for predicting future estimates.
    """

    # Defined ARC method parameters:
    number_of_holdout_years = 10
    omega_step_size = 0.25
    max_omega = 3
    pv_metric = pv_metrics.root_mean_square_error

    def __init__(
        self,
        past_data: xr.DataArray,
        years: YearRange,
        draws: int,
        gbd_round_id: int,
        reference_scenario_statistic: str = "mean",
        reverse_scenarios: bool = False,
        quantiles: Iterable[float] = ArcMethodConstants.DEFAULT_SCENARIO_QUANTILES,
        mean_level_arc: bool = True,
        reference_arc_dims: Optional[List[str]] = None,
        scenario_arc_dims: Optional[List[str]] = None,
        truncate: bool = True,
        truncate_dims: Optional[List[str]] = None,
        truncate_quantiles: Iterable[float] = ArcMethodConstants.DEFAULT_TRUNCATE_QUANTILES,
        replace_with_mean: bool = False,
        scenario_roc: str = "all",
        pv_results: xr.DataArray = None,
        select_omega: bool = True,
        omega_selection_strategy: Optional[Callable] = None,
        omega: Optional[Union[float, xr.DataArray]] = None,
        pv_pre_process_func: Optional[Callable] = None,
        single_scenario_mode: bool = False,
        **kwargs: Any,
    ) -> None:
        """Creates a new ``ArcMethod`` model instance.

        Pre-conditions:
        ===============
        * All given ``xr.DataArray``s must have dimensions with at least 2
          coordinates. This applies for covariates and the dependent variable.

        Args:
            past_data (xr.DataArray): Past data for dependent variable being forecasted
            years (YearRange): forecasting timeseries
            draws (int): Number of draws to generate
            gbd_round_id (int): The ID of the GBD round
            reference_scenario_statistic (str): The statistic used to make the reference
                scenario. If "median" then the reference scenarios is made using the weighted
                median of past annualized rate-of-change across all past years, "mean" then it
                is made using the weighted mean of past annualized rate-of-change across all
                past years. Defaults to "mean".
            reverse_scenarios (bool): If ``True``, reverse the usual assumption that high=bad
                and low=good. For example, we set to ``True`` for vaccine coverage, because
                higher coverage is better. Defaults to ``False``.
            quantiles (Iterable[float]): The quantiles to use for better and worse
                scenarios. Defaults to ``0.15`` and ``0.85``.
            mean_level_arc (bool): If ``True``, then take annual differences for
                means-of-draws, instead of draws. Defaults to ``True``.
            reference_arc_dims (Optional[List[str]]): To calculate the reference ARC, take
                weighted mean or median over these dimensions. Defaults to ["year_id"] when
                ``None``.
            scenario_arc_dims (Optional[List[str]]): To calculate the scenario ARCs, take
                weighted quantiles over these dimensions. Defaults to ["location_id",
                "year_id"] when ``None``.
            truncate (bool): If ``True``, then truncate (clip) the past data over the given
                dimensions. Defaults to ``False``.
            truncate_dims (Optional[List[str]]): A list of strings representing the dimensions
                to truncate over. If ``None``, truncation occurs over location and year.
            truncate_quantiles (Iterable[float]): The  two floats representing the quantiles to
                take. Defaults to ``0.025`` and ``0.975``.
            replace_with_mean (bool): If ``True`` and `truncate` is ``True``, then replace
                values outside of the upper and lower quantiles taken across location and year
                with the mean across "year_id", if False, then replace with the upper and lower
                bounds themselves. Defaults to ``False``.
            scenario_roc (str): If "all", then the scenario rate of change is taken over all
                locations. If "national_only", roc is taken over national
                locations only. Defaults to "all".
            pv_results (xr.DataArray): An array of RMSEs resulting from predictive validity
                tests. The array has one dimension (weight), and the values are the RMSEs from
                each tested weight. When ``pv_results`` is ``None``, the ``fit`` method will
                calculate new ``pv_results``.
            select_omega (bool): If ``True``, the ``fit`` method will select an omega or create
                an omega distribution from ``self.pv_results``
            omega_selection_strategy (Optional[Callable]): Which strategy to use to produce the
                omega(s) from the omega-RMSE array, which gets produced in the fit step.
                Defaults to ``None``, but must be specified unless you are passing the model an
                omega directly. Can be specified as follows:
                ``model.oss.name_of_omega_selection_function``. See omega_selection_strategy.py
                for all omega selection functions.
            omega (Optional[Union[float, xr.DataArray]]): Power to raise the increasing year
                weights Must be non-negative. It can be dataarray, but must have only one
                dimension, ``draw``. It must have the same coordinates on that dimension as
                ``past_data_da``. When omega is ``None``, the fit method will calculate it from
                ``self.pv_results`` if select_omega is ``True``.
            pv_pre_process_func (Optional[Callable]): Function to call if preprocessing pv
                results.
            single_scenario_mode (bool): if true, only produces one scenario, not better and
                worse.
            kwargs (Any): Unused additional keyword arguments
        """
        if select_omega and omega_selection_strategy is None:
            err_msg = (
                "Must provide an omega_selection_strategy function if select_omega is True."
            )
            logger.error(err_msg)
            raise ValueError(err_msg)

        self.past_data = past_data
        self.years = years
        self.draws = draws
        self.gbd_round_id = gbd_round_id
        self.pv_results = pv_results
        self.select_omega = select_omega
        self.omega = omega
        self.pv_pre_process_func = pv_pre_process_func
        self.omega_selection_strategy = omega_selection_strategy
        self.reference_scenario_statistic = reference_scenario_statistic
        self.reverse_scenarios = reverse_scenarios
        self.quantiles = quantiles
        self.mean_level_arc = mean_level_arc
        self.reference_arc_dims = reference_arc_dims
        self.scenario_arc_dims = scenario_arc_dims
        self.truncate = truncate
        self.truncate_dims = truncate_dims
        self.truncate_quantiles = truncate_quantiles
        self.replace_with_mean = replace_with_mean
        self.scenario_roc = scenario_roc
        self.single_scenario_mode = single_scenario_mode

    def fit(self) -> Union[float, xr.DataArray]:
        """Runs a predictive validity process to determine omega to use for forecasting.

        If ``self.select_omega`` is ``False``, this will only calculate ``self.pv_results``
        PV results are only calculated when ``self.pv_results`` is ``None``.

        Returns:
            float | xr.DataArray: Power to raise the increasing year weights -- must be
                nonnegative. It can be dataarray, but must have only one dimension,
                DimensionConstants.DRAW. It must have the same coordinates on that dimension
                as ``past_data_da``.
        """
        holdout_start = self.years.past_end - self.number_of_holdout_years
        pv_years = YearRange(self.years.past_start, holdout_start, self.years.past_end)

        holdouts = self.past_data.sel(year_id=pv_years.forecast_years)
        omegas_to_test = np.arange(
            0, ArcMethod.max_omega + ArcMethod.omega_step_size, ArcMethod.omega_step_size
        )

        if self.pv_pre_process_func is not None:
            holdouts = self.pv_pre_process_func(holdouts)

        if self.pv_results is None:
            pv_result_list = []
            for test_omega in omegas_to_test:
                predicted = self._arc_method(pv_years, test_omega)
                if DimensionConstants.SCENARIO in predicted.coords:
                    predicted = predicted.sel(scenario=0, drop=True)

                assert_coords_same(predicted, self.past_data)

                predicted_holdouts = predicted.sel(year_id=pv_years.forecast_years)

                if self.pv_pre_process_func is not None:
                    predicted_holdouts = self.pv_pre_process_func(predicted_holdouts)

                pv_result = ArcMethod.pv_metric(predicted_holdouts, holdouts)
                pv_result_da = xr.DataArray(
                    [pv_result], coords={"weight": [test_omega]}, dims=["weight"]
                )
                pv_result_list.append(pv_result_da)

            self.pv_results = xr.concat(pv_result_list, dim="weight")

        if self.select_omega:
            self.omega = self.omega_selection_strategy(rmse=self.pv_results, draws=self.draws)

        return self.omega

    def predict(self) -> xr.DataArray:
        """Create projections for reference, better, and worse scenarios using the ARC method.

        Returns:
            xr.DataArray: Projections for future years made with the ARC method. It will
                include all the dimensions and coordinates of the
                ``self.past_data``, except that the ``year_id`` dimension will
                ONLY have coordinates for all of the years from
                ``self.years.forecast_years``. There will also be a new
                ``scenario`` dimension with the coordinates 0 for reference,
                -1 for worse, and 1 for better.
        """
        self.predictions = self._arc_method(
            self.years, self.omega, past_resample_draws=self.draws
        ).sel(year_id=self.years.forecast_years)

        return self.predictions

    def save_coefficients(
        self, output_dir: FHSDirSpec, entity: str, save_omega_draws: bool = False
    ) -> None:
        """Saves omega.

        I.e. the power to raise the increasing year weights to, and/or PV results,
        an array of RMSEs resulting from predictive validity tests.

        Args:
            output_dir (Path): directory to save data to
            entity (str): name to give output file
            save_omega_draws (bool): whether to save omega draws

        Raises:
            ValueError: if no omega or PV results present to save
        """

        def is_xarray(da: Any) -> bool:
            return isinstance(da, xr.Dataset) or isinstance(da, xr.DataArray)

        if self.omega is None and self.pv_results is None:
            err_msg = "No omega or predictive validity results to save"
            logger.error(err_msg)
            raise ValueError(err_msg)

        if self.omega is not None:
            if is_xarray(self.omega) and not save_omega_draws:
                logger.debug(
                    "Computing stats of omega draws",
                    bindings=dict(model=self.__class__.__name__),
                )
                coef_stats = self._compute_stats(self.omega)
            elif is_xarray(self.omega) and save_omega_draws:
                coef_stats = self.omega
            elif isinstance(self.omega, float) or isinstance(self.omega, int):
                logger.debug(
                    "omega is singleton value",
                    bindings=dict(model=self.__class__.__name__),
                )
                coef_stats = xr.DataArray(
                    [self.omega],
                    dims=["omega"],
                    coords={"omega": ["value"]},
                )

            omega_output_file = output_dir.append_sub_path(("coefficients",)).file(
                f"{entity}_omega.nc"
            )

            xarray_wrapper.save_xr_scenario(
                coef_stats,
                omega_output_file,
                metric="rate",
                space="identity",
            )

        if self.pv_results is not None:
            pv_output_file = output_dir.append_sub_path(("coefficients",)).file(
                f"{entity}_omega_rmses.nc"
            )

            xarray_wrapper.save_xr_scenario(
                self.pv_results,
                pv_output_file,
                metric="rate",
                space="identity",
            )

    @staticmethod
    def _compute_stats(da: xr.DataArray) -> Union[xr.DataArray, xr.Dataset]:
        """Compute mean and variance of draws if a ``'draw'`` dim exists.

        Otherwise just return a copy of the original.

        Args:
            da (xr.DataArray): data array for computation

        Returns:
            Union[xr.DataArray, xr.Dataset]: the computed data
        """
        if DimensionConstants.DRAW in da.dims:
            mean_da = da.mean(DimensionConstants.DRAW).assign_coords(stat="mean")
            var_da = da.var(DimensionConstants.DRAW).assign_coords(stat="var")
            stats_da = xr.concat([mean_da, var_da], dim="stat")
        else:
            logger.warning(
                "Draw is NOT a dim, can't compute omega stats",
                bindings=dict(model=__class__.__name__, dims=da.dims),
            )
            stats_da = da.copy()
        return stats_da

    def _arc_method(
        self,
        years: YearRange,
        omega: Union[float, xr.DataArray],
        past_resample_draws: Optional[int] = None,
    ) -> xr.DataArray:
        """Run and return the `arc_method`.

        To keep the PV step and prediction step consistent put the explicit ``arc_method``
        call with all of its defined parameters here.

        Args:
            years (YearRange): years to include in the past when calculating ARC
            omega (Union[float, xr.DataArray]): the omega to assess for draws
            past_resample_draws (Optional[int]): The number of draws to resample from the past
                data. This argument is used in the predict step to avoid NaNs in the forecast
                when there is a mismatch between the number of draw coordinates in the past
                data and the desired number of draw coordinates.

        Returns:
            xr.DataArray: result of the `arc_method` function call
        """
        omega_dim = ArcMethod._get_omega_dim(omega, self.draws)

        if past_resample_draws is not None and "draw" in self.past_data.dims:
            past_data = resample(self.past_data, past_resample_draws)
        else:
            past_data = self.past_data

        return arc_method(
            past_data_da=past_data,
            gbd_round_id=self.gbd_round_id,
            years=years,
            weight_exp=omega,
            reference_scenario=self.reference_scenario_statistic,
            reverse_scenarios=self.reverse_scenarios,
            quantiles=self.quantiles,
            diff_over_mean=self.mean_level_arc,
            reference_arc_dims=self.reference_arc_dims,
            scenario_arc_dims=self.scenario_arc_dims,
            truncate=self.truncate,
            truncate_dims=self.truncate_dims,
            truncate_quantiles=self.truncate_quantiles,
            replace_with_mean=self.replace_with_mean,
            extra_dim=omega_dim,
            scenario_roc=self.scenario_roc,
            single_scenario_mode=self.single_scenario_mode,
        )

    @staticmethod
    def _get_omega_dim(omega: Union[float, int, xr.DataArray], draws: int) -> Optional[str]:
        """Get the omega dimension if passed a data array.

        Args:
            omega (Union[float, int, xr.DataArray]): the omega value or data array
            draws (int): the number of draws to validate omega against

        Returns:
            Optional[str]: ``'draw'``, if ``omega`` contains draw specific omegas as a
                dataarray or ``None``, if ``omega`` is float.

        Raises:
            ValueError: if `omega` draw dim doesn't have the expected coords
            TypeError: if `omega` isn't a float, int, or data array
        """
        if isinstance(omega, float) or isinstance(omega, int):
            omega_dim = None
        elif isinstance(omega, xr.DataArray):
            if set(omega.dims) != {DimensionConstants.DRAW}:
                err_msg = "`omega` can only have 'draw' as a dim"
                logger.error(err_msg)
                raise ValueError(err_msg)
            elif sorted(list(omega[DimensionConstants.DRAW].values)) != list(range(draws)):
                err_msg = "`omega`'s draw dim doesn't have the expected coords"
                logger.error(err_msg)
                raise ValueError(err_msg)
            omega_dim = DimensionConstants.DRAW
        else:
            err_msg = "`omega` must be either a float, an int, or an xarray.DataArray"
            logger.error(err_msg)
            raise TypeError(err_msg)

        return omega_dim


def arc_method(
    past_data_da: xr.DataArray,
    gbd_round_id: int,
    years: Optional[Iterable[int]] = None,
    weight_exp: Union[float, int, xr.DataArray] = 1,
    reference_scenario: str = "median",
    reverse_scenarios: bool = False,
    quantiles: Iterable[float] = ArcMethodConstants.DEFAULT_SCENARIO_QUANTILES,
    diff_over_mean: bool = False,
    reference_arc_dims: Optional[List[str]] = None,
    scenario_arc_dims: Optional[List[str]] = None,
    truncate: bool = False,
    truncate_dims: Optional[List[str]] = None,
    truncate_quantiles: Optional[Iterable[float]] = None,
    replace_with_mean: bool = False,
    extra_dim: Optional[str] = None,
    scenario_roc: str = "all",
    single_scenario_mode: bool = False,
) -> xr.DataArray:
    """Makes rate forecasts using the Annualized Rate-of-Change (ARC) method.

    Forecasts rates by taking a weighted quantile or weighted mean of
    annualized rates-of-change from past data, then walking that weighted
    quantile or weighted mean out into future years.

    A reference scenarios is made using the weighted median or mean of past
    annualized rate-of-change across all past years.

    Better and worse scenarios are made using weighted 15th and 85th quantiles
    of past annualized rates-of-change across all locations and all past years.

    The minimum and maximum are taken across the scenarios (values are
    granular, e.g. age/sex/location/year specific) and the minimum is taken as
    the better scenario and the maximum is taken as the worse scenario. If
    scenarios are reversed (``reverse_scenario = True``) then do the opposite.

    Args:
        past_data_da:
            A dataarray of past data that must at least of the dimensions
            ``year_id`` and ``location_id``. The ``year_id`` dimension must
            have coordinates for all the years in ``years.past_years``.
        gbd_round_id:
            gbd_round_id the data comes from.
        years:
            years to include in the past when calculating ARC.
        weight_exp:
            power to raise the increasing year weights -- must be nonnegative.
            It can be dataarray, but must have only one dimension, "draw", it
            must have the same coordinates on that dimension as
            ``past_data_da``.
        reference_scenario:
            If "median" then the reference scenarios is made using the
            weighted median of past annualized rate-of-change across all past
            years, "mean" then it is made using the weighted mean of past
            annualized rate-of-change across all past years. Defaults to
            "median".
        reverse_scenarios:
            If True, reverse the usual assumption that high=bad and low=good.
            For example, we set to True for vaccine coverage, because higher
            coverage is better. Defaults to False.
        quantiles:
            The quantiles to use for better and worse scenarios. Defaults to
            ``0.15`` and ``0.85`` quantiles.
        diff_over_mean:
            If True, then take annual differences for means-of-draws, instead
            of draws. Defaults to False.
        reference_arc_dims:
            To calculate the reference ARC, take weighted mean or median over
            these dimensions. Defaults to ["year_id"]
        scenario_arc_dims:
            To calculate the scenario ARCs, take weighted quantiles over these
            dimensions.Defaults to ["location_id", "year_id"]
        truncate:
            If True, then truncates the dataarray over the given dimensions.
            Defaults to False.
        truncate_dims:
            A list of strings representing the dimensions to truncate over.
        truncate_quantiles:
            The tuple of two floats representing the quantiles to take.
        replace_with_mean:
            If True and `truncate` is True, then replace values outside of the
            upper and lower quantiles taken across "location_id" and "year_id"
            and with the mean across "year_id", if False, then replace with the
            upper and lower bounds themselves.
        extra_dim:
            Extra dimension that exists in `weights` and `data`. It should not
            be in `stat_dims`.
        scenario_roc:
            If "all", then the scenario rate of change is taken over all
            locations. If "national_only", roc is taken over national
            locations only. Defaults to "all".
        single_scenario_mode:
            If true, better and worse scenarios are not calculated, and the reference scenario
            is returned without a scenario dimension.

    Returns:
        Past and future data with reference, better, and worse scenarios.
        It will include all the dimensions and coordinates of the input
        dataarray and a ``scenario`` dimension with the coordinates 0 for
        reference, -1 for worse, and 1 for better. The ``year_id``
        dimension will have coordinates for all of the years from
        ``years.years``.

    Raises:
        ValueError: If ``weight_exp`` is a negative number or if ``reference_scenario``
            is not "median" or "mean".
    """
    logger.debug(
        "Inputs for `arc_method` call",
        bindings=dict(
            years=years,
            weight_exp=weight_exp,
            reference_scenario=reference_scenario,
            reverse_scenarios=reverse_scenarios,
            quantiles=quantiles,
            diff_over_mean=diff_over_mean,
            truncate=truncate,
            replace_with_mean=replace_with_mean,
            truncate_quantiles=truncate_quantiles,
            extra_dim=extra_dim,
        ),
    )

    years = YearRange(*years) if years else YearRange(*ArcMethodConstants.DEFAULT_YEAR_RANGE)

    past_data_da = past_data_da.sel(year_id=years.past_years)

    # Create baseline forecasts. Take weighted median or mean only across
    # years, so values will be as granular as the inputs (e.g. age/sex/location
    # specific)
    if reference_scenario == "median":
        reference_statistic = QuantileStatistic(0.5)
    elif reference_scenario == "mean":
        reference_statistic = MeanStatistic()
    else:
        raise ValueError("reference_scenario must be either 'median' or 'mean'")

    if truncate and not truncate_dims:
        truncate_dims = [DimensionConstants.LOCATION_ID, DimensionConstants.YEAR_ID]

    truncate_quantiles = (
        Quantiles(*sorted(truncate_quantiles))
        if truncate_quantiles
        else Quantiles(0.025, 0.975)
    )

    reference_arc_dims = reference_arc_dims or [DimensionConstants.YEAR_ID]
    reference_change = arc(
        past_data_da,
        years,
        weight_exp,
        reference_arc_dims,
        reference_statistic,
        diff_over_mean=diff_over_mean,
        truncate=truncate,
        truncate_dims=truncate_dims,
        truncate_quantiles=truncate_quantiles,
        replace_with_mean=replace_with_mean,
        extra_dim=extra_dim,
    )
    reference_da = past_data_da.sel(year_id=years.past_end) + reference_change
    forecast_data_da = past_data_da.combine_first(reference_da)

    if not single_scenario_mode:
        forecast_data_da = _forecast_better_worse_scenarios(
            past_data_da=past_data_da,
            gbd_round_id=gbd_round_id,
            years=years,
            weight_exp=weight_exp,
            reverse_scenarios=reverse_scenarios,
            quantiles=quantiles,
            diff_over_mean=diff_over_mean,
            scenario_arc_dims=scenario_arc_dims,
            replace_with_mean=replace_with_mean,
            extra_dim=extra_dim,
            scenario_roc=scenario_roc,
            forecast_data_da=forecast_data_da,
        )

    return forecast_data_da


def _forecast_better_worse_scenarios(
    forecast_data_da: xr.DataArray,
    past_data_da: xr.DataArray,
    gbd_round_id: int,
    years: YearRange,
    weight_exp: Union[float, int, xr.DataArray],
    reverse_scenarios: bool,
    quantiles: Iterable[float],
    diff_over_mean: bool,
    scenario_arc_dims: Optional[List[str]],
    replace_with_mean: bool,
    extra_dim: Optional[str],
    scenario_roc: str,
) -> xr.DataArray:
    try:
        forecast_data_da = forecast_data_da.rename(
            {DimensionConstants.QUANTILE: DimensionConstants.SCENARIO}
        )
    except ValueError:
        pass  # There is no "quantile" point coordinate.

    forecast_data_da[DimensionConstants.SCENARIO] = ScenarioConstants.REFERENCE_SCENARIO_COORD

    # Create better and worse scenario forecasts. Take weighted 85th and 15th
    # quantiles across year and location, so values will not be location
    # specific (e.g. just age/sex specific).
    scenario_arc_dims = scenario_arc_dims or [
        DimensionConstants.LOCATION_ID,
        DimensionConstants.YEAR_ID,
    ]
    if scenario_roc == "national":
        nation_ids = location.get_location_set(
            gbd_round_id=gbd_round_id, include_aggregates=False, national_only=True
        )[DimensionConstants.LOCATION_ID].unique()

        arc_input = past_data_da.sel(location_id=nation_ids)
    elif scenario_roc == "all":
        arc_input = past_data_da
    else:
        raise ValueError(
            f'scenario_roc should be one of "national" or "all"; got {scenario_roc}'
        )
    scenario_change = arc(
        arc_input,
        years,
        weight_exp,
        scenario_arc_dims,
        QuantileStatistic(quantiles),
        diff_over_mean=diff_over_mean,
        truncate=False,
        replace_with_mean=replace_with_mean,
        extra_dim=extra_dim,
    )

    scenario_change = scenario_change.rename(
        {DimensionConstants.QUANTILE: DimensionConstants.SCENARIO}
    )
    scenarios_da = past_data_da.sel(year_id=years.past_end) + scenario_change

    scenarios_da.coords[DimensionConstants.SCENARIO] = [
        ScenarioConstants.BETTER_SCENARIO_COORD,
        ScenarioConstants.WORSE_SCENARIO_COORD,
    ]

    forecast_data_da = xr.concat(
        [forecast_data_da, scenarios_da], dim=DimensionConstants.SCENARIO
    )

    # Get the minimums and maximums across the scenario dimension, and set
    # worse scenarios to the worst (max if normal or min if reversed), and set
    # better scenarios to the best (min if normal or max if reversed).
    low_values = forecast_data_da.min(DimensionConstants.SCENARIO)
    high_values = forecast_data_da.max(DimensionConstants.SCENARIO)
    if reverse_scenarios:
        forecast_data_da.loc[
            {DimensionConstants.SCENARIO: ScenarioConstants.WORSE_SCENARIO_COORD}
        ] = low_values
        forecast_data_da.loc[
            {DimensionConstants.SCENARIO: ScenarioConstants.BETTER_SCENARIO_COORD}
        ] = high_values
    else:
        forecast_data_da.loc[
            {DimensionConstants.SCENARIO: ScenarioConstants.BETTER_SCENARIO_COORD}
        ] = low_values
        forecast_data_da.loc[
            {DimensionConstants.SCENARIO: ScenarioConstants.WORSE_SCENARIO_COORD}
        ] = high_values

    forecast_data_da = past_data_da.combine_first(forecast_data_da)

    forecast_data_da = forecast_data_da.loc[
        {DimensionConstants.SCENARIO: sorted(forecast_data_da[DimensionConstants.SCENARIO])}
    ]

    return forecast_data_da


def arc(
    past_data_da: xr.DataArray,
    years: YearRange,
    weight_exp: Union[float, int, xr.DataArray],
    stat_dims: Iterable[str],
    statistic: StatisticSpec,
    diff_over_mean: bool = False,
    truncate: bool = False,
    truncate_dims: Optional[List[str]] = None,
    truncate_quantiles: Optional[Iterable[float]] = None,
    replace_with_mean: bool = False,
    extra_dim: Optional[str] = None,
) -> xr.DataArray:
    r"""Makes rate forecasts by forecasting the Annualized Rates-of-Change (ARC).

    Uses either weighted means or weighted quantiles.

    The steps for forecasting logged or logitted rates with ARCs are:

    (1) Annualized rate differentials (or annualized rates-of-change if data is
        in log or logit space) are calculated.

        .. Math::

            \vec{D_{p}} =
            [x_{1991} - x_{1990}, x_{1992} - x_{1991}, ... x_{2016} - x_{2015}]

        where :math:`x` are values from ``past_data_da`` for each year and
        :math:`\vec{D_p}` is the vector of differentials in the past.

    (2) Year weights are used to weight recent years more heavily. Year weights
        are made by taking the interval

        .. math::

            \vec{W} = [1, ..., n]^w

        where :math:`n` is the number of past years, :math:`\vec{w}` is the
        value given by ``weight_exp``, and :math:`\vec{W}` is the vector of
        year weights.

    (3) Weighted quantiles or the weighted mean of the annualized
        rates-of-change are taken over the dimensions.

        .. math::

            s = \text{weighted-statistic}(\vec{W}, \vec{D})

        where :math:`s` is the weighted quantile or weighted mean.

    (4) Future rates-of-change are simulated by taking the interval

        .. math::

            \vec{D_{f}} = [1, ..., m] * s

        where :math:`\vec{D_f}` is the vector of differentials in the future
        and :math:`m` is the number of future years to forecast and

    (5) Lastly, these future differentials are added to the rate of the last
        observed year.

        .. math::

            \vec{X_{f}} = \vec{D_{f}} + x_{2016} = [x_{2017}, ..., x_{2040}]

        where :math:`X_{f}` is the vector of forecasted rates.

    Args:
        past_data_da:
            Past data with a year-id dimension. Must be in log or logit space
            in order for this function to actually calculate ARCs, otherwise
            it's just calculating weighted statistic of the first differences.
        years:
            past and future year-ids
        weight_exp:
            power to raise the increasing year weights -- must be nonnegative.
            It can be dataarray, but must have only one dimension, "draw", it
            must have the same coordinates on that dimension as
            ``past_data_da``.
        stat_dims:
            list of dimensions to take quantiles over
        statistic: A statistic to use to calculate the ARC from the annual
            diff, either MeanStatistic() or QuantileStatistic(quantiles).
        diff_over_mean:
            If True, then take annual differences for means-of-draws, instead
            of draws. Defaults to False.
        truncate:
            If True, then truncates the dataarray over the given dimensions.
            Defaults to False.
        truncate_dims:
            A list of strings representing the dimensions to truncate over.
        truncate_quantiles:
            The iterable of two floats representing the quantiles to take.
        replace_with_mean:
            If True and `truncate` is True, then replace values outside of the
            upper and lower quantiles taken across "location_id" and "year_id"
            and with the mean across "year_id", if False, then replace with the
            upper and lower bounds themselves.
        extra_dim:
            An extra dim to take the `statistic` over. Should exist in
            `weights` and `data`. It should not be in `stat_dims`.

    Returns:
        Forecasts made using the ARC method.

    Raises:
        ValueError: Conditions:

            * If ``statistic`` is ill-formed.
            * If ``weight_exp`` is a negative number.
            * If `truncate` is True, then `truncate_quantiles` must be a list of floats.
    """
    logger.debug(
        "Inputs for `arc` call",
        bindings=dict(
            years=years,
            weight_exp=weight_exp,
            statistic=statistic,
            stat_dims=stat_dims,
            diff_over_mean=diff_over_mean,
            truncate=truncate,
            replace_with_mean=replace_with_mean,
            truncate_quantiles=truncate_quantiles,
            extra_dim=extra_dim,
        ),
    )

    # Calculate the annual differentials.
    if diff_over_mean and DimensionConstants.DRAW in past_data_da.dims:
        annual_diff = past_data_da.mean(DimensionConstants.DRAW)
    else:
        annual_diff = past_data_da
    annual_diff = annual_diff.sel(year_id=years.past_years).diff(
        DimensionConstants.YEAR_ID, n=1
    )

    if isinstance(weight_exp, xr.DataArray):
        if DimensionConstants.DRAW not in weight_exp.dims:  # pytype: disable=attribute-error
            raise ValueError(
                "`weight_exp` must be a float, an int, or an xarray.DataArray "
                "with a 'draw' dimension"
            )

        # If annual-differences were taken over means (`annual_diff` doesn't have a "draw"
        # dimension), but `year_weights` does have a "draw" dimension, then the draw dimension
        # needs to be expanded for `annual_diff` such that the mean is replicated for each draw
        if DimensionConstants.DRAW not in annual_diff.dims:
            annual_diff = expand_dimensions(
                annual_diff, draw=weight_exp[DimensionConstants.DRAW].values
            )
        weight_exp = expand_dimensions(
            weight_exp, year_id=annual_diff[DimensionConstants.YEAR_ID].values
        )

    year_weights = (
        xr.DataArray(
            (np.arange(len(years.past_years) - 1) + 1),
            dims=DimensionConstants.YEAR_ID,
            coords={DimensionConstants.YEAR_ID: years.past_years[1:]},
        )
        ** weight_exp
    )

    if truncate:
        if not is_iterable_of(float, truncate_quantiles):
            raise ValueError(
                "If `truncate` is True, then `truncate_quantiles` must be a list of floats."
            )

        truncate_dims = truncate_dims or [
            DimensionConstants.LOCATION_ID,
            DimensionConstants.YEAR_ID,
        ]
        truncate_quantiles = Quantiles(*sorted(truncate_quantiles))
        annual_diff = truncate_dataarray(
            annual_diff,
            truncate_dims,
            replace_with_mean=replace_with_mean,
            mean_dims=[DimensionConstants.YEAR_ID],
            weights=year_weights,
            quantiles=truncate_quantiles,
            extra_dim=extra_dim,
        )

    stat_dims = list(stat_dims)

    if (xr.DataArray(weight_exp) > 0).any():
        arc_da = statistic.weighted_statistic(annual_diff, stat_dims, year_weights, extra_dim)
    elif (xr.DataArray(weight_exp) == 0).all():
        # If ``weight_exp`` is zero, then just take the unweighted mean or
        # quantile.
        arc_da = statistic.unweighted_statistic(annual_diff, stat_dims)
    else:
        raise ValueError("weight_exp must be nonnegative.")

    # Find future change by multiplying an array that counts the future
    # years, by the quantiles, which is weighted if `weight_exp` > 0. We want
    # the multipliers to start at 1, for the first year of forecasts, and count
    # to one more than the number of years to forecast.
    forecast_year_multipliers = xr.DataArray(
        np.arange(len(years.forecast_years)) + 1,
        dims=[DimensionConstants.YEAR_ID],
        coords={DimensionConstants.YEAR_ID: years.forecast_years},
    )
    future_change = arc_da * forecast_year_multipliers
    return future_change


def is_iterable_of(type: Type, obj: Any) -> bool:
    """True iff the obj is an iterable containing only instances of the given type."""
    return hasattr(obj, "__iter__") and all([isinstance(item, type) for item in obj])


def approach_value_by_year(
    past_data: xr.DataArray,
    years: YearRange,
    target_year: int,
    target_value: float,
    method: str = "linear",
) -> xr.DataArray:
    """Forecasts cases where a target level at a target year is known.

    For e.g., the Rockefeller project for min-risk diet scenarios, wanted to
    see the effect of eradicating diet related risks by 2030 on mortality. For
    this we need to reach 0 SEV for all diet related risks by 2030 and keep
    the level constant at 0 for further years. Here the target_year is 2030
    and target_value is 0.

    Args:
        past_data:
            The past data with all past years.
        years:
            past and future year-ids
        target_year:
            The year at which the target value will be reached.
        target_value:
            The target value that needs to be achieved during the target year.
        method:
            The extrapolation method to be used to calculate the values for
            intermediate years (years between years.past_end and target_year).
            The method currently supported is: `linear`.

    Raises:
        ValueError: if method != "linear"

    Returns:
        The forecasted results.
    """
    if method == "linear":
        forecast = _linear_then_constant_arc(past_data, years, target_year, target_value)
    else:
        raise ValueError(
            f"Method {method} not recognized. Please see the documentation for"
            " the list of supported methods."
        )

    return forecast


def _linear_then_constant_arc(
    past_data: xr.DataArray, years: YearRange, target_year: int, target_value: float
) -> xr.DataArray:
    r"""Makes rate forecasts by linearly extrapolating.

    Extrapolates the point ARC from the last past year till the target year to reach the target
    value.

    The steps for extrapolating the point ARCs are:

    (1) Calculate the rate of change between the last year of the past
        data (eg.2017) and ``target_year`` (eg. 2030).

        .. Math::

            R =
             \frac{target\_value - past\_last\_year_value}
            {target\_year- past\_last\_year}

        where :math:`R` is the slope of the desired linear trend.

    (2) Calculate the rates of change between the  last year of the past and
        each future year by multiplying R with future year weights till
        ``target_year``.

        .. math::

            \vec{W} = [1, ..., m]

            \vec{F_r} = \vec{W} * R

        where :math:`m` is the number of years between the ``target_year`` and
        the last year of the past, and :math:`\vec{W}` forms the vector of
        year weights.
        :math:`\vec{F_r}` contains the linearly extrapolated ARCs for each
        future year till the ``target_year``.

    (3) Add the future rates :math: `\vec{F_r}` to last year of the past
        (eg. 2017) to get the forecasted results.

    (4) Extend the forecasted results till the ``forecast_end`` year by
        filling the ``target_value`` for all the remaining future years.

    Args:
        past_data:
            The past data with all past years. The data is assumed to be in
            normal space.
        years:
            past and future year-ids
        target_year:
            The year at which the target value will be reached.
        target_value:
            The value that needs to be achieved by the `target_year`.

    Returns:
        The forecasted results.
    """
    pre_target_years = np.arange(years.forecast_start, target_year + 1)
    post_target_years = np.arange(target_year + 1, years.forecast_end + 1)

    past_last_year = past_data.sel(year_id=years.past_end)
    target_yr_arc = (target_value - past_last_year) / (target_year - years.past_end)

    forecast_year_multipliers = xr.DataArray(
        np.arange(len(pre_target_years)) + 1,
        dims=[DimensionConstants.YEAR_ID],
        coords={DimensionConstants.YEAR_ID: pre_target_years},
    )

    future_change = target_yr_arc * forecast_year_multipliers
    forecast_bfr_target_year = past_last_year + future_change

    forecast = expand_dimensions(
        forecast_bfr_target_year, fill_value=target_value, year_id=post_target_years
    )

    return forecast

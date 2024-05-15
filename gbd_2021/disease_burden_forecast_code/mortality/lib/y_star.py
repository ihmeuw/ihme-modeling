r"""Compute Module.

This module computes :math:`y^*` (y-star), which is the sum of the latent trend
component, i.e., the epsilon or residual error predictions from an ARIMA or Random Walk model,
and the :math:`\hat{y}` (y-hat) predictions from the GK model. It is assumed that the inputs
are given in log-rate space.
"""

from functools import partial
from typing import List

import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib.correlate import correlate_draws
from fhs_lib_data_transformation.lib.exponentiate_draws import bias_exp_new
from fhs_lib_data_transformation.lib.intercept_shift import (
    ordered_draw_intercept_shift,
    unordered_draw_intercept_shift,
)
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_database_interface.lib import db_session
from fhs_lib_database_interface.lib.constants import (
    CauseConstants,
    DimensionConstants,
    ScenarioConstants,
)
from fhs_lib_database_interface.lib.query.location import get_location_set
from fhs_lib_database_interface.lib.strategy_set.strategy import get_cause_set
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec, VersionMetadata
from fhs_lib_file_interface.lib.versioning import validate_versions_scenarios_listed
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario, save_xr_scenario
from fhs_lib_model.lib.random_walk.pooled_random_walk import PooledRandomWalk
from fhs_lib_model.lib.random_walk.random_walk import RandomWalk
from fhs_lib_model.lib.remove_drift import get_decayed_drift_preds
from fhs_lib_year_range_manager.lib.year_range import YearRange
from sqlalchemy.orm import Session
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_pipeline_mortality.lib.intercept_shift import intercept_shift_draws
from fhs_pipeline_mortality.lib.smoothing import get_smoothing_dims

logger = get_logger()

LOG_SPACE = "log"
NO_DRIFT_LOCATIONS = [
    7, 11, 16, 29, 34, 39, 41, 50, 26, 30, 165, 169, 173, 180, 184, 190, 194, 196, 197, 198,
    207, 208, 214, 218, 157, 202, 217, 168, 171, 182, 191, 200, 201, 204, 215, 216, 205, 122,
    129, 422, 111, 170, 172, 175, 176, 177, 178, 179, 181, 185, 87, 189, 193, 195, 203, 206,
    209, 210, 211, 212, 213, 435, 40, 164,
]


def calculate_y_star(
    acause: str,
    agg_version: VersionMetadata,
    epsilon_version: VersionMetadata,
    past_version: VersionMetadata,
    years: YearRange,
    gbd_round_id: int,
    draws: int,
    decay: float,
    intercept_shift: bool,
    bias_correction: bool,
    national_only: bool,
    seed: int | None,
    output_scenario: int | None,
) -> None:
    r"""Calculates the :math:`y^*` -- aggregate of latent trend & :math:`\hat{y}` predictions.

    Samples mortality residuals from a latent trend model, e.g, ARIMA or Random Walk model, in
    order to aggregate the latent trend and the dependent variable predictions from the GK
    model, i.e, the :math:`\hat{y}`. Formally, the operation is as follows:

    .. math::

        y^* = \hat{y} + \hat{\epsilon}

    Args:
        acause (str): Name of the target acause to aggregate to.
        agg_version (VersionMetadata): Name of the aggregate version.
        epsilon_version (VersionMetadata): Name of the latent trend prediction version.
        past_version (VersionMetadata): The version containing actual raw data, :math:`y`, for
            past years.
        years (YearRange): A container for the three years, which define our forecast.
        gbd_round_id (int): The numeric ID for the GBD round.
        draws (int): Number of draws to take.
        decay (float): Rate at which the slope of the line decays once forecasts start.
        intercept_shift (bool): Whether to intercept shift the :math:`y^*` results.
        bias_correction (bool): Whether to perform log bias correction.
        national_only (bool): Whether to include subnational locations, or to include only
            nations.
        seed (Optional[int]): An optional seed to set for numpy's random number generation
        output_scenario (int | None): The scenario for the output if running in single scenario
            mode.
    """
    validate_versions_scenarios_listed(
        versions=[agg_version, epsilon_version, past_version],
        output_versions=[agg_version, epsilon_version],
        output_scenario=output_scenario,
    )

    legacy_scenario_mode = True if output_scenario is None else False

    logger.info(f"Computing y^* for {acause}", bindings=dict(acause=acause))

    y_hat_infile = FHSFileSpec(agg_version, f"{acause}_hat.nc")

    logger.debug("Opening y-hat data file", bindings=dict(y_hat_file=str(y_hat_infile)))
    y_hat = open_xr_scenario(y_hat_infile).sel(**{DimensionConstants.YEAR_ID: years.years})

    # Make sure y_past only has those locations in y_hat.
    location_ids = y_hat.location_id.values

    # GK intercept shift
    y_hat = intercept_shift_draws(
        preds=y_hat,
        acause=acause,
        past_version=past_version.version,
        gbd_round_id=gbd_round_id,
        years=years,
        draws=draws,
        shift_function=partial(
            ordered_draw_intercept_shift,
            modeled_order_year_id=years.past_end,
            shift_from_reference=legacy_scenario_mode,
        ),
    )
    save_xr_scenario(
        y_hat,
        FHSFileSpec(epsilon_version, f"{acause}_shifted.nc"),
        metric=DimensionConstants.RATE_METRIC,
        space=LOG_SPACE,
    )

    # Get actual past data (in log space).
    y_past = _get_y_past(acause=acause, years=years, past_version=past_version).sel(
        **{DimensionConstants.LOCATION_ID: location_ids}
    )

    with db_session.create_db_session() as session:
        is_ntd = _is_ntd(acause, gbd_round_id, session)
        smoothing = get_smoothing_dims(acause, gbd_round_id, session)

    if not is_ntd:
        logger.debug("Including latent trend")

        # NOTE: latent trend model should be done for every cause except any of the Neglected
        # Tropical Diseases (NTDs).
        logger.debug("Computing ``epsilon_past``")

        # NOTE: epsilon_hat is calculated purely from past epsilon, and is scenario-less
        epsilon_hat_outfile = FHSFileSpec(
            epsilon_version.with_scenario(None), f"{acause}_eps.nc"
        )

        # Calculate past epsilon.
        epsilon_past = y_past.sel(
            **{DimensionConstants.YEAR_ID: years.past_years}
        ) - y_hat.sel(**{DimensionConstants.YEAR_ID: years.past_years})

        if legacy_scenario_mode:
            epsilon_past = epsilon_past.sel(
                scenario=ScenarioConstants.REFERENCE_SCENARIO_COORD,
                drop=True,
            )
        epsilon_past = epsilon_past.mean(DimensionConstants.DRAW)

        # Create future epsilon predictions.
        epsilon_hat = _draw_epsilons(
            epsilon_past=epsilon_past,
            draws=draws,
            smoothing=smoothing,
            years=years,
            acause=acause,
            decay=decay,
            gbd_round_id=gbd_round_id,
            national_only=national_only,
            seed=seed,
        )

        # Save epsilon predictions for future use.
        save_xr_scenario(
            epsilon_hat,
            epsilon_hat_outfile,
            metric=DimensionConstants.RATE_METRIC,
            space=LOG_SPACE,
        )

        # Compute initial y-star (before bias correction and/or intercept shift).
        y_star = _get_y_star(
            y_hat=y_hat,
            epsilon_hat=epsilon_hat,
            years=years,
            epsilon_version=epsilon_version,
        ).copy()

    else:
        logger.debug("No latent trend")

        # Without latent trend component, y-star is just y-hat.
        y_star = y_hat
        y_star.name = "value"

    # NOTE: by this point in the code, in legacy_scenario mode, y_star has all 3 "legacy"
    # scenarios. In single-scenario mode, y_star has just the scenario of interest

    if bias_correction:
        logger.debug("Performing bias correction on logged draws")
        y_star = np.log(bias_exp_new(y_star))
    else:
        logger.debug("Leaving logged draws raw in terms of bias")

    if intercept_shift:
        logger.debug("Performing intercept shift on y^* results")
        y_star = intercept_shift_draws(
            preds=y_star,
            acause=acause,
            past_version=past_version.version,
            gbd_round_id=gbd_round_id,
            years=years,
            draws=draws,
            shift_function=partial(
                unordered_draw_intercept_shift, shift_from_reference=legacy_scenario_mode
            ),
        )
    else:
        logger.debug("NOT applying intercept shift on y^* results")

    y_star_outfile = FHSFileSpec(epsilon_version, f"{acause}_star.nc")
    logger.debug("Saving ``y_star``", bindings=dict(y_star_outfile=y_star_outfile))
    save_xr_scenario(
        y_star,
        y_star_outfile,
        metric=DimensionConstants.RATE_METRIC,
        space=LOG_SPACE,
    )


def _is_ntd(acause: str, gbd_round_id: int, session: Session) -> bool:
    ntd_cause_set = get_cause_set(
        session=session,
        strategy_id=CauseConstants.NTD_STRATEGY_ID,
        gbd_round_id=gbd_round_id,
    )

    return acause in ntd_cause_set[DimensionConstants.ACAUSE]


def _get_y_star(
    y_hat: xr.DataArray,
    epsilon_hat: xr.DataArray,
    years: YearRange,
    epsilon_version: VersionMetadata,
) -> xr.DataArray:
    """Returns draws of mortality or yld rates with estimated uncertainty.

    Args:
        y_hat (xr.DataArray): Expected value of mortality or yld rates.
        epsilon_hat (xr.DataArray): expected value of error.
        years (YearRange): The forecasting time series year range.
        epsilon_version (VersionMetadata): versioning information for the epsilon data

    Returns:
        xr.DataArray: draws of mortality or yld rates with estimated uncertainty.
    """
    logger.debug("Creating ``y_star`` by adding ``y_hat`` with ``epsilon_hat``")

    draws = len(epsilon_hat.coords[DimensionConstants.DRAW])
    logger.debug(
        "Make sure ``y_hat`` has the right number of draws -- resample if needed",
        bindings=dict(expected_draws=draws),
    )
    y_hat_resampled = resample(y_hat, draws)

    # Make sure the dimensions of two dataarrays are in the same order.
    if "acause" in epsilon_hat.coords:
        try:
            epsilon_hat = epsilon_hat.squeeze("acause").drop_vars("acause")
        except KeyError:
            epsilon_hat = epsilon_hat.drop_vars("acause")
    dimension_order = list(epsilon_hat.coords.dims)
    if epsilon_version.scenario is None:  # legacy scenarios mode.
        dimension_order += [DimensionConstants.SCENARIO]

    y_hat_resampled = y_hat_resampled.transpose(*dimension_order)

    # Correlate the time series draws with modeled estimates for uncertainty.
    epsilon_hat_cleaned = _clean_data(data=epsilon_hat, epsilon_version=epsilon_version)
    y_hat_cleaned = _clean_data(data=y_hat_resampled, epsilon_version=epsilon_version)

    epsilon_correlated = correlate_draws(
        epsilon_draws=epsilon_hat_cleaned,
        modeled_draws=y_hat_cleaned,
        years=years,
    )

    return y_hat_resampled + epsilon_correlated


def _clean_data(data: xr.DataArray, epsilon_version: VersionMetadata) -> xr.DataArray:
    """Strips data.

    Strips ``acause`` and ``scenario`` dims if they exist, and expands ``sex_id`` into a one
    coord dim, if there is a ``sex_id`` point coord.

    Args:
        data (xr.DataArray): input data.
        epsilon_version (VersionMetadata): the scenario number identifying the "reference" case

    Returns:
        xr.DataArray: cleaned data.
    """
    if DimensionConstants.SCENARIO in data.dims:
        scenario_to_sel = determine_scenario_to_select(epsilon_version)
        cleaned_data = data.sel(**{DimensionConstants.SCENARIO: scenario_to_sel}, drop=True)
    else:
        cleaned_data = data.copy()

    if DimensionConstants.ACAUSE in data.dims:
        logger.debug("acause is a dimension")
        acause = cleaned_data[DimensionConstants.ACAUSE].values[0]
        cleaned_data = cleaned_data.sel(**{DimensionConstants.ACAUSE: acause}).drop_vars(
            [DimensionConstants.ACAUSE]
        )
    elif DimensionConstants.ACAUSE in cleaned_data.coords:
        logger.debug("acause is a point coordinate")
        cleaned_data = cleaned_data.drop_vars([DimensionConstants.ACAUSE])
    else:
        logger.debug("acause is NOT a dim")

    # Ensure sex-id is a dimension
    if DimensionConstants.SEX_ID in cleaned_data.dims:
        logger.debug("sex_id is a dimension")
    elif DimensionConstants.SEX_ID in cleaned_data.coords:
        logger.debug("sex_id is a point coordinate")
        cleaned_data = cleaned_data.expand_dims(DimensionConstants.SEX_ID)
    else:
        logger.debug("sex_id is not a dim")

    return cleaned_data


def _draw_epsilons(
    epsilon_past: xr.DataArray,
    draws: int,
    smoothing: List[str],
    years: YearRange,
    acause: str,
    decay: float,
    gbd_round_id: int,
    national_only: bool,
    seed: int | None,
) -> xr.DataArray:
    """Draws forecasts for epsilons.

    For all-cause, this is done by running an attenuated drift model first to remove some of
    the time trend from the epsilons. Then for all causes (including all-cause) except NTD's,
    a Pooled AR1 (i.e., and ARIMA variant) or Random walk model is used to forecast the
    remaining residuals and generate expanding uncertainty.

    Args:
        epsilon_past (xr.DataArray): Past epsilons, i.e., error of predictions based on data.
        draws (int): Number of draws to grab.
        smoothing (List[str]): Which dimensions to smooth over during the latent trend model.
        years (YearRange): The forecasting time series year range.
        acause (str): The cause to forecast epsilons for.
        decay (float): Rate at which the slope of the line decays once forecasts start.
        gbd_round_id (int): The numeric ID for the GBD round.
        national_only (bool): Whether to include subnational locations, or to include only
            nations.
        seed (Optional[int]): An optional seed to set for numpy's random number generation

    Returns:
        xr.DataArray: Epsilon predictions for past and future years.
    """
    logger.debug(
        "Sampling epsilon_hat from latent trend with cross sections",
        bindings=dict(smoothing_dims=smoothing),
    )
    if acause == CauseConstants.ALL_ACAUSE:
        # For all-cause, remove drift first, then run random walk on the remainder.
        logger.debug("Computing drift term for all cause")
        epsilon_past_no_drift = epsilon_past.sel(location_id=NO_DRIFT_LOCATIONS)
        full_loc_l = epsilon_past.location_id.values.tolist()
        with_drift_l = [x for x in full_loc_l if x not in NO_DRIFT_LOCATIONS]
        epsilon_past_with_drift = epsilon_past.sel(location_id=with_drift_l)
        drift_component_with_drift = get_decayed_drift_preds(
            epsilon_da=epsilon_past_with_drift,
            years=years,
            decay=decay,
        )
        # Remove drift for selected locations
        drift_component_no_drift = epsilon_past_no_drift * 0
        drift_component = xr.concat(
            [drift_component_with_drift, drift_component_no_drift], dim="location_id"
        )
        drift_component = drift_component.fillna(0)
        remainder = epsilon_past - drift_component.sel(year_id=years.past_years)
        dataset = xr.Dataset(dict(y=remainder.copy()))

    else:
        # If not all-cause, directly model epsilons with random walk.
        logger.debug("Computing drift term for all cause", bindings=dict(acause=acause))
        dataset = xr.Dataset(dict(y=epsilon_past.copy()))
        drift_component = xr.DataArray(0)

    location_set = get_location_set(
        gbd_round_id=gbd_round_id,
        include_aggregates=False,
        national_only=national_only,
    )[
        [
            DimensionConstants.LOCATION_ID,
            DimensionConstants.REGION_ID,
            DimensionConstants.SUPER_REGION_ID,
        ]
    ]
    dataset.update(location_set.set_index(DimensionConstants.LOCATION_ID).to_xarray())

    if acause == CauseConstants.ALL_ACAUSE:
        logger.debug(
            "All cause y^* has drift component",
            bindings=dict(latent_trend_model=RandomWalk.__name__),
        )
        model_obj = RandomWalk(
            dataset=dataset,
            years=years,
            draws=draws,
            seed=seed,
        )
        model_obj.fit()
        predictions = model_obj.predict()
        epsilon_hat = drift_component + predictions
    else:
        # If not all-cause, use a pooled random walk whose location pooling dimension is based
        # on cause level.
        logger.debug(
            "{acause} y^* does NOT have drift component",
            bindings=dict(latent_trend_model=PooledRandomWalk.__name__, acause=acause),
        )
        model_obj = PooledRandomWalk(
            dataset=dataset,
            years=years,
            draws=draws,
            dims=smoothing,
            seed=seed,
        )
        model_obj.fit()
        epsilon_hat = model_obj.predict()

    return epsilon_hat


def _get_y_past(acause: str, years: YearRange, past_version: VersionMetadata) -> xr.DataArray:
    """Gets the raw data for past years.

    Past data is saved in normal rate space. The past data is returned in log rate space.

    Args:
        acause (str): Short name of the target acause to aggregate to.
        years (YearRange): Forecasting time series year range.
        gbd_round_id (int): The numeric ID for the GBD round.
        past_version (VersionMetadata): The version containing predictions for past years.

    Returns:
        xr.DataArray: The expected value of the cause specific mortality or yld rate.
    """
    y_hat_past_file = FHSFileSpec(past_version, f"{acause}_hat.nc")

    logger.debug(
        "Reading in past data",
        bindings=dict(
            y_hat_past_file=y_hat_past_file,
            past_start=years.past_start,
            past_end=years.past_end,
        ),
    )

    y_past = (
        open_xr_scenario(y_hat_past_file)
        .sel(**{DimensionConstants.YEAR_ID: years.past_years})
        .mean(DimensionConstants.DRAW)
    )

    return y_past


def determine_scenario_to_select(version_metadata: VersionMetadata) -> int:
    """Return either the ``version_metadata`` scenario or the reference scenario."""
    scenario_to_sel = (
        version_metadata.scenario
        if version_metadata.scenario is not None
        else ScenarioConstants.REFERENCE_SCENARIO_COORD
    )
    return scenario_to_sel

"""Stage 2: forecasting past ccf using MRBRT."""
import gc
import logging
from typing import List, Union

import numpy as np
import pandas as pd
import xarray as xr
import xskillscore
from fhs_lib_file_interface.lib.versioning import Versions
from fhs_lib_model.lib.constants import ModelConstants
from fhs_lib_model.lib.model import MRBRT
from fhs_lib_year_range_manager.lib.year_range import YearRange
from mrtool import MRData
from scipy.special import expit
from statsmodels.tsa.arima.model import ARIMA, ARIMAResults

from fhs_pipeline_fertility.lib import model_strategy
from fhs_pipeline_fertility.lib.constants import (
    CCF_BOUND_TOLERANCE,
    CCF_LOWER_BOUND,
    CCF_UPPER_BOUND,
    COHORT_AGE_END,
    COVARIATE_START_AGE,
    PAST_MET_NEED_YEAR_START,
    REFERENCE_SCENARIO_COORD,
    DimensionConstants,
    StageConstants,
)
from fhs_pipeline_fertility.lib.input_transform import get_covariate_data
from fhs_pipeline_fertility.lib.stage_3 import ordered_draw_intercept_shift

LOGGER = logging.getLogger(__name__)


def ensemble_forecast_ccf(
    ccfx: xr.DataArray,
    last_cohort: int,
    versions: Versions,
    gbd_round_id: int,
    years: YearRange,
    draws: int,
    locations_df: pd.DataFrame,
    ages_df: pd.DataFrame,
) -> xr.DataArray:
    """Ensemble forecast of ccf.

    If ensemble is True, run submodels with different number of covariate
    counts, and then equally sample from said submodels.

    Args:
        ccf_past (xr.DataArray): past ccf draws.
        last_cohort (int): last completed cohort.
        versions (Versions): All relevant input versions.
        gbd_round_id (int): gbd round id.
        years (YearRange): past_start:forecast_start:forecast_end
            in ``cohort`` space.  Only forecast_end will be used.
        draws (int): number of draws to keep.
        locations_df (pd.DataFrame): all locations metadata.

    Returns:
        (xr.DataArray): forecasted ccf.
    """
    das = []
    fit_summaries = []
    n_submodels = len(StageConstants.ENSEMBLE_COV_COUNTS)
    n_sub_draws = int(np.round(draws / n_submodels))

    # in each iteration we run model with different number of covariates
    for i, n_cov in enumerate(StageConstants.ENSEMBLE_COV_COUNTS):
        keepers = StageConstants.ORDERED_COVARIATES[:n_cov]  # covariates needed
        keepers.append("asfr")  # always keep this
        # essentially we make a mock Versions object here
        # v should look something like "{epoch}/{stage}/{version}"
        submodel_versions = Versions(
            *[v for v in versions.version_strs() if v.split("/")[1] in keepers]
        )
        # unfortunately we run the whole gamut, with all draws, and then subset draws
        sub_da, fit_summary = forecast_ccf(
            ccfx,
            last_cohort,
            submodel_versions,
            gbd_round_id,
            years,
            draws,
            locations_df,
            ages_df,
        )

        # now subset draws
        if i < n_submodels - 1:
            sub_draw_range = range(i * n_sub_draws, (i + 1) * n_sub_draws)
        else:
            sub_draw_range = range(i * n_sub_draws, draws)

        sub_da = sub_da.sel(draw=sub_draw_range)

        das.append(sub_da)
        fit_summaries.append(fit_summary)

    ccf_future = xr.concat(das, dim=DimensionConstants.DRAW)

    return ccf_future, fit_summaries


def forecast_ccf(
    ccfx: xr.DataArray,
    last_cohort: int,
    versions: Versions,
    gbd_round_id: int,
    years: YearRange,
    draws: int,
    locations_df: pd.DataFrame,
    ages_df: pd.DataFrame,
) -> xr.DataArray:
    """Forecast ccf using MRBRT.

    Forecasts ccf in three steps:
    1.) fit on the past means (ccf and covariates).
    2.) use (1) to obtain fitted past mean and mrbrt magic to get future draws.
    3.) take results from (2), run arima, to obtain past/future draws.

    Args:
        ccf_past (xr.DataArray): past ccf draws.
        last_cohort (int): last completed cohort.
        versions (Versions): All relevant input versions.
        gbd_round_id (int): gbd round id.
        years (YearRange): past_start:forecast_start:forecast_end
            in ``cohort`` space.  Only forecast_end will be used.
        draws (int): number of draws to keep.
        locations_df (pd.DataFrame): all locations metadata.

    Returns:
        (xr.DataArray): forecasted ccf.
    """
    # prep and subset data to get ccf50
    ccf_past = ccfx.sel(age=COHORT_AGE_END).drop(DimensionConstants.AGE)
    # to standardize across covariates, we start analyzing past cohorts from
    # cohort_id = PAST_MET_NEED_YEAR_START (1970) - COVARIATE_START_AGE (25)
    #           = 1945, to cohort_id = last_cohort
    first_cohort = PAST_MET_NEED_YEAR_START - COVARIATE_START_AGE
    ccf_past = ccf_past.sel(cohort_id=range(first_cohort, last_cohort + 1))
    ccf_past.name = StageConstants.CCF

    # it turns out that mrbrt in stage 2 only takes year_id for time axis,
    # and so we must rename cohort_id to year_id.
    ccf_past = ccf_past.rename({DimensionConstants.COHORT_ID: DimensionConstants.YEAR_ID})

    # Now we prep the covariate data.
    # research decision: fit the past on national locations
    location_ids_fit = locations_df.query("level == 3").location_id.to_list()

    # combining past & future, we need the following years from covariates
    covariate_years_needed = list(
        range(
            min(ccf_past[DimensionConstants.YEAR_ID].values) + COVARIATE_START_AGE,
            years.forecast_end - COHORT_AGE_END + COVARIATE_START_AGE + 1,
        )
    )

    # infer the covariates (ordered) we have, based on the python call inputs
    covariates = [
        cov for cov in StageConstants.ORDERED_COVARIATES if cov in versions["past"].keys()
    ]

    # collect covariate data into a list
    covariate_data_list = _get_list_of_covariate_data(
        covariates,
        gbd_round_id,
        versions,
        covariate_years_needed,
        draws=draws,
        location_ids=ccf_past[DimensionConstants.LOCATION_ID].values.tolist(),
        ages_df=ages_df,
    )

    # pull from model_strategy.py some objects needed for running MRBRT
    Model, OverallModel_node_models, study_id_cols = get_mrbrt_model_objects()

    # filter cov_models for only the covariates for which we have data
    cov_models = [
        cov
        for cov in OverallModel_node_models[0].cov_models
        if cov.name in covariates or cov.name == "intercept"
    ]

    # research decision: take mean before logit transform
    ccf_past_mean_logit = logit_bounded(
        ccf_past.mean(DimensionConstants.DRAW),
        lower=CCF_LOWER_BOUND,
        upper=CCF_UPPER_BOUND,
        tol=CCF_BOUND_TOLERANCE,
    )

    # define mrbrt model object
    LOGGER.info("Initiatializing MRBRT model object....")

    se_col_name = str(ccf_past.name) + ModelConstants.STANDARD_ERROR_SUFFIX

    def df_func(df: pd.DataFrame) -> pd.DataFrame:
        # fit on nationals only, so df with only past years is filtered
        if years.forecast_start not in df[DimensionConstants.YEAR_ID].values:
            df = df.query(f"{DimensionConstants.LOCATION_ID} in {location_ids_fit}")
        df[se_col_name] = 1  # set this way so mrbrt ignores random effects
        df["intercept"] = 1  # Peng: add if "intercept" in LinearCovModel
        return df

    ccf_past_start = int(ccf_past[DimensionConstants.YEAR_ID].min())
    ccf_forecast_start = int(ccf_past[DimensionConstants.YEAR_ID].max()) + 1
    ccf_forecast_end = years.forecast_end - COHORT_AGE_END
    ccf_years = YearRange(ccf_past_start, ccf_forecast_start, ccf_forecast_end)

    # MRBRT class requires past_data to have scenario=0 dim
    mrbrt = Model(
        past_data=ccf_past_mean_logit.expand_dims(scenario=[REFERENCE_SCENARIO_COORD]),
        years=ccf_years,
        draws=draws,
        cov_models=cov_models,
        study_id_cols=study_id_cols,
        covariate_data=covariate_data_list,
        df_func=df_func,
        index_cols=DimensionConstants.CCF_INDEX_DIMS,
    )

    LOGGER.info("Fitting....")

    mrbrt.fit(outer_max_iter=500)

    fit_summary = mrbrt.model_instance.summary()

    # NOTE make sure the kwargs are specified correctly
    LOGGER.info("Done fitting.  Now making prediction")

    # get fitted past means, and future draws from var/covar matrix.
    np.random.seed(gbd_round_id)
    df_past_mean, df_future = create_ccf_future_uncertainty(mrbrt, ccf_years)

    # the subsequent arima code wants column names "observed" and "predicted".
    # we already have "predicted", now we need to rename "ccf" to "observed"
    df_past_mean = df_past_mean.rename(columns={mrbrt._orig_past_data.name: "observed"})
    df_future = df_future.rename(columns={mrbrt._orig_past_data.name: "observed"})

    del mrbrt
    gc.collect()

    # perform arima using logit-space mean-level residual
    ccf_future_arima_logit = residual_arima_by_locations(
        df_past_mean,
        df_future,
        ccf_years,
        arima_attenuation_end_year=StageConstants.STAGE_2_ARIMA_ATTENUATION_END_YEAR,
    )

    # to bring back to normal space
    ccf_future = expit_bounded(
        ccf_future_arima_logit, lower=CCF_LOWER_BOUND, upper=CCF_UPPER_BOUND
    )

    del ccf_future_arima_logit
    gc.collect()

    # now ordered-draw intercept-shift in normal space
    ccf_future = ordered_draw_intercept_shift(ccf_future, ccf_past, ccf_years)

    ccf_future = ccf_future.where(ccf_future >= 0).fillna(0)  # safeguard

    # last bit of clean up before returning
    ccf_future = ccf_future.rename({DimensionConstants.YEAR_ID: DimensionConstants.COHORT_ID})

    ccf_future = ccf_future.sel(
        cohort_id=range(ccf_years.forecast_start, ccf_years.forecast_end + 1)
    )
    ccf_future.name = StageConstants.CCF

    # now compute some skills and attach to fit_summary (1-tuple of dataframe)
    df_past_mean = df_past_mean.set_index(
        [DimensionConstants.LOCATION_ID, DimensionConstants.YEAR_ID]
    )
    observed = df_past_mean["observed"].to_xarray()
    predicted = df_past_mean["predicted"].to_xarray()
    fit_summary[0]["rmse"] = float(xskillscore.rmse(observed, predicted))
    fit_summary[0]["r2"] = float(xskillscore.r2(observed, predicted))

    return ccf_future, fit_summary


def get_mrbrt_model_objects() -> tuple:
    """Coarse-grain the obtainment of model-related objects for MRBRT.

    Pull from model_strategy.py for mrbrt-specific paramters.

    Returns:
        (tuple): objects useful for running MRBRT forecast
    """
    stage_model_parameters = model_strategy.MODEL_PARAMETERS[StageConstants.CCF]
    model_parameters = stage_model_parameters[StageConstants.CCF]

    (
        Model,
        processor,
        _,
        OverallModel_node_models,
        study_id_cols,
        scenario_quantiles,
    ) = model_parameters

    return Model, OverallModel_node_models, study_id_cols


def _get_list_of_covariate_data(
    covariates: List[str],
    gbd_round_id: int,
    versions: Versions,
    covariate_years_needed: List[int],
    draws: int,
    location_ids: List[int],
    ages_df: pd.DataFrame,
) -> List[xr.DataArray]:
    """Helper function for forecast_ccf() to collect covariate data.

    Args:
        covariates (List[str]): names of covariates.
        gbd_round_id (int): gbd round id.
        versions (Versions): object containing all input/output
            versions metadata.
        covariate_years_needed (List[int]): list of year_ids needed
            from all covariates.
        draws (int): number of draws for run.
        location_ids (List[int]): list of location_ids needed
            from all covariates.
        ages_df (pd.DataFrame): all age group-related metadata.

    Returns:
        (list[xr.DataArray]): list of covariate data, each containing both
            past and future.

    """
    covariate_data_list = []

    for cov_name in covariates:
        past_version = versions["past"][cov_name]
        future_version = versions["future"][cov_name]
        future_scenario = versions["future"].get_version_metadata(cov_name).scenario

        if future_scenario is None:  # then defaults to reference
            future_scenario = REFERENCE_SCENARIO_COORD

        covariate_age_group_id = _map_cov_name_to_age_group_id(cov_name, ages_df)

        # this pulls both past and future and returns them in one big object
        cov_data = get_covariate_data(
            stage=cov_name,
            future_version=future_version,
            past_version=past_version,
            year_ids=covariate_years_needed,
            gbd_round_id=gbd_round_id,
            location_ids=location_ids,
            scenario=future_scenario,
            draws=draws,
            age_group_ids=covariate_age_group_id,
        )

        # MRBRT._convert_covariates expects a scenario = 0 dim
        cov_data = cov_data.expand_dims(scenario=[REFERENCE_SCENARIO_COORD])

        # shift ccf covariates year_id to match their cohorts.
        cov_data[DimensionConstants.YEAR_ID] = (
            cov_data[DimensionConstants.YEAR_ID] - COVARIATE_START_AGE
        )

        covariate_data_list.append(cov_data)

    return covariate_data_list


def _map_cov_name_to_age_group_id(cov_name: str, ages_df: pd.DataFrame) -> int:
    """Map the given covariate name to the age group id(s) it contains.

    Args:
        cov_name (str): name of covariate.
        ages_df (pd.DataFrame): contains age group-related metadata.

    Returns:
        (int): age group id for this particular covariate.
    """
    # these are hard-coded quirks related to each upstream file
    if cov_name == "urbanicity":
        covariate_age_group_id = 22
    elif cov_name == "u5m":
        covariate_age_group_id = 1
    else:  # age group id of 25-30 year olds
        covariate_age_group_id = int(
            ages_df.query("age_group_years_start == @COVARIATE_START_AGE")[
                DimensionConstants.AGE_GROUP_ID
            ]
        )

    return covariate_age_group_id


def create_ccf_future_uncertainty(model: MRBRT, years: YearRange) -> MRBRT:
    """Create future uncertainty.

    Modifies in-place the .prediction_df object of ``model``.

    1.) use MRBRT.predict() to make both past fit based on mean values.
    2.) use MRBRT var/covar methods to create future draws, starting with mean.
    3.) return past mean and future draws for our custom arima function.

    Args:
        model (model.MRBRT): MRBRT model object, after fit.
        years (YearRange): past_start:forecast_start:forecast_end.

    Returns:
        (MRBRT) mrbrt model with modified .prediction_df dataframe.
    """
    # first take the MEAN past ccf/covs values.  We take it from .prediction_df
    # because prediction_df has sub-national locations, whereas .combined_df
    # was used for fitting and only contains national locations.
    df_past_mean = (
        model.prediction_df.query(f"year_id in {years.past_years.tolist()}")
        .groupby(DimensionConstants.CCF_INDEX_DIMS)
        .mean()
        .reset_index()
    )

    # now introduce the pred_logit fit to the MEAN past ccf/covs.
    # model.model_instance already has fitting coefficients based on nationals.
    # But that also means model.combined_mr was made with only sub-nationals,
    # so in order to fit all (including subnats) past locations (using
    # already-obtained fitting coefficients) one must make a new MRData object
    # with this df_past_mean to run model.model_instance.predict() on.
    mr_data = MRData()
    mr_data.load_df(
        data=df_past_mean,
        col_obs=model._orig_past_data.name,
        col_obs_se=(model._orig_past_data.name + ModelConstants.STANDARD_ERROR_SUFFIX),
        col_covs=model.col_covs + model.index_cols,
        col_study_id=model.study_id_col_name,
    )
    df_past_mean["predicted"] = model.model_instance.predict(
        data=mr_data, predict_for_study=False, sort_by_data_id=True
    )

    # new future predictions (with draws)
    df_future = _create_uncertainty(model, years)  # using mean of cov draws

    # these two will be used downstream for arima
    return df_past_mean, df_future


def _create_uncertainty(model: MRBRT, years: YearRange) -> pd.DataFrame:
    """Create uncertainty using covariate means.

    Args:
        model (model.MRBRT): MRBRT model object, after fit.
        years (YearRange): past_start:forecast_start:forecast_end.

    Returns:
        (pd.DataFrame): df with new draws.
    """
    sample_size = model.draws
    beta_samples, gamma_samples = model.model_instance.sample_soln(sample_size)

    # df_future contains all MEAN future cov data, indexed by location/year
    df_future = (
        model.prediction_df[model.col_covs + model.index_cols]
        .query(f"year_id in {years.forecast_years.tolist()}")
        .groupby(DimensionConstants.CCF_INDEX_DIMS)
        .mean()
        .reset_index()
    )

    df_future = _get_draws(df_future, model, beta_samples, gamma_samples)

    # df_future is wide-by-draw, and our canonical format is long-by-draw.
    # so here we do some memory-expensive transformation.
    df_future = _wide_to_long_by_draws(df_future, model.index_cols, sample_size)

    return df_future


def _wide_to_long_by_draws(
    df: pd.DataFrame, index_cols: List[str], sample_size: int
) -> pd.DataFrame:
    """Convert wide-by-draw df to long-by-draw df.

    Args:
        df (pd.DataFrame): wide-by-draw df with "draw_{}" columns.
        index_cols (List[str]): list of index column names.
        sample_size (int): number of draws.

    Returns:
        (pd.DataFrame): long-by-draw df, with "draw" columns.
    """
    df.columns = [col.replace("draw_", "") for col in df.columns]

    df = pd.melt(
        df,
        id_vars=index_cols,
        value_vars=[str(i) for i in range(sample_size)],
        var_name=DimensionConstants.DRAW,
        value_name="predicted",
    )

    df = df.astype({DimensionConstants.DRAW: int})

    return df


def _get_draws(
    df: pd.DataFrame, model: MRBRT, beta_samples: np.ndarray, gamma_samples: np.ndarray
) -> pd.DataFrame:
    """Predict MRBRT model with uncertainty from covariance matrix.

    Args:
        df (pd.DataFrame): has future covariates, mean by draw.
        model (model.MRBRT): MRBRT object.
        beta_samples (np.ndarray): beta samples.
        gamme_samples (np.ndarray): gamma samples.

    Returns:
        (pd.DataFrame): wide-by-draw dataframe of forecast.
    """
    data_pred = MRData()
    data_pred.load_df(
        df,  # df has the future covariates, mean-by-draws
        col_covs=model.col_covs + model.index_cols,
        col_study_id=DimensionConstants.LOCATION_ID,
    )
    draw_ids = [f"draw_{i}" for i in range(model.draws)]
    result = model.model_instance.create_draws(
        data_pred, beta_samples, gamma_samples, sort_by_data_id=True
    )
    result = pd.DataFrame(result, columns=draw_ids)
    result = pd.concat([df[model.index_cols].reset_index(drop=True), result], axis=1)
    return result


# this function is also used by stage 3
def residual_arima_by_locations(
    df_past_mean: pd.DataFrame,
    df_future: pd.DataFrame,
    years: YearRange,
    arima_attenuation_end_year: Union[int, None],
) -> pd.DataFrame:
    """Introduce arima uncertainty to forecasted draws.

    For every location:

    1.) use arima(1,0,0) to fit the residual between true past mean
        and predicted past mean.
    2.) use said arima to forecast future residual.

    Args:
        df_past_mean (pd.DataFrame): data frame that contains mean past
            observed data and its fit.
        df_future (pd.DataFrame): predicted future draws, and covariates.
        years (YearRange): past_start:forecast_start:forecast_end.
            If used in stage 2, then these are probably cohort years.
            In stage 3, the years are in period space.
        arima_attenuation_end_year (Union[int, None]): last year of
            finite arima value.  None means no attenuation.

    Returns:
        (pd.DataFrame): data frame with arimaed residuals.
    """
    basic_index_dims = DimensionConstants.CCF_INDEX_DIMS  # for past/future
    arima_pasts = []
    arima_forecasts = []

    for location_id in df_past_mean[DimensionConstants.LOCATION_ID].unique():
        df_loc = df_past_mean.query(f"location_id == {location_id}")
        df_loc = df_loc.sort_values(DimensionConstants.YEAR_ID)

        if not (np.diff(df_loc[DimensionConstants.YEAR_ID].values) == 1).all():
            raise ValueError("Arima requires sequential past years.")

        # we want to compute residual = mean(true past) - mean(predicted past)
        resid = (df_loc["observed"] - df_loc["predicted"]).values

        arima = ARIMA(resid, order=(1, 0, 0), trend="n")
        arima_fit = arima.fit(method="innovations_mle")

        arima_past = arima_fit.fittedvalues

        arima_past = pd.DataFrame(
            {
                DimensionConstants.LOCATION_ID: np.repeat(location_id, len(arima_past)),
                DimensionConstants.YEAR_ID: df_loc[DimensionConstants.YEAR_ID].values,
                "residual_arima": arima_past,
            }
        )

        arima_pasts.append(arima_past)  # collect arima fits of the past

        # NOTE: this attenuation scheme is designed for ARIMA 100
        if arima_attenuation_end_year is not None:
            arima_forecast = attenuate_arima(arima_fit, years, arima_attenuation_end_year)
        else:  # normal arima forecast
            arima_forecast = arima_fit.forecast(steps=len(years.forecast_years))

        arima_forecast = pd.DataFrame(
            {
                DimensionConstants.LOCATION_ID: np.repeat(
                    location_id, len(years.forecast_years)
                ),
                DimensionConstants.YEAR_ID: years.forecast_years,
                "residual_arima": arima_forecast,
            }
        )

        arima_forecasts.append(arima_forecast)  # collect arima forecasts

    arima_past = pd.concat(arima_pasts, axis=0)
    arima_forecast = pd.concat(arima_forecasts, axis=0)

    del arima_pasts, arima_forecasts
    gc.collect()

    # add arima to mrbrt forecast to make final forecast
    arima_past = df_past_mean[basic_index_dims + ["predicted"]].merge(
        arima_past, on=basic_index_dims, how="right"
    )

    # replace predicted with arimaed predicted, and drop un-needed column
    arima_past["predicted"] = (arima_past["predicted"] + arima_past["residual_arima"]).drop(
        columns="residual_arima"
    )

    # convert to xr.DataArray
    arima_past = arima_past.set_index(basic_index_dims).to_xarray()["predicted"]

    # expand the past draw dim so we can later concat past and future
    arima_past = arima_past.expand_dims(
        draw=df_future[DimensionConstants.DRAW].unique().tolist()
    )

    # now the future part
    arima_forecast = df_future[
        basic_index_dims + ["predicted", DimensionConstants.DRAW]
    ].merge(arima_forecast, on=basic_index_dims, how="right")

    # replace predicted with arimaed predicted, and drop unused column
    arima_forecast["predicted"] = (
        arima_forecast["predicted"] + arima_forecast["residual_arima"]
    ).drop(columns="residual_arima")

    # convert to xr.DataArray
    arima_forecast = arima_forecast.set_index(
        basic_index_dims + [DimensionConstants.DRAW]
    ).to_xarray()["predicted"]

    # return both past and future in one dataarray
    return xr.concat([arima_past, arima_forecast], dim=DimensionConstants.YEAR_ID)


def logit_bounded(x: xr.DataArray, lower: float, upper: float, tol: float) -> xr.DataArray:
    r"""Compute the bounded logit transformation.

    The transformation is defined by

    ..math::
        f(x, a, b) =\
            \log \left( \frac{x_\text{trunc} - a}{b - x_\text_trunc} \right)

    where :math:`a` is the lower bound, :math:`b` is the upper bound, and
    :math:`x_\text{trunct}` is :math:`x` truncated to be in the closed interval
    :math:`[a + \text{tol}, b - \text{tol}]` (to avoid undefined logit).

    Args:
        x (xr.DataArray): data to be transformed.
        lower (float): lower bound.
        upper (float): upper bound
        tol (float): tolerance for approaching the bounds.

    Returns:
        xr.DataArray: the supplied data transformed to bounded logit space.
    """
    x = x.clip(min=lower + tol, max=upper - tol)
    return np.log((x - lower) / (upper - x))


def expit_bounded(x: xr.DataArray, lower: float, upper: float) -> xr.DataArray:
    r"""Compute the inverse of bounded logit transoformation :func:`logit_bounded`.

    Args:
        x (xr.DataArray): data to be transformed.
        lower (float): lower bound.
        upper (float): upper bound.
    """
    y = (upper - lower) * expit(x) + lower
    return y


def attenuate_arima(
    arima_fit: ARIMAResults, years: YearRange, arima_attenuation_end_year: int
) -> np.array:
    """Linearly attenuates arima forecast to 0 by input end year.

    Args:
        arima_fit (ARIMAResults): arima fit from running arima.fit().
        years (YearRange): past start:forecast start:forecast end.
            If used in stage 2, then these are likely ``cohort`` years.
        arima_attenuation_end_year (int): last year of non-zero arima.

    Returns:
        (np.array): forecasted arima values, attenuated to zero by
            the prescribed end year.
    """
    # - is from statsmodel convention
    starting_coeff = -float(arima_fit.polynomial_ar[1])
    yearly_coeff_change = -starting_coeff / (arima_attenuation_end_year - years.forecast_start)
    forecast_value = arima_fit.fittedvalues[-1]  # starting point
    arima_forecast = np.zeros(len(years.forecast_years))  # initialize to all zeros

    # linearly attenuate
    for i in range(arima_attenuation_end_year - years.forecast_start):
        arima_coeff = starting_coeff + (i * yearly_coeff_change)
        forecast_value = forecast_value * arima_coeff
        arima_forecast[i] = forecast_value

    return arima_forecast

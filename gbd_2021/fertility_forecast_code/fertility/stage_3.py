"""Fertility stage 3.

1.) Unfold forecasted CCF50 back to 5-year-group cohort fertility via
    linear mixed effects model, using education and met_need as
    fixed effect covariates, and region_id as random intercept.
2.) Interpolate the 5-year-group ASFR to get single year ASFR.
3.) Apply same ARIMA model used in stage 2 (now in period space) to single year ASFR.
"""
import gc
import itertools as it
import logging
from typing import Iterable, Tuple

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
import xarray as xr
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_file_interface.lib.versioning import Versions
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr
from fhs_lib_year_range_manager.lib.year_range import YearRange
from scipy.special import expit, logit

from fhs_pipeline_fertility.lib import stage_2
from fhs_pipeline_fertility.lib.constants import (
    COHORT_AGE_END,
    COHORT_AGE_START,
    FIVE_YEAR_AGE_GROUP_SIZE,
    OLD_TERMINAL_AGES,
    PAST_MET_NEED_YEAR_START,
    REFERENCE_SCENARIO_COORD,
    YOUNG_TERMINAL_AGES,
    DimensionConstants,
    StageConstants,
)
from fhs_pipeline_fertility.lib.input_transform import get_covariate_data

LOGGER = logging.getLogger(__name__)


def forecast_asfr_from_ccf(
    ccfx: xr.DataArray,
    ccf50_future: xr.DataArray,
    last_cohort: int,
    gbd_round_id: int,
    versions: Versions,
    locations_df: pd.DataFrame,
    ages_df: pd.DataFrame,
    years: YearRange,
) -> xr.DataArray:
    """Forecast period-space asfr using forecasted ccf and education/met_need.

    This pipeline includes four/five steps:

    1.) prepare forecasted education and met need for regression.
    2.) forecast logit(5-year-cohort-fertility / ccf50) using linear mixed
        effects model, with education and met need as covariates, region_id
        as random intercept.
    3.) interpolate between cohort 5-year age groups to get single-year
        cohort asfr.
    4.) convert cohort asfr to period-space asfr and perform arima for future
        uncertainty.

    Args:
        ccfx (xr.DataArray): past ccfx (with fill-in).  This has NaNs.
        ccf50_future (xr.DataArray): forecasted ccf50.
        last_cohort (int): last completed past cohort.
        gbd_round_id (int): gbd round id.
        versions (Versions): contains all relevant past/future versions.
        locations_df (pd.DataFrame): locations metadata.
        ages_df (pd.DataFrame): ages metadata.
        years (YearRange): past_start:forecast_start:forecast_end.

    Returns:
        (tuple[xr.DataArray]): past/future ccf50, logit(asfr/ccf50),
            education, met need.
    """
    LOGGER.info("Stage 3 step 1, prepping covariate data...")

    education, met_need = prepare_covariate_data(versions, gbd_round_id, ccf50_future, ages_df)

    LOGGER.info("Stage 3 step 2, LME regression...")
    # now begins step 2 (regression) of stage 3
    # where we separate fits between high-income and non-high-income locations
    cohort_5yr_fert_future, cohort_5yr_fert_past_fit = forecast_five_year_cohort_asfr(
        ccfx, ccf50_future, education, met_need, locations_df
    )

    del education, met_need
    gc.collect()

    LOGGER.info("Stage 3 step 3, interpolation to single-year asfr...")
    # step 3 is the interpolation to single year asfr in cohort space
    asfr_single_year, asfr_single_year_past_mean_fit = interpolate_for_single_year_cohort_asfr(
        cohort_5yr_fert_future, cohort_5yr_fert_past_fit
    )

    del cohort_5yr_fert_future, cohort_5yr_fert_past_fit
    gc.collect()

    LOGGER.info("Stage 3 step 4, arima of single year asfr...")
    # step 4 is the arima of single year asfr
    asfr_single_year = arima_single_year_asfr(
        ccfx,
        asfr_single_year_past_mean_fit,
        asfr_single_year,
        years,
        gbd_round_id,
        versions,
        ages_df,
    )

    return asfr_single_year


def prepare_covariate_data(
    versions: Versions, gbd_round_id: int, ccf50_future: xr.DataArray, ages_df: pd.DataFrame
) -> Tuple[xr.DataArray, xr.DataArray]:
    """Prepare education and met_need covariates for logit(asfr / ccf50) regression.

    Args:
        versions (Versions): input versions metadata.
        gbd_round_id (int): gbd round id.
        ccf50_future (xr.DataArray): forecasted ccf, used to help inform the
            reading-in of covariate files.
        ages_df (pd.DataFrame): age-related metadata.

    Returns:
        (Tuple[xr.DataArray, xr.DataArray]): two-tuple of education and met_need.
    """
    covariates = StageConstants.STAGE_3_COVARIATES
    covariate_list = []

    for cov_name in covariates:
        past_version = versions["past"][cov_name]
        future_version = versions["future"][cov_name]
        future_scenario = versions["future"].get_version_metadata(cov_name).scenario

        if future_scenario is None:  # then defaults to reference
            future_scenario = REFERENCE_SCENARIO_COORD

        # TODO confirm the 41 is necessary
        cov_data = get_covariate_data(
            stage=cov_name,
            future_version=future_version,
            past_version=past_version,
            year_ids=list(
                range(PAST_MET_NEED_YEAR_START, int(ccf50_future.cohort_id.max()) + 41)
            ),
            gbd_round_id=gbd_round_id,
            location_ids=ccf50_future[DimensionConstants.LOCATION_ID].values.tolist(),
            scenario=future_scenario,
            draws=len(ccf50_future[DimensionConstants.DRAW].values.tolist()),
            age_group_ids=ages_df[DimensionConstants.AGE_GROUP_ID].values.tolist(),
        )

        # we chose to do things at mean level
        cov_data = _convert_incremental_covariates_to_cohort_space(cov_data, ages_df)

        covariate_list.append(cov_data)

    education, met_need = covariate_list

    return education, met_need


def forecast_five_year_cohort_asfr(
    ccfx: xr.DataArray,
    ccf50_future: xr.DataArray,
    education: xr.DataArray,
    met_need: xr.DataArray,
    locations_df: pd.DataFrame,
) -> Tuple[xr.DataArray, xr.DataArray]:
    """Fit and forecast logit(5-year-cohort-fertility / ccf50).

    Everything in cohort space.

    Start with ccfx, labeled by single years, to compute  cohort fertility
    by 5-year age groups (sometimes referred to as cohort asfr).

    Then fit these 5-year cohort fertility rates over cohort_id,
    using linear mixed effects model with education & met need
    as fixed effect covariates, and region_id as random intercept.

    Returns predicted future draws and past means of cohort asfr.

    Args:
        ccfx (xr.DataArray): past ccfx, by single years.
        ccf50_future (xr.DataArray): forecasted ccf50.
        education (xr.DataArray): education, past and future.  Probably does
            not go as far back as ccf due to data limitations.
        met_need (xr.DataArray): met_need, past and future.  Probably does not
            go as far back as ccf due to data limitations.
        locations_df (pd.DataFrame): location metadata.

    Returns:
        (Tuple[xr.DataArray, xr.DataArray]): predicted future draws and
            fitted past means of cohort asfr.
    """
    # first compute the 5-year-age-group cohort fertility (cohort asfr)
    cohort_5yr_fert = _make_5yr_cohort_fertility_from_ccfx(ccfx)

    # need some additional
    last_past_cohort = ccf50_future[DimensionConstants.COHORT_ID].values.min() - 1
    # this will make a ccf50 with past through future.
    ccf = ccfx.sel(age=COHORT_AGE_END, drop=True).combine_first(ccf50_future)

    # one fit/predict for high-income locations
    laf_hi, laf_hi_mean_past_fit = _age_group_specific_fit_and_predict(
        cohort_5yr_fert,
        ccf,
        education,
        met_need,
        last_past_cohort,
        locations_df.query("super_region_name == 'High-income'"),
    )
    # another fit/predict for non-high-income locations
    laf_nhi, laf_nhi_mean_past_fit = _age_group_specific_fit_and_predict(
        cohort_5yr_fert,
        ccf,
        education,
        met_need,
        last_past_cohort,
        locations_df.query("super_region_name != 'High-income'"),
    )
    # concat to get back all locations
    laf_prediction = xr.concat([laf_hi, laf_nhi], dim=DimensionConstants.LOCATION_ID)

    laf_mean_past_fit = xr.concat(
        [laf_hi_mean_past_fit, laf_nhi_mean_past_fit], dim=DimensionConstants.LOCATION_ID
    )

    # intercept-shift within fit means asfr fractions won't sum to 1 over age groups
    ccf_fractions_future = expit(laf_prediction)  # needs renormalization
    ccf_fractions_future = ccf_fractions_future / ccf_fractions_future.sum(
        DimensionConstants.AGE
    )  # renormalization of fractional ccf

    cohort_5yr_fert_future = ccf * ccf_fractions_future  # drops past

    # do the same renormalization for past mean fit
    # but keep in mind that there are nans in very early cohorts, in the later
    # years, due to the covariates past data starting later.  Hence we must
    # remove those cohorts with nans first, otherwise they'd be
    # disproportionally renormalized.
    # complete cohorts start from first met_need year - cohort_age_start,
    # and end at last_past_cohort
    laf_mean_past_fit = laf_mean_past_fit.sel(
        cohort_id=range(PAST_MET_NEED_YEAR_START - COHORT_AGE_START, last_past_cohort + 1)
    )
    ccf_frac_mean_past_fit = expit(laf_mean_past_fit)
    # same fallacy as earlier
    ccf_frac_mean_past_fit = ccf_frac_mean_past_fit / ccf_frac_mean_past_fit.sum(
        DimensionConstants.AGE
    )
    cohort_5yr_fert_past_fit = (
        ccf.sel(cohort_id=ccf_frac_mean_past_fit[DimensionConstants.COHORT_ID]).mean(
            DimensionConstants.DRAW
        )
        * ccf_frac_mean_past_fit
    )

    return cohort_5yr_fert_future, cohort_5yr_fert_past_fit


def _make_5yr_cohort_fertility_from_ccfx(ccfx: xr.DataArray) -> xr.DataArray:
    """Compute cohort fertilility of 5-year age grouops from ccfx (single years).

    Args:
        ccfx (xr.DataArray): past ccfx, by single year ages.

    Returns:
        (xr.DataArray): cohort fertility by 5-year age groups.
            Some cells will be NaN because the later cohorts have not
            completed their fertile life yet.  For example, the 2015
            cohort is only 18 years old by 2023, and hence won't even
            have the 15-20 age group fertility yet.
    """
    # because we want asfr of 5-year age groups, we need to take the diff
    # of ccfx between end-ages.  The starting point is the 15-19 age group,
    # and we assume that asfr is 0 before 15.
    age_group_end_ages = range(COHORT_AGE_START + 4, COHORT_AGE_END + 1, 5)

    # because ccf is cumulative fertility, we use .diff here to get the 5-year
    # cohort fertility values we want to fit/predict.
    fert_1st_age_group = ccfx.sel(age=age_group_end_ages[0])
    fert_later_age_groups = ccfx.sel(age=age_group_end_ages).diff(DimensionConstants.AGE)

    # This makes the 5-year cohort fertility whose fractions to ccf we fit/predict
    cohort_5yr_fert = xr.concat(
        [fert_1st_age_group, fert_later_age_groups], dim=DimensionConstants.AGE
    )  # some cells will be NaNs due to cohorts being yet too young.

    return cohort_5yr_fert


def interpolate_for_single_year_cohort_asfr(
    cohort_5yr_fert_future: xr.DataArray, cohort_5yr_fert_past_fit: xr.DataArray
) -> Tuple[xr.DataArray, xr.DataArray]:
    """Interpolate to get single year cohort asfr.

    Args:
        cohort_5yr_fert_future (xr.DataArray): future cohort 5-year fertility,
            with draws.
        cohort_5yr_fert_past_fit (xr.DataArray): past cohort 5-year fertility
            mean fit.

    Returns:
        (Tuple[xr.DataArray, xr.DataArray]): future single year asfr draws and
            past single year asfr mean fit.
    """
    interp_ages = range(COHORT_AGE_START, COHORT_AGE_END + 1)

    asfr_single_year = _interp_single_year_rates_from_five_year_age_groups(
        cohort_5yr_fert_future, interp_ages
    )

    asfr_single_year_past_fit = _interp_single_year_rates_from_five_year_age_groups(
        cohort_5yr_fert_past_fit, interp_ages
    )

    return asfr_single_year, asfr_single_year_past_fit


def _interp_single_year_rates_from_five_year_age_groups(
    da_5yr: xr.DataArray, interp_ages: Iterable[int]
) -> xr.DataArray:
    """Linearly interpolate 1-year rates from 5-year rates.

    Uses spline-linear interpolation.

    Args:
        da_5yr (xr.DataArray): five-year rates.
        interp_ages (Iterable[int]): single years to interpolate.

    Returns:
        (xr.DataArray): single year rates.
    """
    # first compute mean single year asfr
    da_single_year = da_5yr / FIVE_YEAR_AGE_GROUP_SIZE  # hence "mean"
    # now reassign these mean values to some mid-age-group points
    da_single_year = da_single_year.assign_coords(
        age=(da_single_year[DimensionConstants.AGE] + 1) - (FIVE_YEAR_AGE_GROUP_SIZE / 2.0)
    )

    da_single_year = expand_dimensions(
        da_single_year, age=[COHORT_AGE_START - 1, COHORT_AGE_END + 1], fill_value=0
    )

    da_single_year = da_single_year.interp(age=interp_ages, method="slinear")

    # renormalize so sum over age is equal before/after interpolation.
    da_single_year = da_single_year * (
        da_5yr.sum(DimensionConstants.AGE) / da_single_year.sum(DimensionConstants.AGE)
    )

    return da_single_year


def arima_single_year_asfr(
    ccfx: xr.DataArray,
    asfr_past_mean_fit: xr.DataArray,
    asfr_future: xr.DataArray,
    years: YearRange,
    gbd_round_id: int,
    versions: Versions,
    ages_df: pd.DataFrame,
) -> xr.DataArray:
    """Apply ARIMA to single year period-space asfr.

    Fit ARIMA 100 between observed past and predicted past, in period space,
    and then add the forecast to the period space future.

    Starting with cohort space inputs (ccfx, asfr_past_mean_fit, and asfr_future):
    1.) Make period-space past/future asfrs using the cohort inputs.  ccfx contains
        all observed past period-space data, but asfr_past_mean_fit is missing a
        triangle of past predicted values.  Use asfr_future to fill in said missing
        values.
    2.) Now in period space, for every location-age, fit past/forecast residual.
    3.) Add forecasted residual to forecast.
    4.) Infer terminal (10-15, 50-54) single year asfrs.
    5.) Ordered-draw intercept-shift with the past.

    Args:
        ccfx_past (xr.DataArray): past single-year ccfx, has draws.
        asfr_past_mean_fit (xr.DataArray): fitted past single-year cohort asfr.
        asfr_future (xr.DataArray): future single-year cohort asfr, with draws.
        years (YearRange): past_start:forecast_start:forecast_end.
        gbd_round_id (int): gbd round id.
        versions (Versions): contains all relevant input/output versions.
        ages_df (pd.DataFrame): age metadata.

    Returns:
        (xr.DataArray): post-arima future single-year asfr.
    """
    df_past, asfr_past_mean_fit = _prep_past_cohort_asfr_da_into_period_asfr_df(
        ccfx, asfr_past_mean_fit, asfr_future
    )

    # Now prep for the future draws.
    df_future, da_predicted_last_past_year = _prep_future_cohort_asfr_da_into_period_asfr_df(
        asfr_past_mean_fit, asfr_future, years
    )

    # now ready to make arima call for every age year
    age_da_list = []

    for age in df_future[DimensionConstants.AGE].unique():
        age_df_past = df_past.query(f"{DimensionConstants.AGE} == {age}")
        arima_past_years = age_df_past[DimensionConstants.YEAR_ID].unique().tolist()
        # age_df_past might have some years into the future already (stage 1)
        age_df_future = df_future.query(
            f"{DimensionConstants.AGE} == {age} & "
            f"{DimensionConstants.YEAR_ID} not in {arima_past_years}"
        )

        # due to cohort-to-period space conversion, every age has a different
        # forecast start year, so we make a special YearRange object for it
        arima_years = YearRange(
            years.past_start, max(arima_past_years) + 1, years.forecast_end
        )
        # this function returns past and future together, both arimaed
        past_and_future_da = stage_2.residual_arima_by_locations(
            age_df_past, age_df_future, arima_years, arima_attenuation_end_year=None
        )

        # take the arimaed future years
        future_da = past_and_future_da.sel(year_id=years.forecast_years)

        # need the "predicted" last past year, post arima and all that.
        # as the anchor for intercept-shift.
        # We'll compute the arima residual added to the first future year,
        # and add it to the pre-arima predicted last past year, as the anchor.
        pre_arima_first_future_year = (
            df_future.query(
                f"{DimensionConstants.AGE} == {age} & "
                f"{DimensionConstants.YEAR_ID} == {years.forecast_start}"
            )
            .set_index(DimensionConstants.SINGLE_YEAR_ASFR_INDEX_DIMS)["predicted"]
            .to_xarray()
        )

        resid_arima_first_future_year = past_and_future_da.sel(
            year_id=years.forecast_start, drop=True
        ) - pre_arima_first_future_year.sel(age=age, year_id=years.forecast_start, drop=True)

        # now make a future_da that has an arima-residual-added last past year
        future_da = xr.concat(
            [
                da_predicted_last_past_year.sel(age=age, drop=True)
                + resid_arima_first_future_year,
                future_da,
            ],
            dim=DimensionConstants.YEAR_ID,
        ).sortby(DimensionConstants.YEAR_ID)

        future_da[DimensionConstants.AGE] = age

        age_da_list.append(future_da)

    # need to expit back to normal space.
    asfr_future = expit(
        xr.concat(age_da_list, dim=DimensionConstants.AGE).sortby(DimensionConstants.AGE)
    )  # this overwrites the input variable

    # now infer and append the terminal ages (10-14, 50-54)
    asfr_future, asfr_past = infer_and_append_terminal_ages(
        gbd_round_id, versions, years.past_end, asfr_future
    )

    # we also need to intercept-shift to the past
    ishift_years = YearRange(
        years.past_start, years.forecast_start, years.forecast_end - COHORT_AGE_END
    )

    asfr_future = ordered_draw_intercept_shift(asfr_future, asfr_past, ishift_years)

    # this is a safeguard that sets all negatives to 0
    asfr_future = asfr_future.where(asfr_future >= 0).fillna(0)

    asfr_future = asfr_future.sel(year_id=years.forecast_years)

    return asfr_future


def _prep_past_cohort_asfr_da_into_period_asfr_df(
    ccfx: xr.DataArray, asfr_past_mean_fit: xr.DataArray, asfr_future: xr.DataArray
) -> Tuple[pd.DataFrame, xr.DataArray]:
    """Prepare past period space observed/predicted mean values in pd.DataFrame.

    Start with cohort space true/predicted values in past, make period-space
    dataframe.  Need to use some forecasted values to fill in for the past
    predicted values.

    Args:
        ccfx (xr.DataArray): True past cohort space single year fertility.
            Contains all past values needed.  Has draws.
        asfr_past_mean_fit (xr.DataArray): mean past fit of cohort-space
            single year fertility.  No draw dimension.  For young cohorts,
            some past values are missing and need to be filled in with
            forecasted values, which do have draws.
        asfr_future (xr.DataArray): forecasted cohort-space single year
            fertility.  When converted to period space, contains some
            past values that will be transfered to asfr_past_mean_fit.

    Returns:
        (Tuple[pd.DataFrame, xr.DataArray]): dataframe of past mean
            observed/predicted values (logit) in period space.
            Also return xarray of the past mean fit values in cohort space.
    """
    # we call the past draws from stage 1 "true past"
    asfr_true_past = xr.concat(
        [ccfx.sel(age=COHORT_AGE_START), ccfx.diff(DimensionConstants.AGE)],
        dim=DimensionConstants.AGE,
    )  # remember this is in cohort space, and still has many nans

    asfr_true_past.name = "observed"  # because these values come from GBD
    asfr_past_mean_fit.name = "predicted"  # because we fitted these earlier

    # need to fill in for the past mean fit to match the true past in year id.
    first_missing_cohort = int(asfr_past_mean_fit[DimensionConstants.COHORT_ID].max()) + 1
    last_missing_cohort = int(asfr_true_past[DimensionConstants.COHORT_ID].max())

    # use these forecasted cohort values to fill in for past predictions
    asfr_past_draw_fit = asfr_future.sel(
        location_id=asfr_past_mean_fit[DimensionConstants.LOCATION_ID],
        cohort_id=range(first_missing_cohort, last_missing_cohort + 1),
    ).mean(DimensionConstants.DRAW)

    # this fills the predicted past vlaues
    asfr_past_mean_fit = asfr_past_mean_fit.combine_first(asfr_past_draw_fit)

    # we arima fit the past residual: past mean - past prediction
    asfr_true_past_mean = asfr_true_past.mean(DimensionConstants.DRAW)

    # now we prep the past mean & fit for residual_arima_by_locations().
    # merge into a dataset is a way to align the coordinates.
    ds_past = xr.merge([asfr_true_past_mean, asfr_past_mean_fit], join="inner")
    df_past = ds_past.to_dataframe().reset_index().dropna()  # don't need NaNs

    # Now start converting to period space.
    df_past[DimensionConstants.YEAR_ID] = (
        df_past[DimensionConstants.COHORT_ID] + df_past[DimensionConstants.AGE]
    )

    # some last bits of pruning before arima
    df_past = df_past.drop(DimensionConstants.COHORT_ID, axis=1)  # need no more
    df_past = df_past.query(f"{DimensionConstants.YEAR_ID} >= {PAST_MET_NEED_YEAR_START}")

    df_past["observed"] = logit(df_past["observed"])  # fit arima in logit
    df_past["predicted"] = logit(df_past["predicted"])

    return df_past, asfr_past_mean_fit  # df_past (period), asfr_past_mean_fit (cohort)


def _prep_future_cohort_asfr_da_into_period_asfr_df(
    asfr_past_mean_fit: xr.DataArray, asfr_future: xr.DataArray, years: YearRange
) -> Tuple[pd.DataFrame, xr.DataArray]:
    """Make future period-space ASFR from cohort space ASFRs.

    Need "predicted" last past year draws as anchor for intercept-shifting.
    asfr_future (in cohort space) already has most of it, except for age 49,
    which is part of last complete cohort and hence has only mean fit.
    So we fill in asfr_future with asfr_past_mean_fit (repeat age 49 draws),
    and will later prune the year_ids of both past and future to be consistent.

    Args:
        asfr_past_mean_fit (xr.DataArray): mean past fit of cohort-space
            single year fertility.  No draw dimension.  For young cohorts,
            some past values are missing and need to be filled in with
            forecasted values, which do have draws.
        asfr_future (xr.DataArray): forecasted cohort-space single year
            fertility.  When converted to period space, contains some
            past values that will be transfered to asfr_past_mean_fit.
        years (YearRange): past_start:forecast_start:forecast_end

    Returns:
        (pd.DataFrame): dataframe of forecasted values (logit) in period space.
            Also return xarray of last past year's values (logit) for
            downstream intercept-shift.

    """
    asfr_future = asfr_future.combine_first(asfr_past_mean_fit)

    # also need to transform the future data a bit before arima call
    asfr_future = logit(asfr_future)  # arima in logit space
    asfr_future.name = "predicted"
    df_future = asfr_future.to_dataframe().reset_index()
    df_future[DimensionConstants.YEAR_ID] = (
        df_future[DimensionConstants.COHORT_ID] + df_future[DimensionConstants.AGE]
    )
    df_future = df_future.drop("cohort_id", axis=1)

    # need this object as an anchor for intercept-shift
    df_predicted_last_past_year = df_future.query(
        f"{DimensionConstants.YEAR_ID} == {years.past_end}"
    )

    da_predicted_last_past_year = df_predicted_last_past_year.set_index(
        DimensionConstants.SINGLE_YEAR_ASFR_INDEX_DIMS
    )[
        "predicted"
    ].to_xarray()  # later will be last past year's intercept-shift anchor

    df_future = df_future.query(
        f"{DimensionConstants.YEAR_ID} >= {years.forecast_start} & "
        f"{DimensionConstants.YEAR_ID} <= {years.forecast_end}"
    )

    return df_future, da_predicted_last_past_year


def infer_and_append_terminal_ages(
    gbd_round_id: int, versions: Versions, last_past_year: int, asfr_future: xr.DataArray
) -> xr.DataArray:
    """Append terminal ages to forecasted single-year ASFR.

    Infer terminal ages ASFR based on last past year's ratio.

    The terminal ages are defined by YOUNG_TERMINAL_AGES and OLD_TERMINAL_AGES.

    Args:
        gbd_round_id (int): gbd round id.
        versions (Versions): used to pull past single-year asfr version.
        last_past_year (int): used to filter for last past year.
        asfr_future (xr.DataArray): forecasted single-year asfr, without
            terminal ages.

    Returns:
        (Tuple[xr.DataArray]): terminal-age-inferred future asfr, and the
            observed past asfr.
    """
    LOGGER.info("Inferring terminl ages...")
    path = versions.data_dir(gbd_round_id, "past", "asfr")

    # need 2 past years here for intercept-shift in arima_single_year_asfr()
    asfr_past = (
        open_xr(path / "asfr.nc")
        .sel(
            location_id=asfr_future[DimensionConstants.LOCATION_ID],
            draw=asfr_future[DimensionConstants.DRAW],
            year_id=[last_past_year - 1, last_past_year],
        )
        .rename({DimensionConstants.AGE_GROUP_ID: DimensionConstants.AGE})
    )

    # use mean-level ratios for inference
    ratios = asfr_past.sel(age=YOUNG_TERMINAL_AGES, year_id=last_past_year).mean(
        DimensionConstants.DRAW
    ) / asfr_past.sel(age=COHORT_AGE_START, year_id=last_past_year, drop=True).mean(
        DimensionConstants.DRAW
    )

    young_asfr_future = ratios * asfr_future.sel(age=COHORT_AGE_START, drop=True)

    ratios = asfr_past.sel(age=OLD_TERMINAL_AGES, year_id=last_past_year).mean(
        DimensionConstants.DRAW
    ) / asfr_past.sel(age=COHORT_AGE_END, year_id=last_past_year, drop=True).mean(
        DimensionConstants.DRAW
    )

    old_asfr_future = ratios * asfr_future.sel(age=COHORT_AGE_END, drop=True)

    LOGGER.info("Done inferring terminal ages...")

    asfr_future = xr.concat(
        [young_asfr_future, asfr_future, old_asfr_future], dim=DimensionConstants.AGE
    ).sortby(DimensionConstants.AGE)

    return asfr_future, asfr_past


# NOTE: this should be centralized.
# ordered-draw intercept-shift should only be done in normal space.
def ordered_draw_intercept_shift(
    da_future: xr.DataArray, da_past: xr.DataArray, years: YearRange
) -> xr.DataArray:
    """Ordered-draw intercept-shift based on fan-out trajectories.

    Trajectories are determined by the difference between last forecast year
    and last past year.

    Assumes there's no scenario dimension.  Should only be done in normal space,
    never in any non-linear transformation.

    Args:
        da_future (xr.DataArray): Conctains last past + all future years.
        da_past (xr.DataArray): Contains last past year.  Should probably have
            multiple past years up to the last past year.
        years (YearRange): first past year:first future year:last future year.

    Returns:
        (xr.DataArray): ordered-draw intercept-shifted da_future.
    """
    # we determine draw sort order by trajectory = last year - last past year
    trajectories = da_future.sel(year_id=years.forecast_end) - da_future.sel(
        year_id=years.past_end
    )  # no more year_id dim

    # these are coords after removing year and draw dims
    non_draw_coords = trajectories.drop_vars(DimensionConstants.DRAW).coords
    coords = list(non_draw_coords.indexes.values())
    dims = list(non_draw_coords.indexes.keys())

    for coord in it.product(*coords):
        slice_dict = {dims[i]: coord[i] for i in range(len(coord))}

        trajs = trajectories.sel(**slice_dict)  # should have only draw dim now
        # using argsort once gives the indices that sort the list,
        # using it twice gives the rank of each value, from low to high
        traj_rank = trajs.argsort().argsort().values
        # in case draw labels don't start at 0, we obtain labels
        # traj_rank_labels = trajectories[DimensionConstants.DRAW].\
        #    values[traj_rank.values.tolist()]  # np array of draw labels

        # from now on we will do some calculations in "rank space".
        # ranked_future has year_id/draw dims.
        ranked_future = da_future.sel(**slice_dict).assign_coords(draw=traj_rank)
        # each draw label corresponds to the draw value's rank now
        predicted_last_past_rank = ranked_future.sel(
            year_id=years.past_end
        )  # now only has draws

        # our goal is to allocate the highest trajectory rank to the highest
        # observed last past rank, and lowest to lowest, etc.
        # so we need to bring the last observed year into rank space as well.
        observed_last_past = da_past.sel(**slice_dict, year_id=years.past_end)  # draws only
        past_draw_labels = observed_last_past[DimensionConstants.DRAW].values
        past_rank = observed_last_past.argsort().argsort().values
        observed_last_past_rank = observed_last_past.assign_coords(draw=past_rank)

        # Important: diff inherits the rank labels from observed_last_past_rank
        diff = observed_last_past_rank - predicted_last_past_rank

        if not (
            diff[DimensionConstants.DRAW] == observed_last_past_rank[DimensionConstants.DRAW]
        ).all():
            raise ValueError("diff must inherit rank values from " "observed_last_past_rank")

        # diff added to the future draws in rank space.
        # Important: inherits rank labels from diff (~observed_last_past_rank).
        ranked_future = diff + ranked_future  # has year_id / draw (rank) dims

        if not (ranked_future[DimensionConstants.DRAW] == diff[DimensionConstants.DRAW]).all():
            raise ValueError("ranked_future must inherit rank values from " "diff")

        # ranked_future is now labeled by the rank labels of past draws.
        # it is then straight-forward to map to draw labels:
        ranked_future = ranked_future.assign_coords(draw=past_draw_labels)

        # prep the shape of ranked_future before inserting into da_future
        dim_order = da_future.sel(**slice_dict).dims
        ranked_future = ranked_future.transpose(*dim_order)

        # modify in-place
        da_future.loc[slice_dict] = ranked_future  # ~ "re_aligned_future"

    return da_future


def _age_group_specific_fit_and_predict(
    cohort_5yr_fert: xr.DataArray,
    ccf: xr.DataArray,
    education: xr.DataArray,
    met_need: xr.DataArray,
    last_past_cohort: int,
    locations_df: pd.DataFrame,
) -> xr.DataArray:
    """Fit age-group-specific logit ccf50 fractions using education/met_need.

    Education and met_need used as fixed effects and region_id as random intercept.

    For every 5-year cohort age group, fit
    logit(mean age group fertility / mean ccf50) over mean education and mean
    met_need using LME with region_id random intercept over country locations.
    This is followed by prediction over draws and subnationals.

    Contains a straight-up intercept-shift in the end.

    Args:
        cohort_5yr_fert (xr.DataArray): 5 year fertility in cohort space.
            Has cohort_id, location_id, age, and draw dims.
        ccf (xr.DataArray): ccf50.  Has cohort_id, location_id, and draw dims.
        education (xr.DataArray): same dims as cohort_asfr.
        met_need (xr.DataArray): same dims as cohort_asfr.
        last_past_cohort (int): last completed past cohort.
        locations_df (pd.DataFrame):  locations metadata.  The region ids
            within will filter for the subset locations desired for analysis.

    Returns:
        (xr.DataArray): predicted logit asfr fractions.
    """
    # we will make a mean logit( asfr / ccf) and a draw logit( asfr / ccf)
    # these objects have onyl past values because cohort_asfr only has past
    logit_asfr_fractions = logit(cohort_5yr_fert / ccf)  # for prediction
    logit_asfr_fractions.name = "logit_5yr_fert_over_ccf"

    logit_asfr_fractions_mean = logit(
        cohort_5yr_fert.mean(DimensionConstants.DRAW) / ccf.mean(DimensionConstants.DRAW)
    )  # for fitting
    logit_asfr_fractions_mean.name = logit_asfr_fractions.name

    # Convert to dataframes to work with statsmodels package
    ds_mean = xr.merge(
        [
            logit_asfr_fractions_mean,
            education.mean(DimensionConstants.DRAW),
            met_need.mean(DimensionConstants.DRAW),
        ]
    )

    # the .dropna() call here will remove all future years from df_mean
    df_mean = ds_mean.to_dataframe().dropna().reset_index()  # don't fit on NaN

    # the df for prediction only needs to contain the future cohorts
    ds = xr.merge([logit_asfr_fractions, education, met_need])
    # need the last_past_cohort year here for later intercept-shift
    ds = ds.sel(
        cohort_id=range(last_past_cohort, ccf[DimensionConstants.COHORT_ID].values.max() + 1)
    )
    df = ds.to_dataframe().reset_index()  # has future rows where Y = NaN

    del ds, ds_mean  # no longer needed
    gc.collect()

    # merge with locations_df to assign region_id to locations
    df = df.merge(
        locations_df[[DimensionConstants.LOCATION_ID, DimensionConstants.REGION_ID]],
        how="inner",
        on=DimensionConstants.LOCATION_ID,
    )  # filters for locations

    df_mean = df_mean.merge(
        locations_df[[DimensionConstants.LOCATION_ID, DimensionConstants.REGION_ID, "level"]],
        how="inner",
        on=DimensionConstants.LOCATION_ID,
    )
    # df_mean_nats is for fitting and contains only national means
    df_mean_nats = df_mean.query("level == 3")

    age_df_mean_list = []  # we're fitting the past for later arima
    age_df_list = []  # necessary prep before concats

    for age in logit_asfr_fractions[DimensionConstants.AGE].values:
        # the mean df is used only to fit
        age_df_mean_nats = df_mean_nats.query(f"{DimensionConstants.AGE} == {age}")
        # statsmodel mixedlm api needs "Y ~ X_1 + X_2" to specify fixed effects
        md = smf.mixedlm(
            f"{logit_asfr_fractions.name} ~ " f"{education.name} + {met_need.name}",
            data=age_df_mean_nats,
            groups=age_df_mean_nats[DimensionConstants.REGION_ID],
        )

        mdf = md.fit()

        # the random intercepts are stored in a weird dict that needs pruning
        re_dict = dict([(k, v.values[0]) for k, v in mdf.random_effects.items()])

        # no build-in to predict fixed + random effects.  Predict separately.
        age_df = df.query(f"{DimensionConstants.AGE} == {age}")
        age_df[logit_asfr_fractions.name] = mdf.predict(
            exog=age_df[StageConstants.STAGE_3_COVARIATES]
        ) + age_df[DimensionConstants.REGION_ID].map(
            re_dict
        )  # fe + re

        age_df_list.append(age_df)

        # now fit the past mean because we need this fit for later arima
        age_df_mean = df_mean.query(f"{DimensionConstants.AGE} == {age}")
        age_df_mean[logit_asfr_fractions.name] = mdf.predict(
            exog=age_df_mean[StageConstants.STAGE_3_COVARIATES]
        ) + age_df_mean[DimensionConstants.REGION_ID].map(re_dict)

        age_df_mean_list.append(age_df_mean)

    del df, df_mean
    gc.collect()

    prediction_df = pd.concat(age_df_list, axis=0)
    prediction_da = prediction_df.set_index(list(logit_asfr_fractions.dims)).to_xarray()[
        logit_asfr_fractions.name
    ]

    past_prediction_df = pd.concat(age_df_mean_list, axis=0)
    past_prediction_da = past_prediction_df.set_index(
        list(logit_asfr_fractions_mean.dims)
    ).to_xarray()[logit_asfr_fractions_mean.name]

    # now the intercept-shift for both
    prediction_da = (
        prediction_da
        + logit_asfr_fractions.sel(cohort_id=last_past_cohort, drop=True)
        - prediction_da.sel(cohort_id=last_past_cohort, drop=True)
    )
    # don't need last past cohort in future data no more
    last_future_cohort = int(prediction_da[DimensionConstants.COHORT_ID].max())
    prediction_da = prediction_da.sel(
        cohort_id=range(last_past_cohort + 1, last_future_cohort + 1)
    )

    past_prediction_da = (
        past_prediction_da
        + logit_asfr_fractions_mean.sel(cohort_id=last_past_cohort, drop=True)
        - past_prediction_da.sel(cohort_id=last_past_cohort, drop=True)
    )

    return prediction_da, past_prediction_da


def _convert_incremental_covariates_to_cohort_space(
    da: xr.DataArray, ages_df: pd.DataFrame
) -> xr.DataArray:
    """Convert covariate data from period to cohort space.

    Naive conversion (via reindexing) of covariates for incremental fertilty
    modeling from period space to cohort space by using the start year of the
    age group interval to define the cohort birth year. Differs from the
    function used in ccf modeling because it is also indexed by age.

    Args:
        da (xr.DataArray): period space covariates to reindex.
        ages_df (pd.DataFrame): age metadata.

    Returns:
        (xr.DataArray): covariate data reindexed in cohort space, with
            age_group_id converted to age, the end age of the interval.
    """
    if not np.isin(
        da[DimensionConstants.AGE_GROUP_ID].values,
        ages_df[DimensionConstants.AGE_GROUP_ID].values,
    ).all():
        raise ValueError("Missing age data")

    das = []
    for age_group_id in da[DimensionConstants.AGE_GROUP_ID].values.tolist():
        da_age = da.sel(age_group_id=age_group_id, drop=True)
        ages = ages_df.query(f"age_group_id == {age_group_id}")
        da_age[DimensionConstants.YEAR_ID] = da_age[DimensionConstants.YEAR_ID] - int(
            ages["age_group_years_start"]
        )
        da_age = da_age.rename({DimensionConstants.YEAR_ID: DimensionConstants.COHORT_ID})
        # GBD age_group_years_end is excessive by 1
        da_age[DimensionConstants.AGE] = int(ages["age_group_years_end"]) - 1
        das.append(da_age)

    return xr.concat(das, dim=DimensionConstants.AGE)

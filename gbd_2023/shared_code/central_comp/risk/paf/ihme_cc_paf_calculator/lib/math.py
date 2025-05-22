import functools
import math
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import numpy.typing as npt
import pandas as pd
from scipy import integrate, stats

from ihme_cc_risk_utils import get_ensemble_weights, get_mediation_delta

from ihme_cc_paf_calculator.lib import constants, data_utils
from ihme_cc_paf_calculator.lib import ensemble_utils as eu
from ihme_cc_paf_calculator.lib import input_utils
from ihme_cc_paf_calculator.lib import intervention_utils as iu
from ihme_cc_paf_calculator.lib import io_utils, mediation

LOGNORMAL_PERCENTILE = 0.999999
SUBDIVISIONS = 100  # default of R integrate()
TOLERANCE = np.finfo(float).eps ** 0.25  # default of R integrate()


def _merge_and_format_datasets(rr: pd.DataFrame, exposure: pd.DataFrame) -> pd.DataFrame:
    """Join datasets together and set multiindex to make vectorized math easier."""
    merge_cols = ["age_group_id", "sex_id", "parameter"]

    # not all RRs are year specific
    if "year_id" in rr:
        merge_cols = ["year_id"] + merge_cols

    df = pd.merge(exposure, rr, on=merge_cols, how="inner", suffixes=("_exp", "_rr"))
    if df.empty:
        raise RuntimeError("Mismatch in demographics between exposure and RR")

    df = df.set_index(
        [
            "rei_id",
            "location_id",
            "year_id",
            "age_group_id",
            "sex_id",
            "cause_id",
            "mortality",
            "morbidity",
            "parameter",
            "tmrel",
        ]
    )
    new_cols = pd.MultiIndex.from_tuples(
        [(col.split("_")[-1], int(col.split("_")[1])) for col in df.columns]
    )
    df.columns = new_cols
    df = df.reset_index("tmrel")
    return df


def expand_morbidity_mortality(df: pd.DataFrame) -> pd.DataFrame:
    """Create new separate rows for data with mortality and morbidity = 1"""
    to_expand_mask = (df["mortality"] == 1) & (df["morbidity"] == 1)
    to_expand = df[to_expand_mask]

    expanded = pd.concat([to_expand.assign(mortality=0), to_expand.assign(morbidity=0)])

    return pd.concat([expanded, df[~to_expand_mask]], ignore_index=True)


def _calculate_categorical_paf(df: pd.DataFrame) -> pd.DataFrame:
    """Need to compute PAF = (SUM(exp * rr) - SUM(tmrel * rr)) / SUM(exp * rr)"""
    # we collapse over parameter
    levels = [col for col in df.index.names if col != "parameter"]

    rr = df["rr"]
    exp = df["exp"]
    # need to use numpy array here to broadcast despite differences in column indices
    tmrel = df[["tmrel"]].to_numpy()

    sum_exp_rr = (exp * rr).groupby(level=levels).sum()
    sum_tmrel_rr = (tmrel * rr).groupby(level=levels).sum()

    paf = (sum_exp_rr - sum_tmrel_rr) / sum_exp_rr
    return paf


def _format_categorical_paf_result(df: pd.DataFrame) -> pd.DataFrame:
    """Format PAF draws into standard dataframe format.

    We used multiindices to make the math read easier, but for rest of pipeline
    we expect no multiindices and columns like draw_0.

    Expand rows where mortality and morbidity both = 1.
    """
    df.columns = [f"draw_{i}" for i in df.columns]
    df = df.reset_index()
    df = expand_morbidity_mortality(df)
    return df


def calculate_categorical_paf(rr: pd.DataFrame, exposure: pd.DataFrame) -> pd.DataFrame:
    """Use relative risk and exposure draws to calculate categorical pafs.

    PAF = (SUM(exp * rr) - SUM(tmrel * rr)) / SUM(exp * rr)

    Original implementation:

    Arguments:
        exposure: dataframe of exposure draws
            columns are: location_id, year_id, age_group_id, sex_id, parameter, draw_*
        rr: dataframe of relative risk draws
            columns are: (rei_id, Optional[year_id], age_group_id, sex_id, cause_id,
            mortality, morbidity, parameter, draw_*, tmrel)
    """
    data = _merge_and_format_datasets(rr, exposure)
    result = _calculate_categorical_paf(data)
    formatted = _format_categorical_paf_result(result)
    return formatted


def _make_lognormal_function_and_bounds(
    exposure_mean_draw: float, exposure_sd_draw: float
) -> Tuple[Callable, float, float]:
    """
    Create a lognormal exposure density function, lower, and upper bounds for integration
    based on a draw of exposure mean and exposure standard deviation.
    """
    mu = exposure_mean_draw / math.sqrt(1 + (exposure_sd_draw**2 / exposure_mean_draw**2))
    sigma = math.sqrt(math.log(1 + (exposure_sd_draw**2 / exposure_mean_draw**2)))

    lower = 0
    upper = stats.lognorm.ppf(LOGNORMAL_PERCENTILE, sigma, scale=mu)

    # integration can fail when the exposure_sd is very small, so iteratively increase
    # it until the area under the pdf is near 1
    while stats.lognorm.cdf(upper, sigma, scale=mu) < 0.99:
        sigma *= 1.1
        upper = stats.lognorm.ppf(LOGNORMAL_PERCENTILE, sigma, scale=mu)

    return (functools.partial(stats.lognorm.pdf, s=sigma, scale=mu), lower, upper)


def _make_exposure_dependent_rr_functions(
    exposures: npt.NDArray[np.float64], rr_draws: Dict[str, npt.ArrayLike]
) -> Dict[str, Callable]:
    """Creates functions for interpolating RR as a function of exposure.

    This is the 'base' interpolation function logic. Details relating to
    bounding the results by TMREL or mediation are handled at a higher level.

    Because the relationship between exposures and RR changes by demographic, this function
    should be called once per demographic group/cause/mortality/morbidity

    Assumes/requires exposures is already sorted.

    Arguments:
        exposures: array of input exposures
        rr_draws: dictionary of draw columns to matching array of rr values

    Returns:
        dictionary mapping rr draw column name to function
    """
    interp_funcs = {}
    for rr_column, rr_array in rr_draws.items():
        # need to match ApproxFun's behavior with rule=2 (value at the closest
        # data extreme is returned)
        y_first, y_last = rr_array[0], rr_array[-1]
        interp_funcs[rr_column] = functools.partial(
            np.interp, xp=exposures, fp=rr_array, left=y_first, right=y_last
        )

    return interp_funcs


def _modify_exposure_dependent_rr_function(
    rr_func: Callable,
    is_protective: bool,
    tmrel: float,
    mediation_factor: Optional[float] = None,
) -> Callable:
    """Modifies an RR function to deal with bounding RRs by TMREL and mediation.

    Arguments:
        rr_func: function that estimates RR as a function of exposure
        is_protective: whether exposure is protective
        tmrel: TMREL for the risk factor
        mediation_factor: an optional factor to apply to the function result, representing
            the portion of the risk's effect to be removed and used to calculate an
            unmediated PAF

    Returns:
        modified RR function
    """

    def bound_domain_by_tmrel(
        exposures: npt.NDArray[np.float64], rr_func: Callable
    ) -> Callable:
        """
        Adds TMREL bounds to the RR interpolation function.

        If a harmful exposure value is less than TMREL, the RR interpolation function
        should return the RR at TMREL instead.

        And if exposure is protective, that means if exposure > TMREL then the
        RR interpolation function should return the RR at TMREL instead.
        """
        if is_protective:
            exposures = np.where(exposures > tmrel, tmrel, exposures)
        else:
            exposures = np.where(exposures < tmrel, tmrel, exposures)
        return rr_func(exposures)

    def apply_mediation_factor(
        exposures: npt.NDArray[np.float64], rr_func: Callable
    ) -> Callable:
        """
        Modify the RR function by applying a mediation factor to the result of the function

        The mediation factor represents the portion of this risk's effect that is due to
        the effect of one or more other risk factors. After applying mediation, we are
        left with the residual (the effect of this risk alone) which is used to calculate
        the unmediated PAF.
        """
        return (rr_func(exposures) - 1) * (1 - mediation_factor) + 1

    modified_func = functools.partial(bound_domain_by_tmrel, rr_func=rr_func)
    if mediation_factor is not None:
        modified_func = functools.partial(apply_mediation_factor, rr_func=modified_func)
    return modified_func


def _make_rr_function_log_linear(
    rr_draw: float,
    tmrel_draw: float,
    cap: float,
    is_protective: bool,
    rr_scalar: float,
    mediation_factor: Optional[float] = None,
) -> Callable:
    """
    Returns a function that calculates relative risk as a log-linear function of exposure.

    Arguments:
        rr_draw: relative risk draw that will be raised to some power (depending on exposure)
        tmrel_draw: theoretical minimum exposure level
            (used to help determine severity of exposure)
        cap: maximum exposure possible (used to help determine severity of exposure)
        is_protective: whether the exposure is protective or harmful
        rr_scalar: unit magnitude for relative risk (ie 10 if RR is in units 10 mmHg)
        mediation_factor: an optional factor to apply to the relative risk input value,
            representing the portion of the risk's effect to be removed and used to calculate
            an unmediated PAF
    """
    # Modify the RR value using the mediation factor if it was provided
    total_or_unmediated_rr = (
        (rr_draw - 1) * (1 - mediation_factor) + 1 if mediation_factor else rr_draw
    )

    def rr_power(
        exposure: float,
        exposure_cap: float,
        tmrel: float,
        is_protective: bool,
        rr_scalar: float,
    ) -> float:
        """Raise RR to this exponent.

        We're computing a ratio of the distance the exposure is from tmrel /
        distance the exposure is from maximum possible, except since we're in
        log space we subtract instead of divide.
        """
        tmrel_exposure_difference = exposure - tmrel
        exposure_exposure_cap_difference = exposure - exposure_cap
        if is_protective:
            tmrel_exposure_difference *= -1
            exposure_exposure_cap_difference *= -1
        return (
            ((tmrel_exposure_difference + np.abs(tmrel_exposure_difference)) / 2)
            - (exposure_exposure_cap_difference + np.abs(exposure_exposure_cap_difference))
            / 2
        ) / rr_scalar

    def rr_func(x: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        return total_or_unmediated_rr ** rr_power(
            exposure=x,
            exposure_cap=cap,
            tmrel=tmrel_draw,
            is_protective=is_protective,
            rr_scalar=rr_scalar,
        )

    return rr_func


def calculate_continuous_paf(
    settings: constants.PafCalculatorSettings,
    location_id: int,
    rei_metadata: pd.DataFrame,
    mediator_rei_metadata: pd.DataFrame,
    rr_metadata: pd.DataFrame,
    exposure_min_max: pd.DataFrame,
    input_models: input_utils.ContinuousInputContainer,
    mediation_factors: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    r"""Determine the appropriate calculation method for a risk-mediator-cause based
    based on the types of the input models. Then call the function to calculate the
    PAF draws.

    Recall that continuous PAFs are calculated like this:

      .. math::

         PAF_{\text {joasgt }}=\frac{\int_{x=l}^u RR_{\text {joasg }}(x)
         P_{\text {jasgt }}(x) d x-RR_{\text {joasg }}\left(TMREL_{\text {jas
         }}\right)}{\int_{x=l}^u RR_{\text {joasg }}(x) P_{\text {jasgt }}(x) d
         x}

    We compute an exposure-weighted relative risk, and subtract out the
    relative risk at an ideal exposure level. The PAF is the ratio of
    (exposure-weighted RR - ideal RR) / exposure-weighted RR.

    For continuous risks, exposure inputs can be in the form of a lognormal distribution
    or an ensemble distribution. Relative risk inputs can be either a log-linear function
    of risk increase per unit of exposure increase, or they can be an interpolated
    curve representing risk as a function of exposure.

    Arguments:
        settings: global parameters for this PAF calculator run
        location_id: the location this job is running for
        rei_metadata: information about the risk
        mediator_rei_metadata: information about the mediator risk for 2-stage mediation.
            Not used otherwise. Contains only one mediator.
        rr_metadata: information about the relative risk model, a single-row dataframe
        exposure_min_max: dataframe of min and max exposures by age/rei for the distal
            risk and any mediators with columns
            age_group_id/rei_id/exposure_min/exposure_max
        input_models: a ContinuousInputContainer providing dataframes for exposure,
            exposure_sd, tmrel, and rr. If 2-stage mediation applies, also provides
            a dataframe for exposure, exposure_sd, and tmrel for all mediators. If risk is
            health outcome of an intervention, also provides a dataframe for intervention
            coverage and effect size between the intervention and risk factor.
        mediation_factors: optional dataframe of total mediation factors for this
            risk-cause, used for calculating the unmediated PAF. Contains columns
            cause_id/draw_*

    Returns:
        dataframe of PAF results for the given cause. If cause was a parent cause,
            the PAFs are split to subcauses so results can contain more than
            one cause. Contains columns
            rei_id/cause_id/location_id/year_id/age_group_id/sex_id/morbidity
            /mortality/draw_*
    """
    is_protective = (rei_metadata["inv_exp"] == 1).bool()
    mediator_is_protective = False

    is_ensemble_exposure = (
        rei_metadata["exposure_type"].iat[0] == constants.ExposureType.ENSEMBLE
    )
    is_lognormal_exposure = (
        rei_metadata["exposure_type"].iat[0] == constants.ExposureType.LOGNORMAL
    )
    is_exp_dependent_rr = (
        rr_metadata["relative_risk_type_id"].iat[0]
        == constants.RelativeRiskType.EXPOSURE_DEPENDENT
    )
    is_log_linear_rr = (
        rr_metadata["relative_risk_type_id"].iat[0] == constants.RelativeRiskType.LOG_LINEAR
    )
    is_two_stage = rr_metadata["source"].iat[0] == "delta"
    mediator_rei_id = rr_metadata["med_id"].iat[0]

    if is_two_stage:
        if is_log_linear_rr:
            # This configuration doesn't exist in GBD risk factors and isn't supported
            # yet. If we ever need to support this, work is described in CCYELLOW-844
            raise NotImplementedError(
                "PAF calculation is not supported for two-stage mediation with a "
                "log-linear per-unit relative risk model."
            )

        mediator_is_protective = (
            int(mediator_rei_metadata.query(f"rei_id == {mediator_rei_id}")["inv_exp"]) == 1
        )

        ensemble_weight_df = get_ensemble_weights(
            rei=rei_metadata["rei"].iat[0],
            release_id=settings.release_id,
            location_id=location_id,
        )

        mediator_ensemble_weight_df = get_ensemble_weights(
            rei=mediator_rei_metadata["rei"].iat[0],
            release_id=settings.release_id,
            location_id=location_id,
        )

        # Delta for salt-SBP is a function of hypertension prevalence, so has special handling
        if (
            settings.rei_id == constants.SALT_REI_ID
            and mediator_rei_id == constants.METAB_SBP_REI_ID
        ):
            delta_df = calculate_delta_for_salt_sbp(
                exposure=input_models.mediator_exposure,
                exposure_sd=input_models.mediator_exposure_sd,
                ensemble_weight_df=mediator_ensemble_weight_df,
                release_id=settings.release_id,
                n_draws=settings.n_draws,
            )
        else:
            delta_df = get_mediation_delta(
                rei_id=settings.rei_id,
                release_id=settings.release_id,
                n_draws=settings.n_draws,
            ).query(f"med_id == {mediator_rei_id}")
            delta_df = mediation.expand_delta(delta_df, input_models.exposure)

        paf_df = calculate_ensemble_two_stage_paf(
            input_models=input_models,
            delta_df=delta_df,
            rei_id=settings.rei_id,
            mediator_rei_id=mediator_rei_id,
            ensemble_weight_df=ensemble_weight_df,
            mediator_ensemble_weight_df=mediator_ensemble_weight_df,
            min_max_df=exposure_min_max,
            is_protective=is_protective,
            mediator_is_protective=mediator_is_protective,
        )
    elif is_ensemble_exposure and is_exp_dependent_rr:
        ensemble_weight_df = get_ensemble_weights(
            rei=rei_metadata["rei"].iat[0],
            release_id=settings.release_id,
            location_id=location_id,
        )
        paf_df = calculate_ensemble_exposure_dependent_paf(
            input_models=input_models,
            ensemble_weight_df=ensemble_weight_df,
            min_max_df=exposure_min_max.query(f"rei_id == {settings.rei_id}"),
            is_protective=is_protective,
            mediation_factors=mediation_factors,
        )
    elif is_ensemble_exposure and is_log_linear_rr:
        ensemble_weight_df = get_ensemble_weights(
            rei=rei_metadata["rei"].iat[0],
            release_id=settings.release_id,
            location_id=location_id,
        )
        paf_df = calculate_ensemble_loglinear_paf(
            input_models=input_models,
            ensemble_weight_df=ensemble_weight_df,
            min_max_df=exposure_min_max.query(f"rei_id == {settings.rei_id}"),
            is_protective=is_protective,
            rr_scalar=rei_metadata["rr_scalar"].iat[0],
            mediation_factors=mediation_factors,
        )
    elif is_lognormal_exposure and is_log_linear_rr:
        if mediation_factors:
            raise NotImplementedError(
                "Mediation factors found but mediation is not supported for PAFs with "
                "lognormal exposure and log-linear per-unit relative risk."
            )
        if input_models.intervention_rei_id:
            raise NotImplementedError(
                "Intervention found, but intervention-risk PAFs are not supported for risk "
                "factors with lognormal exposure and log-linear per-unit relative risk."
            )
        paf_df = calculate_lognormal_loglinear_paf(
            input_models=input_models,
            min_max_df=exposure_min_max.query(f"rei_id == {settings.rei_id}"),
            is_protective=is_protective,
            rr_scalar=rei_metadata["rr_scalar"].iat[0],
        )
    else:
        raise NotImplementedError(
            "There is no PAF calculation method defined for "
            f"{rei_metadata['exposure_type']} distribution and relative risk type "
            f"{rr_metadata['relative_risk_type_id']}"
        )

    # Some final formatting
    cause_id = input_models.rr["cause_id"].iat[0]
    rei_id = settings.intervention_rei_id or settings.rei_id
    paf_df = paf_df.assign(cause_id=cause_id, rei_id=rei_id)
    _, draw_cols = data_utils.get_index_draw_columns(paf_df)
    paf_df[draw_cols] = paf_df[draw_cols].astype("float64")
    paf_df = expand_morbidity_mortality(paf_df)

    input_models.reset_all_indices()
    return paf_df


def calculate_lognormal_loglinear_paf(
    input_models: input_utils.ContinuousInputContainer,
    min_max_df: pd.DataFrame,
    is_protective: bool,
    rr_scalar: float,
) -> pd.DataFrame:
    """
    Calculate a PAF for one cause using a lognormal exposure distribution and
    log-linear RR function.

    For this method, our exposure probability distribution is a log-normal
    distribution with the modeled exposure mean and standard deviation.
    Relative risks are a log-linear relationship between risk and per-unit
    increase in exposure.

    Arguments:
        input_models: a ContinuousInputContainer providing dataframes for exposure,
            exposure_sd, tmrel, and rr
        min_max_df: dataframe of min and max exposures by age/rei. contains data
            for only this risk and no mediators. Contains columns
            age_group_id/rei_id/exposure_min/exposure_max
        is_protective: whether the exposure to the risk is protective
        rr_scalar: unit magnitude for relative risk (ie 10 if RR is in units 10 mmHg)

    Returns:
        dataframe of PAF results with columns
            location_id/year_id/age_group_id/sex_id/morbidity/mortality/draw_*
    """
    draw_cols = [c for c in input_models.exposure if "draw" in c]

    # Find the appropriate limit of the exposure distribution. Protective risks use the
    # minimum exposure; harmful risks use the maximum. We need values for each age group
    # as some risks use age-specific exposure percentiles.
    cap_column = "exposure_min" if is_protective else "exposure_max"
    cap_per_age = min_max_df.set_index(constants.AGE_GROUP_ID)[cap_column].to_dict()

    # Set up indexing for the input models
    input_models.index_by_demographic()

    # Set the index for the output PAFs. We do this join to handle the case where rr
    # is missing year_id. Otherwise output_index == rr.index
    output_index = input_models.exposure.index.join(input_models.rr.index, how="outer")
    output_index = output_index.reorder_levels(
        constants.DEMOGRAPHIC_COLS + constants.MORB_MORT_COLS
    )
    pafs = pd.DataFrame(index=output_index, columns=draw_cols)
    rr_year_specific = constants.YEAR_ID in input_models.rr.index.names

    # for each demographic (and morbidity/mortality grouping), compute the
    # exposure-weighted relative risk and RR at tmrel. Compute the PAF as
    # (exposure-weighted RR - ideal RR) / exposure-weighted RR
    for output_row in output_index:
        rr_indexer, other_indexer = _compute_row_index(output_row, rr_year_specific)
        these_exposures = input_models.exposure.loc[other_indexer, draw_cols]
        these_exposure_sds = input_models.exposure_sd.loc[other_indexer, draw_cols]
        these_tmrels = input_models.tmrel.loc[other_indexer, draw_cols]
        these_rrs = input_models.rr.loc[rr_indexer, draw_cols]

        row_demographics = dict(zip(constants.DEMOGRAPHIC_COLS, output_row))
        exposure_cap = cap_per_age[row_demographics[constants.AGE_GROUP_ID]]

        for draw_col in draw_cols:
            # create lognormal exposure function
            exposure_function, lower, upper = _make_lognormal_function_and_bounds(
                exposure_mean_draw=these_exposures[draw_col],
                exposure_sd_draw=these_exposure_sds[draw_col],
            )
            # create log linear rr function
            rr_function = _make_rr_function_log_linear(
                rr_draw=these_rrs[draw_col],
                tmrel_draw=these_tmrels[draw_col],
                cap=exposure_cap,
                is_protective=is_protective,
                rr_scalar=rr_scalar,
            )
            # calculate the PAFs
            rr_at_tmrel = rr_function(these_tmrels[draw_col])
            exposure_weighted_rr = integrate.quad(
                lambda x, exp_func=exposure_function, rr_func=rr_function: exp_func(x)
                * rr_func(x),
                a=lower,
                b=upper,
            )[0]
            paf = (exposure_weighted_rr - rr_at_tmrel) / exposure_weighted_rr

            pafs.loc[output_row, draw_col] = paf

    return pafs.reset_index()


def calculate_ensemble_loglinear_paf(
    input_models: input_utils.ContinuousInputContainer,
    ensemble_weight_df: pd.DataFrame,
    min_max_df: pd.DataFrame,
    is_protective: bool,
    rr_scalar: float,
    mediation_factors: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Calculate a PAF for one cause using an ensemble exposure distribution and
    log-linear RR function.

    For this method, our exposure probability distribution is a mixture
    distribution defined by weights from various analytical distributions. And
    our relative risks are a log-linear relationship between risk and per-unit
    increase in exposure.

    Arguments:
        input_models: a ContinuousInputContainer providing dataframes for exposure,
            exposure_sd, tmrel, and rr
        ensemble_weight_df: dataframe of ensemble distribution weights with
            columns location_id/year_id/age_group_id/sex_id/(various weight
            distribution names like norm,glnorm etc)
        min_max_df: dataframe of min and max exposures by age/rei. contains data
            for only this risk and no mediators. Contains columns
            age_group_id/rei_id/exposure_min/exposure_max
        is_protective: whether the exposure to the risk is protective
        rr_scalar: unit magnitude for relative risk (ie 10 if RR is in units 10 mmHg)
        mediation_factors: optional dataframe of total mediation factors for this
            risk-cause, used for calculating the unmediated PAF. Contains columns
            cause_id/draw_*

    Returns:
        dataframe of PAF results with columns
            location_id/year_id/age_group_id/sex_id/morbidity/mortality/draw_*

    Note that the mortality/morbidity indicator column convention in the RR
    dataset have been expanded so that a row with mortality=1 and morbidity=1
    would become 2 rows. This means the number of rows of PAFs might increase
    compared to the rr_df dataset.
    """
    draw_cols = [c for c in input_models.exposure if "draw" in c]

    # ensemble weight demographics (loc/years) often don't align with our other
    # datasets. Ensemble weights should not vary by loc/year, so it should be safe
    # to force alignment
    ensemble_weight_df = eu.expand_ensemble_weights(ensemble_weight_df, input_models.exposure)
    min_max_df = _expand_min_max_df(min_max_df=min_max_df, exposure=input_models.exposure)

    # Set up indexing for the input models
    input_models.index_by_demographic()
    min_max_df = min_max_df.set_index(constants.DEMOGRAPHIC_COLS)
    ensemble_weight_df = ensemble_weight_df.set_index(constants.DEMOGRAPHIC_COLS)

    # Set the index for the output PAFs. We do this join to handle the case where rr
    # is missing year_id. Otherwise output_index == rr.index
    exposure_index = input_models.exposure.index
    output_index = exposure_index.join(input_models.rr.index, how="outer")
    output_index = output_index.reorder_levels(
        constants.DEMOGRAPHIC_COLS + constants.MORB_MORT_COLS
    )
    pafs = pd.DataFrame(index=output_index, columns=draw_cols)
    rr_year_specific = constants.YEAR_ID in input_models.rr.index.names

    # for each demographic (and morbidity/mortality grouping), compute the
    # exposure-weighted relative risk and RR at tmrel. Compute the PAF as
    # (exposure-weighted RR - ideal RR) / exposure-weighted RR
    for output_row in output_index:
        rr_indexer, other_indexer = _compute_row_index(output_row, rr_year_specific)
        these_tmrels = input_models.tmrel.loc[other_indexer, draw_cols]
        these_rrs = input_models.rr.loc[rr_indexer, draw_cols]

        # Create exposure density vectors; these represent our exposure mixture distribution.
        these_edensity_vectors = eu.get_edensity(
            weights=ensemble_weight_df.loc[other_indexer],
            mean=input_models.exposure.loc[other_indexer, draw_cols],
            sd=input_models.exposure_sd.loc[other_indexer, draw_cols],
            bounds=min_max_df.loc[other_indexer],
        )

        # Modify risk exposure distribution for a given intervention counterfactual.
        if input_models.intervention_rei_id:
            these_edensity_vectors = iu.modify_exposure_edensity_for_intervention(
                exposure_density_vectors=these_edensity_vectors,
                intervention_coverage=input_models.intervention_coverage.loc[other_indexer],
                intervention_effect_size=input_models.intervention_effect_size,
            )

        for draw_col in draw_cols:
            mediation_factor_draw = (
                mediation_factors[draw_col].iat[0] if mediation_factors is not None else None
            )
            exposure_cap = (
                these_edensity_vectors[draw_col]["XMIN"]
                if is_protective
                else these_edensity_vectors[draw_col]["XMAX"]
            )
            # create log-linear rr function
            rr_function = _make_rr_function_log_linear(
                rr_draw=these_rrs[draw_col],
                tmrel_draw=these_tmrels[draw_col],
                cap=exposure_cap,
                is_protective=is_protective,
                rr_scalar=rr_scalar,
                mediation_factor=mediation_factor_draw,
            )
            pafs.loc[output_row, draw_col] = evaluate_paf_formula(
                rr_at_tmrel=rr_function(these_tmrels[draw_col]),
                exposure_domain=these_edensity_vectors[draw_col]["x"],
                exposure_probability=these_edensity_vectors[draw_col]["fx"],
                rr_over_exposure_domain=rr_function(these_edensity_vectors[draw_col]["x"]),
            )

    return pafs.reset_index()


def calculate_ensemble_exposure_dependent_paf(
    input_models: input_utils.ContinuousInputContainer,
    ensemble_weight_df: pd.DataFrame,
    min_max_df: pd.DataFrame,
    is_protective: bool,
    mediation_factors: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Calculate a PAF for one cause using an ensemble exposure distribution and
    exposure-dependent RR function.

    For this method, our exposure probability distribution is a mixture
    distribution defined by weights from various analytical distributions. And
    our relative risks are a linear function of our exposures.

    Arguments:
        input_models: a ContinuousInputContainer providing dataframes for exposure,
            exposure_sd, tmrel, and rr
        ensemble_weight_df: dataframe of ensemble distribution weights with
            columns location_id/year_id/age_group_id/sex_id/(various weight
            distribution names like norm,glnorm etc)
        min_max_df: dataframe of min and max exposures by age/rei. contains data
            for only this risk and no mediators. Contains columns
            age_group_id/rei_id/exposure_min/exposure_max
        is_protective: whether the exposure to the risk is protective
        mediation_factors: optional dataframe of total mediation factors for this
            risk-cause, used for calculating the unmediated PAF. Contains columns
            cause_id/draw_*

    Returns:
        dataframe of PAF results with columns
            location_id/year_id/age_group_id/sex_id/morbidity/mortality/draw_*

    Note that the mortality/morbidity indicator column convention in the RR
    dataset have been expanded so that a row with mortality=1 and morbidity=1
    would become 2 rows. This means the number of rows of PAFs might increase
    compared to the rr_df dataset.
    """
    draw_cols = [c for c in input_models.exposure if "draw" in c]

    # ensemble weight demographics (loc/years) often don't align with our other
    # datasets. Ensemble weights should not vary by loc/year, so it should be safe
    # to force alignment
    ensemble_weight_df = eu.expand_ensemble_weights(ensemble_weight_df, input_models.exposure)
    min_max_df = _expand_min_max_df(min_max_df=min_max_df, exposure=input_models.exposure)

    # Set up indexing for the input models
    input_models.index_by_demographic()
    min_max_df = min_max_df.set_index(constants.DEMOGRAPHIC_COLS)
    ensemble_weight_df = ensemble_weight_df.set_index(constants.DEMOGRAPHIC_COLS)

    # create RR functions for calculating RRs as a function of exposure. The
    # relationship between RR and exposure can vary by demographic, so we must
    # do this once demographic/mortality/morbidity grouping
    rr_id_cols = input_models.rr.index.names
    input_models.rr = input_models.rr.sort_values(rr_id_cols + ["exposure"])

    def _make_funcs_per_demographic(subdf: pd.DataFrame) -> Dict[str, Callable]:
        exposures = subdf["exposure"].values
        rr_draws = {rr_col: subdf[rr_col].values for rr_col in draw_cols}
        return _make_exposure_dependent_rr_functions(exposures, rr_draws)

    rr_functions = input_models.rr.groupby(rr_id_cols).apply(_make_funcs_per_demographic)

    # Set the index for the output PAFs. We do this join to handle the case where
    # rr_functions is missing year_id. Otherwise output_index == rr_functions.index
    output_index = rr_functions.index.join(input_models.exposure.index, how="outer")
    output_index = output_index.reorder_levels(
        constants.DEMOGRAPHIC_COLS + constants.MORB_MORT_COLS
    )
    pafs = pd.DataFrame(index=output_index, columns=draw_cols)
    rr_year_specific = constants.YEAR_ID in input_models.rr.index.names

    # for each demographic (and morbidity/mortality grouping), compute the
    # exposure-weighted relative risk and RR at tmrel. Compute the PAF as
    # (exposure-weighted RR - ideal RR) / exposure-weighted RR
    for output_row in output_index:
        rr_indexer, other_indexer = _compute_row_index(output_row, rr_year_specific)
        these_rr_functions = rr_functions.loc[rr_indexer]
        these_tmrels = input_models.tmrel.loc[other_indexer, draw_cols]

        # Create exposure density vectors; these represent our exposure mixture distribution.
        these_edensity_vectors = eu.get_edensity(
            weights=ensemble_weight_df.loc[other_indexer],
            mean=input_models.exposure.loc[other_indexer, draw_cols],
            sd=input_models.exposure_sd.loc[other_indexer, draw_cols],
            bounds=min_max_df.loc[other_indexer],
        )

        # Modify risk exposure distribution for a given intervention counterfactual.
        if input_models.intervention_rei_id:
            these_edensity_vectors = iu.modify_exposure_edensity_for_intervention(
                exposure_density_vectors=these_edensity_vectors,
                intervention_coverage=input_models.intervention_coverage.loc[other_indexer],
                intervention_effect_size=input_models.intervention_effect_size,
            )

        for draw_col in draw_cols:
            mediation_factor_draw = (
                mediation_factors[draw_col].iat[0] if mediation_factors is not None else None
            )
            rr_function = _modify_exposure_dependent_rr_function(
                rr_func=these_rr_functions[draw_col],
                is_protective=is_protective,
                tmrel=these_tmrels[draw_col],
                mediation_factor=mediation_factor_draw,
            )
            pafs.loc[output_row, draw_col] = evaluate_paf_formula(
                rr_at_tmrel=rr_function(these_tmrels[draw_col]),
                exposure_domain=these_edensity_vectors[draw_col]["x"],
                exposure_probability=these_edensity_vectors[draw_col]["fx"],
                rr_over_exposure_domain=rr_function(these_edensity_vectors[draw_col]["x"]),
            )

    return pafs.reset_index()


def calculate_ensemble_two_stage_paf(
    input_models: input_utils.ContinuousInputContainer,
    delta_df: pd.DataFrame,
    rei_id: int,
    mediator_rei_id: int,
    ensemble_weight_df: pd.DataFrame,
    mediator_ensemble_weight_df: pd.DataFrame,
    min_max_df: pd.DataFrame,
    is_protective: bool,
    mediator_is_protective: bool,
) -> pd.DataFrame:
    """
    Calculate a PAF for one cause using an ensemble exposure distribution and
    exposure-dependent RR function via two-stage mediation.

    For this method, our exposure probability distribution is a mixture
    distribution defined by weights from various analytical distributions. And
    our relative risks are a linear function of our exposures. The RR here
    belongs to a mediator risk but is shifted into the exposure space of the
    distal risk to be used in PAF calculation.

    Arguments:
        input_models: a ContinuousInputContainer providing dataframes for exposure,
            exposure_sd, and tmrel for the distal risk, as well as exposure,
            exposure_sd, tmrel, and rr for the mediator risk
        delta_df: dataframe of delta draws for this risk and mediator. Will
            contain same number of rows as exposure and columns
            rei_id/med_id/delta_unit/mean_delta/draw_*
        rei_id: the risk for this PAF calculator run
        mediator_rei_id: the mediator risk providing a risk curve via 2-stage
            mediation
        ensemble_weight_df: dataframe of ensemble distribution weights with
            columns location_id/year_id/age_group_id/sex_id/(various weight
            distribution names like norm,glnorm etc)
        mediator_ensemble_weight_df: dataframe of ensemble distribution weights
            for the mediator risk
        min_max_df: dataframe of min and max exposures by age/rei. Includes both
            the distal and mediator risks. Contains columns
            age_group_id/rei_id/exposure_min/exposure_max
        is_protective: whether the exposure to the distal risk is protective
        mediator_is_protective: whether the exposure to the mediator risk is
            protective

    Returns:
        dataframe of PAF results with columns
            location_id/year_id/age_group_id/sex_id/morbidity/mortality/draw_*

    Note that the mortality/morbidity indicator column convention in the RR
    dataset have been expanded so that a row with mortality=1 and morbidity=1
    would become 2 rows. This means the number of rows of PAFs might increase
    compared to the rr_df dataset.
    """
    draw_cols = [c for c in input_models.exposure if "draw" in c]

    # ensemble weight demographics (loc/years) often don't align with our other
    # datasets. Ensemble weights should not vary by loc/year, so it should be safe
    # to force alignment
    ensemble_weight_df = eu.expand_ensemble_weights(ensemble_weight_df, input_models.exposure)
    mediator_ensemble_weight_df = eu.expand_ensemble_weights(
        mediator_ensemble_weight_df, input_models.mediator_exposure
    )
    min_max_df = _expand_min_max_df(min_max_df=min_max_df, exposure=input_models.exposure)

    # Set up indexing for the input models
    input_models.index_by_demographic()
    delta_df = delta_df.set_index(constants.DEMOGRAPHIC_COLS)
    min_max_df = min_max_df.set_index(constants.DEMOGRAPHIC_COLS)
    ensemble_weight_df = ensemble_weight_df.set_index(constants.DEMOGRAPHIC_COLS)
    mediator_ensemble_weight_df = mediator_ensemble_weight_df.set_index(
        constants.DEMOGRAPHIC_COLS
    )

    # create RR functions for calculating RRs as a function of exposure. The
    # relationship between RR and exposure can vary by demographic, so we must
    # do this once demographic/mortality/morbidity grouping
    rr_id_cols = input_models.rr.index.names
    input_models.rr = input_models.rr.sort_values(rr_id_cols + ["exposure"])

    def _make_funcs_per_demographic(subdf: pd.DataFrame) -> Dict[str, Callable]:
        exposures = subdf["exposure"].values
        rr_draws = {rr_col: subdf[rr_col].values for rr_col in draw_cols}
        return _make_exposure_dependent_rr_functions(exposures, rr_draws)

    rr_functions = input_models.rr.groupby(rr_id_cols).apply(_make_funcs_per_demographic)

    # We modify the 2-stage calculation for absolute risk curves, which can have regions
    # of zero risk
    cause_id = input_models.rr["cause_id"].iat[0]
    uses_absolute_risk_curve = (
        mediator_rei_id,
        cause_id,
    ) in constants.RISK_CAUSES_WITH_ABS_RISK_CURVES

    # Set the index for the output PAFs. We do this join to handle the case where
    # rr_functions is missing year_id. Otherwise output_index == rr_functions.index
    output_index = rr_functions.index.join(input_models.exposure.index, how="outer")
    output_index = output_index.reorder_levels(
        constants.DEMOGRAPHIC_COLS + constants.MORB_MORT_COLS
    )
    pafs = pd.DataFrame(index=output_index, columns=draw_cols)
    rr_year_specific = constants.YEAR_ID in input_models.rr.index.names

    # for each demographic (and morbidity/mortality grouping), compute the
    # exposure-weighted relative risk and RR at tmrel. Compute the PAF as
    # (exposure-weighted shifted RR - ideal shifted RR) / exposure-weighted shifted RR
    # assuming that RR at the TMREL is 1
    for output_row in output_index:
        rr_indexer, other_indexer = _compute_row_index(output_row, rr_year_specific)
        these_rr_functions = rr_functions.loc[rr_indexer]
        these_deltas = delta_df.loc[other_indexer, draw_cols]
        these_tmrels = input_models.tmrel.loc[other_indexer, draw_cols]
        these_mediator_tmrels = input_models.mediator_tmrel.loc[other_indexer, draw_cols]

        # Create exposure density vectors; these represent our exposure mixture distribution.
        these_edensity_vectors = eu.get_edensity(
            weights=ensemble_weight_df.loc[other_indexer],
            mean=input_models.exposure.loc[other_indexer, draw_cols],
            sd=input_models.exposure_sd.loc[other_indexer, draw_cols],
            bounds=min_max_df.query("rei_id == @rei_id").loc[other_indexer],
        )
        these_mediator_density_vectors = eu.get_edensity(
            weights=mediator_ensemble_weight_df.loc[other_indexer],
            mean=input_models.mediator_exposure.loc[other_indexer, draw_cols],
            sd=input_models.mediator_exposure_sd.loc[other_indexer, draw_cols],
            bounds=min_max_df.query("rei_id == @mediator_rei_id").loc[other_indexer],
        )

        # Modify risk exposure distribution for a given intervention counterfactual.
        if input_models.intervention_rei_id:
            these_edensity_vectors = iu.modify_exposure_edensity_for_intervention(
                exposure_density_vectors=these_edensity_vectors,
                intervention_coverage=input_models.intervention_coverage.loc[other_indexer],
                intervention_effect_size=input_models.intervention_effect_size,
            )

        for draw_col in draw_cols:
            rr_pdf = _modify_exposure_dependent_rr_function(
                rr_func=these_rr_functions[draw_col],
                is_protective=mediator_is_protective,
                tmrel=these_mediator_tmrels[draw_col],
            )
            exposure_domain = these_edensity_vectors[draw_col]["x"]
            exposure_probability = these_edensity_vectors[draw_col]["fx"]
            # translate mediator RR curve for the distal risk
            rr_over_exposure_domain = shift_mediator_rr_value(
                distal_exposure_values=exposure_domain,
                rr_function=rr_pdf,
                delta=these_deltas[draw_col],
                mediator_density_vectors=these_mediator_density_vectors[draw_col],
                distal_tmrel=these_tmrels[draw_col],
                distal_is_protective=is_protective,
                uses_absolute_risk_curve=uses_absolute_risk_curve,
            )
            # Calculate the PAF draw. When shifting a mediator RR for the distal risk,
            # we know we know that rr(TMREL) will be 1 so no need to calculate it here
            pafs.loc[output_row, draw_col] = evaluate_paf_formula(
                rr_at_tmrel=1,
                exposure_domain=exposure_domain,
                exposure_probability=exposure_probability,
                rr_over_exposure_domain=rr_over_exposure_domain,
            )

    return pafs.reset_index()


def _compute_row_index(
    index: Tuple[int, ...], rr_year_specific: bool
) -> Tuple[Tuple[int, ...], Tuple[int, int, int, int]]:
    """
    Indexing into our datasets depends on whether the RRs are year specific, so we handle
    that here.

    We assume/require the order of columns in index is loc/year/age/sex/morbidity/mortality
    """
    (loc, year, age, sex, morbidity, mortality) = index
    if rr_year_specific:
        rr_indexer = index
    else:
        rr_indexer = (loc, age, sex, morbidity, mortality)
    # every dataset except RR and outputs are just demographic cols (loc/year/age/sex)
    other_indexer = (loc, year, age, sex)
    return rr_indexer, other_indexer


def shift_mediator_rr_value(
    distal_exposure_values: npt.NDArray[np.float64],
    rr_function: Callable,
    delta: float,
    mediator_density_vectors: Dict[str, Any],
    distal_tmrel: float,
    distal_is_protective: bool,
    uses_absolute_risk_curve: bool,
) -> npt.NDArray[np.float64]:
    r"""Two-stage mediation: given an array of values of exposure representing the distal risk
    exposure domain, apply the delta to the mediator RR to get an RR value for the distal
    risk at those exposure levels. The mediated RR calculation can be described as:

      .. math::

        RR_{distal-cause}(a)=\int\frac{RR_{mediator-cause}(b+\delta(a - TMREL_{distal})+)}
        {RR_{mediator-cause}(b)}p(b)db

    where 'a' is the distal risk exposure, 'b' is mediator exposure, and 𝛿 is the
    delta between distal and mediator.

    In some cases, the risk-cause uses an absolute risk curve instead of a relative risk
    curve, which means some RR values can be zero. (Representing zero prevalence below
    a definitional threshold for example). In this case we integrate the numerator and
    denominator separately. When the denominator is zero, there is no excess risk so we
    set the shifted RR value to 1. This slightly different formula is correct for absolute
    risk curves: see CCBLUE-506

    Arguments:
        distal exposure values: an array of exposures to the distal risk
        rr_function: a function that returns the relative risk at the given exposure value
        delta: a draw of delta, the value used to translate between the exposure space of
            a mediator and a distal risk. The mediator RR curve is shifted to the distal
            risk using this value
        mediator_density_vectors: a dictionary representing the exposure density function
            for the mediator risk. Contains "x", the exposure domain, and "fx", the
            density vector
        distal_tmrel: a draw of TMREL for the distal risk
        distal_is_protective: whether exposure to the distal risk is protective
            against the cause
        uses_absolute_risk_curve: whether the risk curve represents absolute rather than
            relative risk. Absolute risk curves can contain zeroes

    Returns an array of RRs at the given distal exposures, shifted from the mediator RR curve.
    """
    mediator_exposure_domain = mediator_density_vectors["x"]
    mediator_exposure_probability = mediator_density_vectors["fx"]
    delta_shift = delta * (
        np.minimum(distal_exposure_values - distal_tmrel, 0)
        if distal_is_protective
        else np.maximum(distal_exposure_values - distal_tmrel, 0)
    )
    if uses_absolute_risk_curve:
        shifted_domains = mediator_exposure_domain[None, :] + delta_shift[:, None]
        numerators = np.trapz(
            y=rr_function(shifted_domains) * mediator_exposure_probability,
            x=mediator_exposure_domain,
        )
        denominator = np.trapz(
            y=rr_function(mediator_exposure_domain) * mediator_exposure_probability,
            x=mediator_exposure_domain,
        )
        # force RR to 1 when there is no excess risk. This assumes a harmful mediator risk
        if denominator == 0:
            shifted_rrs = np.ones_like(numerators)
        else:
            quotients = numerators / denominator
            rr_under_one = quotients < 1
            no_shift = delta_shift == 1
            shifted_rrs = np.where(rr_under_one | no_shift, 1, quotients)
    else:
        constant_factor = mediator_exposure_probability / rr_function(
            mediator_exposure_domain
        )
        shifted_domains = mediator_exposure_domain[None, :] + delta_shift[:, None]
        shifted_rrs = np.trapz(
            y=rr_function(shifted_domains) * constant_factor[None, :],
            x=mediator_exposure_domain,
            axis=1,
        )
    return shifted_rrs


def calculate_delta_for_salt_sbp(
    exposure: pd.DataFrame,
    exposure_sd: pd.DataFrame,
    ensemble_weight_df: pd.DataFrame,
    release_id: int,
    n_draws: int,
) -> pd.DataFrame:
    """Calculate mediation delta for salt-SBP.

    Mediation delta for salt and high systolic blood pressure (SBP) is a function
    of the prevalence of hypertension, defined as blood pressure above 140 mmHG.

    First, the prevalence of hypertension is computed using the SBP exposure and exposure SD
    models for each draw. SBP exposure represents the level of blood pressure in mmHG
    observed in the given demographic.

    Using the SBP ensemble weights, we generate an ensemble density
    function, similar to other PAF models with ensemble distributions. Prevalence is
    calculated as the integral (area under the curve) above 140. The value is normalized by
    dividing by the total integral, which should be close to 1 anyway.

    Second, we use hypertension prevalence to calculate the salt-SBP delta. Mediation deltas
    have an additional dimension, blood pressure less than 140 (non-hypertensive) or
    over 140 (hypertensive). The two categories are summed together, weighting by prevalence
    in order to calculate the overall delta.

    Returns:
        deltas dataframe with columns: rei_id, med_id, age_group_id, sex_id, location_id,
            year_id, draw_0, ..., draw_n; number of rows match exposure
    """
    # Pull delta and copy dimensions in exposure
    delta = io_utils.get_mediation_delta_for_salt_sbp(release_id, n_draws).merge(
        exposure[constants.DEMOGRAPHIC_COLS], on="age_group_id"
    )
    delta_less140 = delta.query("sbp_shift == 'less140'")
    delta_more140 = delta.query("sbp_shift == 'more140'")

    # Calculate hypertension prevalence
    ensemble_weight_df = eu.expand_ensemble_weights(ensemble_weight_df, exposure)

    exposure = exposure.set_index(constants.DEMOGRAPHIC_COLS)
    exposure_sd = exposure_sd.set_index(constants.DEMOGRAPHIC_COLS)
    ensemble_weight_df = ensemble_weight_df.set_index(constants.DEMOGRAPHIC_COLS)

    def pdf(x: int, density_vectors: Dict[str, Any]) -> int:
        """Probability density function defined by density_vectors.

        Given an x, returns a y (fx) using linear interpolation via np.interp.
        """
        return np.interp(x=x, xp=density_vectors["x"], fp=density_vectors["fx"])

    # Calculate hypertension prevalence = integral above 140 / total integral
    # Run this once for each exposure row and draw
    draw_cols = [f"draw_{i}" for i in range(n_draws)]
    prevalence = pd.DataFrame(index=exposure.index, columns=draw_cols)
    for demographic in prevalence.index:
        these_exposure_density_vectors = eu.get_edensity(
            weights=ensemble_weight_df.loc[demographic],
            mean=exposure.loc[demographic, draw_cols],
            sd=exposure_sd.loc[demographic, draw_cols],
        )
        for draw_col in draw_cols:
            vectors_for_draw = these_exposure_density_vectors[draw_col]
            # Total integral is total area under the curve. Should be close to 1
            total_integral = integrate.quad(
                func=pdf,
                args=vectors_for_draw,
                a=vectors_for_draw["XMIN"],
                b=vectors_for_draw["XMAX"],
                limit=100,
            )[0]
            # Area under curve above 140 BP (or XMAX if XMAX < 140 to avoid negative integrals)
            integral_above140 = integrate.quad(
                func=pdf,
                args=vectors_for_draw,
                a=min(140, vectors_for_draw["XMAX"]),
                b=vectors_for_draw["XMAX"],
                limit=100,
            )[0]
            this_prevalence = integral_above140 / total_integral
            prevalence.loc[demographic, draw_col] = this_prevalence

    delta_less140 = delta_less140.set_index(constants.DEMOGRAPHIC_COLS)[draw_cols]
    delta_more140 = delta_more140.set_index(constants.DEMOGRAPHIC_COLS)[draw_cols]

    calculated_delta = (
        (prevalence * delta_more140 + (1 - prevalence) * delta_less140)
        .reset_index()
        .apply(pd.to_numeric)
        .assign(rei_id=constants.SALT_REI_ID, med_id=constants.METAB_SBP_REI_ID)
    )

    # Invariant check: input exposure df length should match calculated delta length
    if len(exposure) != len(calculated_delta):
        raise RuntimeError(
            f"Internal error: exposure df length ({len(exposure)}) does not match "
            f"calculated delta length ({len(calculated_delta)})."
        )

    return calculated_delta


def aggregate_pafs(paf_df: pd.DataFrame, collapse_cols: List[str]) -> pd.DataFrame:
    r"""Aggregate a dataframe of PAF draws using the standard joint PAF formula:

      .. math::

        PAF_{joint} = 1 - \prod_{i=1}^{n}(1 - PAF_{i})

    where n = the number of risk factors being aggregated

    Args:
        paf_df: dataframe of PAF draws containing any index columns and draw columns
        collapse_cols: the list of index columns that are not included in grouping,
            and are collapsed during aggregation. These columns will not be included
            in the output dataframe.

    Returns a dataframe of aggregated PAF draws. Output columns will be all index
        columns from the input dataframe except those in collapse_cols, plus
        the draw columns.
    """
    index_cols, draw_cols = data_utils.get_index_draw_columns(paf_df)
    group_cols = list(set(index_cols) - set(collapse_cols))

    aggregated_pafs = (
        paf_df.groupby(group_cols)
        .apply(lambda row: 1 - (1 - row[draw_cols]).prod())
        .reset_index()
    )
    return aggregated_pafs


def evaluate_paf_formula(
    rr_at_tmrel: float,
    exposure_domain: npt.NDArray[np.float64],
    exposure_probability: npt.NDArray[np.float64],
    rr_over_exposure_domain: npt.NDArray[np.float64],
) -> float:
    """Find the exposure_weighted relative risk by integrating the exposure probability
    vector * relative risk over the exposure domain, and apply the PAF formula:
    PAF = (observed - ideal) / observed. This function operates over a single draw
    of inputs and produces a single PAF draw.
    """
    if np.all(rr_over_exposure_domain == rr_at_tmrel):
        return 0
    exposure_weighted_rr = np.trapz(
        y=exposure_probability * rr_over_exposure_domain, x=exposure_domain
    )
    if exposure_weighted_rr == 0:
        return 0
    paf = (exposure_weighted_rr - rr_at_tmrel) / exposure_weighted_rr
    return paf


def _expand_min_max_df(min_max_df: pd.DataFrame, exposure: pd.DataFrame) -> pd.DataFrame:
    """Expand min_max_df to include all demographic columns by merging exposure demographics
    on age_group_id, in order to provide a consistent set of index columns.
    """
    return min_max_df.merge(exposure[constants.DEMOGRAPHIC_COLS], on="age_group_id")

from typing import Dict, Tuple, TYPE_CHECKING

import numpy as np
import pandas as pd
import tqdm

from covid_model_seiir_pipeline.lib import (
    math,
    ode,
)
from covid_model_seiir_pipeline.pipeline.forecasting.model.containers import (
    Indices,
    PostprocessingParameters,
    RatioData,
    HospitalCorrectionFactors,
)

if TYPE_CHECKING:
    # The model subpackage is a library for the pipeline stage and shouldn't
    # explicitly depend on things outside the subpackage.
    from covid_model_seiir_pipeline.pipeline.forecasting.specification import (
        ScenarioSpecification,
    )
    # Support type checking but keep the pipeline stages as isolated as possible.
    from covid_model_seiir_pipeline.pipeline.regression.specification import (
        HospitalParameters,
    )


##############################
# ODE parameter construction #
##############################

def build_model_parameters(indices: Indices,
                           ode_parameters: pd.DataFrame,
                           beta_regression: pd.DataFrame,
                           covariates: pd.DataFrame,
                           coefficients: pd.DataFrame,
                           rhos: pd.DataFrame,
                           beta_scales: pd.DataFrame,
                           vaccine_data: pd.DataFrame,
                           log_beta_shift: Tuple[float, pd.Timestamp],
                           beta_scale: Tuple[float, pd.Timestamp]) -> ode.ForecastParameters:
    # These are all the same by draw.  Just broadcasting them over a new index.
    ode_params = {
        param: pd.Series(ode_parameters[param].mean(), index=indices.full, name=param)
        for param in ['alpha', 'sigma', 'gamma1', 'gamma2', 'pi', 'chi']
    }

    beta, beta_wild, beta_variant, beta_hat, rho, rho_variant, rho_b1617, rho_total = get_betas_and_prevalences(
        indices,
        beta_regression,
        covariates,
        coefficients,
        beta_scales,
        rhos,
        ode_parameters['kappa'].mean(),
        ode_parameters['phi'].mean(),
        ode_parameters['psi'].mean(),
        log_beta_shift, 
        beta_scale,
    )

    vaccine_data = vaccine_data.reindex(indices.full, fill_value=0)
    vaccine_data = {k: vaccine_data[k] for k in vaccine_data}

    return ode.ForecastParameters(
        **ode_params,
        beta=beta,
        beta_wild=beta_wild,
        beta_variant=beta_variant,
        beta_hat=beta_hat,
        rho=rho,
        rho_variant=rho_variant,
        rho_b1617=rho_b1617,
        rho_total=rho_total,
        **vaccine_data,
    )


def get_betas_and_prevalences(indices: Indices,
                              beta_regression: pd.DataFrame,
                              covariates: pd.DataFrame,
                              coefficients: pd.DataFrame,
                              beta_shift_parameters: pd.DataFrame,
                              rhos: pd.DataFrame,
                              kappa: float,
                              phi: float,
                              psi: float,
                              log_beta_shift: Tuple[float, pd.Timestamp],
                              beta_scale: Tuple[float, pd.Timestamp]) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series,
                                                                               pd.Series, pd.Series, pd.Series, pd.Series]:
    rhos = rhos.reindex(indices.full).fillna(method='ffill')

    log_beta_hat = math.compute_beta_hat(covariates, coefficients)
    log_beta_hat.loc[pd.IndexSlice[:, log_beta_shift[1]:]] += log_beta_shift[0]
    beta_hat = np.exp(log_beta_hat).loc[indices.future].rename('beta_hat').reset_index()

    beta = (beta_shift(beta_hat, beta_shift_parameters)
            .set_index(['location_id', 'date'])
            .beta_hat
            .rename('beta'))
    beta = beta_regression.loc[indices.past, 'beta'].append(beta).sort_index()
    beta.loc[pd.IndexSlice[:, beta_scale[1]:]] *= beta_scale[0]
    beta_wild = beta * (1 + kappa * rhos.rho)
    beta_variant = beta * (1 + kappa * (phi * (1 - rhos.rho_b1617) + rhos.rho_b1617 * psi))

    return (beta, beta_wild, beta_variant, np.exp(log_beta_hat),
            rhos.rho, rhos.rho_variant, rhos.rho_b1617, rhos.rho_total)


def beta_shift(beta_hat: pd.DataFrame,
               beta_scales: pd.DataFrame) -> pd.DataFrame:
    """Shift the raw predicted beta to line up with beta in the past.

    This method performs both an intercept shift and a scaling based on the
    residuals of the ode fit beta and the beta hat regression in the past.

    Parameters
    ----------
        beta_hat
            Dataframe containing the date, location_id, and beta hat in the
            future.
        beta_scales
            Dataframe containing precomputed parameters for the scaling.

    Returns
    -------
        Predicted beta, after scaling (shift).

    """
    beta_hat = beta_hat.sort_values(['location_id', 'date']).set_index('location_id')
    scale_init = beta_scales['scale_init']
    scale_final = beta_scales['scale_final']
    window_size = beta_scales['window_size']

    beta_final = []
    for location_id in beta_hat.index.unique():
        if window_size is not None:
            t = np.arange(len(beta_hat.loc[location_id])) / window_size.at[location_id]
            scale = scale_init.at[location_id] + (scale_final.at[location_id] - scale_init.at[location_id]) * t
            scale[(window_size.at[location_id] + 1):] = scale_final.at[location_id]
        else:
            scale = scale_init.at[location_id]
        loc_beta_hat = beta_hat.loc[location_id].set_index('date', append=True)['beta_hat']
        loc_beta_final = loc_beta_hat * scale
        beta_final.append(loc_beta_final)

    beta_final = pd.concat(beta_final).reset_index()

    return beta_final


#######################################
# Construct postprocessing parameters #
#######################################

def build_postprocessing_parameters(indices: Indices,
                                    past_compartments: pd.DataFrame,
                                    past_infections: pd.Series,
                                    past_deaths: pd.Series,
                                    ratio_data: RatioData,
                                    model_parameters: ode.ForecastParameters,
                                    correction_factors: HospitalCorrectionFactors,
                                    hospital_parameters: 'HospitalParameters',
                                    scenario_spec: 'ScenarioSpecification') -> PostprocessingParameters:
    ratio_data = correct_ratio_data(indices, ratio_data, model_parameters)

    correction_factors = forecast_correction_factors(
        indices,
        correction_factors,
        hospital_parameters,
    )

    return PostprocessingParameters(
        past_compartments=past_compartments,
        past_infections=past_infections,
        past_deaths=past_deaths,
        **ratio_data.to_dict(),
        **correction_factors.to_dict()
    )


def correct_ratio_data(indices: Indices,
                       ratio_data: RatioData,
                       model_params: ode.ForecastParameters) -> RatioData:
    variant_prevalence = model_params.rho_total
    p_start = variant_prevalence.loc[indices.initial_condition].reset_index(level='date', drop=True)
    variant_prevalence -= p_start.reindex(variant_prevalence.index, level='location_id')
    variant_prevalence[variant_prevalence < 0] = 0.0
    
    ifr_scalar = ratio_data.ifr_scalar * variant_prevalence + (1 - variant_prevalence)
    ifr_scalar = ifr_scalar.groupby('location_id').shift(ratio_data.infection_to_death).fillna(0.)
    ratio_data.ifr = ifr_scalar * _expand_rate(ratio_data.ifr, indices.full)
    ratio_data.ifr_lr = ifr_scalar * _expand_rate(ratio_data.ifr_lr, indices.full)
    ratio_data.ifr_hr = ifr_scalar * _expand_rate(ratio_data.ifr_hr, indices.full)
    
    ihr_scalar = ratio_data.ihr_scalar * variant_prevalence + (1 - variant_prevalence)
    ihr_scalar = ihr_scalar.groupby('location_id').shift(ratio_data.infection_to_admission).fillna(0.)
    ratio_data.ihr = ihr_scalar * _expand_rate(ratio_data.ihr, indices.full)

    ratio_data.idr = _expand_rate(ratio_data.idr, indices.full)
    ratio_data.ihr = _expand_rate(ratio_data.ihr, indices.full)
    return ratio_data


def _expand_rate(rate: pd.Series, index: pd.MultiIndex):
    return (rate
            .reindex(index)
            .groupby('location_id')
            .fillna(method='ffill')
            .fillna(method='bfill'))


def forecast_correction_factors(indices: Indices,
                                correction_factors: HospitalCorrectionFactors,
                                hospital_parameters: 'HospitalParameters') -> HospitalCorrectionFactors:
    averaging_window = pd.Timedelta(days=hospital_parameters.correction_factor_average_window)
    application_window = pd.Timedelta(days=hospital_parameters.correction_factor_application_window)

    new_cfs = {}
    for cf_name, cf in correction_factors.to_dict().items():
        cf = cf.reindex(indices.full)
        loc_cfs = []
        for loc_id, loc_today in indices.initial_condition.tolist():
            loc_cf = cf.loc[loc_id]
            mean_cf = loc_cf.loc[loc_today - averaging_window: loc_today].mean()
            loc_cf.loc[loc_today:] = np.nan
            loc_cf.loc[loc_today + application_window:] = mean_cf
            loc_cf = loc_cf.interpolate().reset_index()
            loc_cf['location_id'] = loc_id
            loc_cfs.append(loc_cf.set_index(['location_id', 'date'])[cf_name])
        new_cfs[cf_name] = pd.concat(loc_cfs).sort_index()
    return HospitalCorrectionFactors(**new_cfs)


###########
# Run ODE #
###########

def run_ode_model(initial_conditions: pd.DataFrame,
                  model_parameters: ode.ForecastParameters,
                  progress_bar: bool) -> pd.DataFrame:
    mp_dict = model_parameters.to_dict()
    ordered_fields = list(ode.PARAMETERS._fields) + list(ode.FORECAST_PARAMETERS._fields)

    parameters = pd.concat(
        [mp_dict[p] for p in ordered_fields]
        + [model_parameters.vaccinations_unprotected_lr,
           model_parameters.vaccinations_non_escape_protected_lr,
           model_parameters.vaccinations_escape_protected_lr,
           model_parameters.vaccinations_non_escape_immune_lr,
           model_parameters.vaccinations_escape_immune_lr,
           
           model_parameters.vaccinations_unprotected_hr,
           model_parameters.vaccinations_non_escape_protected_hr,                                                                                                                                                
           model_parameters.vaccinations_escape_protected_hr,                                                                                                                                                    
           model_parameters.vaccinations_non_escape_immune_hr,                                                                                                                                                   
           model_parameters.vaccinations_escape_immune_hr,],
        axis=1
    )

    forecasts = []
    initial_conditions_iter = tqdm.tqdm(initial_conditions.iterrows(),
                                        total=len(initial_conditions),
                                        disable=not progress_bar)
    for location_id, initial_condition in initial_conditions_iter:
        loc_parameters = parameters.loc[location_id].sort_index()
        loc_date = loc_parameters.reset_index().date
        loc_times = np.array((loc_date - loc_date.min()).dt.days)

        ic = initial_condition.values
        p = loc_parameters.values.T  # Each row is a param, each column a day

        solution = math.solve_ode(
            system=ode.forecast_system,
            t=loc_times,
            init_cond=ic,
            params=p
        )

        result = pd.DataFrame(
            data=solution.T,
            columns=initial_conditions.columns.tolist()
        )
        result['date'] = loc_date
        result['location_id'] = location_id
        forecasts.append(result.set_index(['location_id', 'date']))
    forecasts = pd.concat(forecasts).sort_index()
    return forecasts

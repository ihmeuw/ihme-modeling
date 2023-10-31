import functools
import multiprocessing
from typing import List, TYPE_CHECKING

import pandas as pd

from covid_model_seiir_pipeline.pipeline.postprocessing.model.aggregators import summarize

if TYPE_CHECKING:
    # The model subpackage is a library for the pipeline stage and shouldn't
    # explicitly depend on things outside the subpackage.
    from covid_model_seiir_pipeline.pipeline.postprocessing.data import PostprocessingDataInterface


def load_deaths(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
    _runner = functools.partial(
        data_interface.load_raw_output_deaths,
        scenario=scenario,
    )
    draws = range(data_interface.get_n_draws())
    with multiprocessing.Pool(num_cores) as pool:
        outputs = pool.map(_runner, draws)
    return outputs


def load_unscaled_deaths(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
    death_draws = load_deaths(scenario, data_interface, num_cores)
    em_scalars = data_interface.load_excess_mortality_scalars()
    em_scalars = em_scalars.reindex(death_draws[0].index).groupby('location_id').fillna(method='ffill')
    unscaled_deaths = [deaths / em_scalars.loc[:, draw] for draw, deaths in enumerate(death_draws)]
    return unscaled_deaths


def load_output_data(output_name: str, fallback: str = None):
    def inner(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
        try:
            return _load_output_data(scenario, output_name, data_interface, num_cores)
        except ValueError:
            if fallback is None:
                raise
            return _load_output_data(scenario, fallback, data_interface, num_cores)

    return inner


def load_vaccine_summaries(output_name: str):
    def inner(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
        n_draws = data_interface.get_n_draws()
        summary = data_interface.load_vaccination_summaries(output_name, scenario)
        summary = pd.concat([summary]*n_draws, axis=1)    
        summary.columns = list(range(n_draws))
        return summary
    return inner


def load_vaccine_efficacy_table(data_interface: 'PostprocessingDataInterface'):
    return data_interface.load_vaccine_efficacy()


def load_variant_prevalence(data_interface: 'PostprocessingDataInterface'):
    return data_interface.load_raw_variant_prevalence()


def load_ode_params(output_name: str):
    def inner(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
        return _load_ode_params(scenario, output_name, data_interface, num_cores)
    return inner


def load_effectively_vaccinated(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
    _runner = functools.partial(
        data_interface.load_effectively_vaccinated,
        scenario=scenario,
    )
    draws = range(data_interface.get_n_draws())
    with multiprocessing.Pool(num_cores) as pool:
        outputs = pool.map(_runner, draws)
    return outputs


def load_beta_residuals(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int) -> List[pd.Series]:
    _runner = functools.partial(
        data_interface.load_beta_residuals,
        scenario=scenario,
    )

    draws = range(data_interface.get_n_draws())
    with multiprocessing.Pool(num_cores) as pool:
        beta_residuals = pool.map(_runner, draws)
    return beta_residuals


def load_scaled_beta_residuals(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int) -> List[pd.Series]:
    _runner = functools.partial(
        data_interface.load_scaled_beta_residuals,
        scenario=scenario,
    )

    draws = range(data_interface.get_n_draws())
    with multiprocessing.Pool(num_cores) as pool:
        beta_residuals = pool.map(_runner, draws)
    return beta_residuals


def load_covariate(covariate: str, time_varying: bool, scenario: str,
                   data_interface: 'PostprocessingDataInterface', num_cores: int) -> List[pd.Series]:
    _runner = functools.partial(
        data_interface.load_covariate,
        covariate=covariate,
        time_varying=time_varying,
        scenario=scenario,
    )
    draws = range(data_interface.get_n_draws())
    with multiprocessing.Pool(num_cores) as pool:
        outputs = pool.map(_runner, draws)

    return outputs


def load_coefficients(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
    draws = range(data_interface.get_n_draws())
    with multiprocessing.Pool(num_cores) as pool:
        outputs = pool.map(data_interface.load_regression_coefficients, draws)
    return outputs


def load_excess_mortality_scalars(data_interface: 'PostprocessingDataInterface'):
    return summarize(data_interface.load_excess_mortality_scalars())


def load_raw_census_data(data_interface: 'PostprocessingDataInterface'):
    return data_interface.load_hospital_census_data()


def load_hospital_correction_factors(data_interface: 'PostprocessingDataInterface'):
    return data_interface.load_hospital_correction_factors()


def load_hospital_bed_capacity(data_interface: 'PostprocessingDataInterface'):
    return data_interface.load_hospital_bed_capacity()


def load_scaling_parameters(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
    _runner = functools.partial(
        data_interface.load_scaling_parameters,
        scenario=scenario,
    )
    draws = range(data_interface.get_n_draws())
    with multiprocessing.Pool(num_cores) as pool:
        outputs = pool.map(_runner, draws)
    return outputs


def load_full_data_unscaled(data_interface: 'PostprocessingDataInterface') -> pd.DataFrame:
    full_data = data_interface.load_full_data_unscaled().reset_index()
    location_ids = data_interface.load_location_ids()
    full_data = full_data[full_data.location_id.isin(location_ids)].set_index(['location_id', 'date'])
    return full_data


def load_total_covid_deaths(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int) -> pd.DataFrame:
    full_data = load_full_data_unscaled(data_interface)
    deaths = full_data['cumulative_deaths'].dropna()
    em_scalars = (data_interface.load_excess_mortality_scalars()
                  .reindex(deaths.index)
                  .groupby('location_id')
                  .fillna(method='ffill'))
    init_cond = em_scalars.mul(deaths.values, axis=0).groupby('location_id').first()
    scaled_deaths = em_scalars.mul(deaths.groupby('location_id').diff().values, axis=0).groupby('location_id').cumsum().fillna(0.0)
    scaled_deaths = scaled_deaths + init_cond.reindex(scaled_deaths.index, level='location_id')
    return scaled_deaths


def load_age_specific_deaths(data_interface: 'PostprocessingDataInterface') -> pd.DataFrame:
    full_data = data_interface.load_full_data()
    total_deaths = (full_data
                    .groupby('location_id')
                    .cumulative_deaths
                    .max()
                    .dropna())
    mortality_ratio = data_interface.load_mortality_ratio()
    age_specific_deaths = (mortality_ratio * total_deaths).rename('age_specific_deaths').to_frame()
    return age_specific_deaths


def build_version_map(data_interface: 'PostprocessingDataInterface') -> pd.Series:
    return data_interface.build_version_map()


def load_populations(data_interface: 'PostprocessingDataInterface'):
    idx_cols = ['location_id', 'sex_id', 'year_id', 
                'age_group_id', 'age_group_years_start', 'age_group_years_end']
    return data_interface.load_population().set_index(idx_cols)


def load_hierarchy(data_interface: 'PostprocessingDataInterface'):
    return data_interface.load_hierarchy()


def get_locations_modeled_and_missing(data_interface: 'PostprocessingDataInterface'):
    return data_interface.get_locations_modeled_and_missing()


def load_ifr_es(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
    draws = range(data_interface.get_n_draws())
    with multiprocessing.Pool(num_cores) as pool:
        outputs = pool.map(data_interface.load_ifr, draws)
    return outputs


def load_ifr_high_risk_es(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
    draws = range(data_interface.get_n_draws())
    with multiprocessing.Pool(num_cores) as pool:
        outputs = pool.map(data_interface.load_ifr_hr, draws)
    return outputs


def load_ifr_low_risk_es(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
    draws = range(data_interface.get_n_draws())
    with multiprocessing.Pool(num_cores) as pool:
        outputs = pool.map(data_interface.load_ifr_lr, draws)
    return outputs


def load_infection_to_death(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
    draws = range(data_interface.get_n_draws())
    with multiprocessing.Pool(num_cores) as pool:
        outputs = pool.map(data_interface.load_infection_to_death, draws)
    return outputs


def load_ihr_es(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
    draws = range(data_interface.get_n_draws())
    with multiprocessing.Pool(num_cores) as pool:
        outputs = pool.map(data_interface.load_ihr, draws)
    return outputs


def load_infection_to_admission(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
    draws = range(data_interface.get_n_draws())
    with multiprocessing.Pool(num_cores) as pool:
        outputs = pool.map(data_interface.load_infection_to_admission, draws)
    return outputs


def load_idr_es(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
    draws = range(data_interface.get_n_draws())
    with multiprocessing.Pool(num_cores) as pool:
        outputs = pool.map(data_interface.load_idr, draws)
    return outputs


def load_infection_to_case(scenario: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
    draws = range(data_interface.get_n_draws())
    with multiprocessing.Pool(num_cores) as pool:
        outputs = pool.map(data_interface.load_infection_to_case, draws)
    return outputs


#########################
# Non-interface methods #
#########################

def _load_output_data(scenario: str, measure: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
    _runner = functools.partial(
        data_interface.load_single_raw_output,
        scenario=scenario,
        measure=measure,
    )
    draws = range(data_interface.get_n_draws())
    with multiprocessing.Pool(num_cores) as pool:
        outputs = pool.map(_runner, draws)
    return outputs


def _load_ode_params(scenario: str, measure: str, data_interface: 'PostprocessingDataInterface', num_cores: int):
    _runner = functools.partial(
        data_interface.load_single_ode_param,
        scenario=scenario,
        measure=measure,
    )
    draws = range(data_interface.get_n_draws())
    with multiprocessing.Pool(num_cores) as pool:
        outputs = pool.map(_runner, draws)
    return outputs

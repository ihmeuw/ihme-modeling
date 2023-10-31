import functools
from typing import Any, Callable, Dict, List, Union, TYPE_CHECKING

import pandas as pd

from covid_model_seiir_pipeline.pipeline.postprocessing.model import aggregators, loaders, combiners

if TYPE_CHECKING:
    # The model subpackage is a library for the pipeline stage and shouldn't
    # explicitly depend on things outside the subpackage.
    from covid_model_seiir_pipeline.pipeline.postprocessing.data import PostprocessingDataInterface


class MeasureConfig:
    def __init__(self,
                 loader: Callable[[str, 'PostprocessingDataInterface', int], Any],
                 label: str,
                 splice: bool = True,
                 resample: bool = True,
                 calculate_cumulative: bool = False,
                 cumulative_label: str = None,
                 aggregator: Callable = None,
                 write_draws: bool = False):
        self.loader = loader
        self.label = label
        self.splice = splice
        self.resample = resample
        self.calculate_cumulative = calculate_cumulative
        self.cumulative_label = cumulative_label
        self.aggregator = aggregator
        self.write_draws = write_draws


class CompositeMeasureConfig:
    def __init__(self,
                 base_measures: Dict[str, MeasureConfig],
                 label: str,
                 combiner: Callable,
                 write_draws: bool = False):
        self.base_measures = base_measures
        self.label = label
        self.combiner = combiner
        self.write_draws = write_draws


class CovariateConfig:
    def __init__(self,
                 loader: Callable[[str, bool, str, 'PostprocessingDataInterface', int], List[pd.Series]],
                 label: str,
                 splice: bool = False,
                 time_varying: bool = False,
                 aggregator: Callable = None,
                 write_draws: bool = False):
        self.loader = loader
        self.label = label
        self.splice = splice
        self.time_varying = time_varying
        self.aggregator = aggregator
        self.write_draws = write_draws


class MiscellaneousConfig:
    def __init__(self,
                 loader: Callable[['PostprocessingDataInterface'], Any],
                 label: str,
                 is_table: bool = True,
                 aggregator: Callable = None,
                 soft_fail: bool = False):
        self.loader = loader
        self.label = label
        self.is_table = is_table
        self.aggregator = aggregator
        self.soft_fail = soft_fail


DataConfig = Union[MeasureConfig, CovariateConfig, CompositeMeasureConfig, MiscellaneousConfig]


MEASURES = {
    # Death measures
    'deaths': MeasureConfig(
        loaders.load_deaths,
        'daily_deaths',
        calculate_cumulative=True,
        cumulative_label='cumulative_deaths',
        aggregator=aggregators.sum_aggregator,
        write_draws=True,
    ),
    'unscaled_deaths': MeasureConfig(
        loaders.load_unscaled_deaths,
        'unscaled_daily_deaths',
        calculate_cumulative=True,
        cumulative_label='cumulative_unscaled_deaths',
        aggregator=aggregators.sum_aggregator,
        write_draws=True,
    ),
    'total_covid_deaths_data': MeasureConfig(
        loaders.load_total_covid_deaths,
        'total_covid_deaths_data',
        aggregator=aggregators.sum_aggregator,
    ),
    'deaths_wild': MeasureConfig(
        loaders.load_output_data('modeled_deaths_wild'),
        'daily_deaths_wild',
        aggregator=aggregators.sum_aggregator,
    ),
    'deaths_variant': MeasureConfig(
        loaders.load_output_data('modeled_deaths_variant'),
        'daily_deaths_variant',
        aggregator=aggregators.sum_aggregator,
    ),
    'deaths_lr': MeasureConfig(
        loaders.load_output_data('modeled_deaths_lr'),
        'daily_deaths_low_risk',
        splice=False,
        aggregator=aggregators.sum_aggregator,
    ),
    'deaths_hr': MeasureConfig(
        loaders.load_output_data('modeled_deaths_hr'),
        'daily_deaths_high_risk',
        splice=False,
        aggregator=aggregators.sum_aggregator,
    ),
    'deaths_modeled': MeasureConfig(
        loaders.load_output_data('modeled_deaths_total'),
        'daily_deaths_modeled',
        splice=False,
        aggregator=aggregators.sum_aggregator,
    ),

    # Infection measures

    'infections': MeasureConfig(
        loaders.load_output_data('infections'),
        'daily_infections',
        calculate_cumulative=True,
        cumulative_label='cumulative_infections',
        aggregator=aggregators.sum_aggregator,
        write_draws=True,
    ),
    'infected': MeasureConfig(
        loaders.load_output_data('modeled_infected_total'),
        'daily_infected',
        calculate_cumulative=True,
        cumulative_label='cumulative_infected',
        aggregator=aggregators.sum_aggregator,
        write_draws=True,
    ),
    'infections_wild': MeasureConfig(
        loaders.load_output_data('modeled_infections_wild'),
        'daily_infections_wild',
        aggregator=aggregators.sum_aggregator,
    ),
    'infections_variant': MeasureConfig(
        loaders.load_output_data('modeled_infections_variant'),
        'daily_infections_variant',
        aggregator=aggregators.sum_aggregator,
    ),
    'infections_natural_breakthrough': MeasureConfig(
        loaders.load_output_data(
            'modeled_infections_natural_breakthrough',
            fallback='natural_immunity_breakthrough',
        ),
        'daily_infections_natural_immunity_breakthrough',
        aggregator=aggregators.sum_aggregator,
    ),
    'infections_vaccine_breakthrough': MeasureConfig(
        loaders.load_output_data(
            'modeled_infections_vaccine_breakthrough',
            fallback='vaccine_breakthrough',
        ),
        'daily_infections_vaccine_breakthrough',
        aggregator=aggregators.sum_aggregator,
    ),
    'infections_unvaccinated_wild': MeasureConfig(
        loaders.load_output_data('modeled_infections_unvaccinated_wild'),
        'daily_infections_unvaccinated_wild',
        aggregator=aggregators.sum_aggregator,
        splice=False,
    ),
    'infections_unvaccinated_variant': MeasureConfig(
        loaders.load_output_data('modeled_infections_unvaccinated_variant'),
        'daily_infections_unvaccinated_variant',
        aggregator=aggregators.sum_aggregator,
        splice=False,
    ),
    'infections_unvaccinated_total': MeasureConfig(
        loaders.load_output_data('modeled_infections_unvaccinated_total'),
        'daily_infections_unvaccinated_total',
        aggregator=aggregators.sum_aggregator,
        splice=False,
    ),
    'infections_unvaccinated_natural_breakthrough': MeasureConfig(
        loaders.load_output_data(
            'modeled_infections_unvaccinated_natural_breakthrough',
        ),
        'daily_infections_unvaccinated_natural_immunity_breakthrough',
        aggregator=aggregators.sum_aggregator,
        splice=False,
    ),
    'infections_lr': MeasureConfig(
        loaders.load_output_data('modeled_infections_lr'),
        'daily_infections_low_risk',
        splice=False,
        aggregator=aggregators.sum_aggregator,
    ),
    'infections_hr': MeasureConfig(
        loaders.load_output_data('modeled_infections_hr'),
        'daily_infections_high_risk',
        aggregator=aggregators.sum_aggregator,
        splice=False,
    ),
    'infections_modeled': MeasureConfig(
        loaders.load_output_data('modeled_infections_total'),
        'daily_infections_modeled',
        splice=False,
        aggregator=aggregators.sum_aggregator,
    ),
    'cases': MeasureConfig(
        loaders.load_output_data('cases'),
        'daily_cases',
        calculate_cumulative=True,
        cumulative_label='cumulative_cases',
        aggregator=aggregators.sum_aggregator,
        write_draws=True,
    ),

    # Hospital measures

    'hospital_admissions': MeasureConfig(
        loaders.load_output_data('hospital_admissions'),
        'hospital_admissions',
        aggregator=aggregators.sum_aggregator,
        write_draws=True,
    ),
    'icu_admissions': MeasureConfig(
        loaders.load_output_data('icu_admissions'),
        'icu_admissions',
        aggregator=aggregators.sum_aggregator,
        write_draws=True,
    ),
    'hospital_census': MeasureConfig(
        loaders.load_output_data('hospital_census'),
        'hospital_census',
        aggregator=aggregators.sum_aggregator,
    ),
    'icu_census': MeasureConfig(
        loaders.load_output_data('icu_census'),
        'icu_census',
        aggregator=aggregators.sum_aggregator,
    ),
    'hospital_census_correction_factor': MeasureConfig(
        loaders.load_output_data('hospital_census_correction_factor'),
        'hospital_census_correction_factor',
        splice=False,
    ),
    'icu_census_correction_factor': MeasureConfig(
        loaders.load_output_data('icu_census_correction_factor'),
        'icu_census_correction_factor',
        splice=False,
    ),

    # Vaccination measures

    'effectively_vaccinated': MeasureConfig(
        loaders.load_effectively_vaccinated,
        'daily_vaccinations_effective_input',
        calculate_cumulative=True,
        cumulative_label='cumulative_vaccinations_effective_input',
        aggregator=aggregators.sum_aggregator,
        splice=False,
    ),
    'cumulative_all_effective': MeasureConfig(
        loaders.load_vaccine_summaries('cumulative_all_effective'),
        'cumulative_vaccinations_all_effective',
        aggregator=aggregators.sum_aggregator,
        resample=False,
        splice=False,
    ),
    'cumulative_all_vaccinated': MeasureConfig(
        loaders.load_vaccine_summaries('cumulative_all_vaccinated'),
        'cumulative_vaccinations_all_vaccinated',
        aggregator=aggregators.sum_aggregator,
        resample=False,
        splice=False,
    ),
    'cumulative_all_fully_vaccinated': MeasureConfig(
        loaders.load_vaccine_summaries('cumulative_all_fully_vaccinated'),
        'cumulative_vaccinations_all_fully_vaccinated',
        aggregator=aggregators.sum_aggregator,
        resample=False,
        splice=False,
    ),
    'cumulative_lr_vaccinated': MeasureConfig(
        loaders.load_vaccine_summaries('lr_vaccinated'),
        'cumulative_vaccinations_lr',
        aggregator=aggregators.sum_aggregator,
        resample=False,
        splice=False,
    ),
    'cumulative_hr_vaccinated': MeasureConfig(
        loaders.load_vaccine_summaries('hr_vaccinated'),
        'cumulative_vaccinations_hr',
        aggregator=aggregators.sum_aggregator,
        resample=False,
        splice=False,
    ),
    'vaccine_acceptance': MeasureConfig(
        loaders.load_vaccine_summaries('vaccine_acceptance'),
        'vaccine_acceptance',
        aggregator=aggregators.mean_aggregator,
        resample=False,
        splice=False,
    ),
    'vaccine_acceptance_point': MeasureConfig(
        loaders.load_vaccine_summaries('vaccine_acceptance_point'),
        'vaccine_acceptance_point',
        aggregator=aggregators.mean_aggregator,
        resample=False,
        splice=False,
    ),

    'vaccines_immune_all': MeasureConfig(
        loaders.load_output_data('vaccinations_immune_all'),
        'daily_vaccinations_all_immune',
        aggregator=aggregators.sum_aggregator,
    ),
    'vaccines_immune_wild': MeasureConfig(
        loaders.load_output_data('vaccinations_immune_wild'),
        'daily_vaccinations_wild_immune',
        aggregator=aggregators.sum_aggregator,
    ),
    'vaccines_protected_all': MeasureConfig(
        loaders.load_output_data('vaccinations_protected_all'),
        'daily_vaccinations_all_protected',
        aggregator=aggregators.sum_aggregator,
    ),
    'vaccines_protected_wild': MeasureConfig(
        loaders.load_output_data('vaccinations_protected_wild'),
        'daily_vaccinations_wild_protected',
        aggregator=aggregators.sum_aggregator,
    ),
    'vaccines_effective': MeasureConfig(
        loaders.load_output_data('vaccinations_effective'),
        'daily_vaccinations_effective',
        calculate_cumulative=True,
        cumulative_label='cumulative_vaccinations_effective',
        aggregator=aggregators.sum_aggregator,
    ),
    'vaccines_ineffective': MeasureConfig(
        loaders.load_output_data('vaccinations_ineffective'),
        'daily_vaccinations_ineffective',
        aggregator=aggregators.sum_aggregator,
    ),
    'vaccines_n_unvaccinated': MeasureConfig(
        loaders.load_output_data('vaccinations_n_unvaccinated'),
        'total_unvaccinated',
        aggregator=aggregators.sum_aggregator,
        splice=False,
    ),

    # Other epi measures

    'total_susceptible_wild': MeasureConfig(
        loaders.load_output_data('total_susceptible_wild'),
        'total_susceptible_wild',
        aggregator=aggregators.sum_aggregator,
    ),
    'total_susceptible_variant': MeasureConfig(
        loaders.load_output_data('total_susceptible_variant'),
        'total_susceptible_variant',
        aggregator=aggregators.sum_aggregator,
    ),
    'total_susceptible_variant_only': MeasureConfig(
        loaders.load_output_data('total_susceptible_variant_only'),
        'total_susceptible_variant_only',
        aggregator=aggregators.sum_aggregator,
    ),
    'total_susceptible_variant_unprotected': MeasureConfig(
        loaders.load_output_data('total_susceptible_variant_unprotected'),
        'total_susceptible_variant_unprotected',
        aggregator=aggregators.sum_aggregator,
        splice=False,
    ),
    'total_susceptible_unvaccinated_wild': MeasureConfig(
        loaders.load_output_data('total_susceptible_unvaccinated_wild'),
        'total_susceptible_unvaccinated_wild',
        aggregator=aggregators.sum_aggregator,
        splice=False,
    ),
    'total_susceptible_unvaccinated_variant': MeasureConfig(
        loaders.load_output_data('total_susceptible_unvaccinated_variant'),
        'total_susceptible_unvaccinated_variant',
        aggregator=aggregators.sum_aggregator,
        splice=False,
    ),
    'total_susceptible_unvaccinated_variant_only': MeasureConfig(
        loaders.load_output_data('total_susceptible_unvaccinated_variant_only'),
        'total_susceptible_unvaccinated_variant_only',
        aggregator=aggregators.sum_aggregator,
        splice=False,
    ),
    'total_immune_wild': MeasureConfig(
        loaders.load_output_data('total_immune_wild'),
        'total_immune_wild',
        aggregator=aggregators.sum_aggregator,
    ),
    'total_immune_variant': MeasureConfig(
        loaders.load_output_data('total_immune_variant'),
        'total_immune_variant',
        aggregator=aggregators.sum_aggregator,
    ),
    'r_controlled_wild': MeasureConfig(
        loaders.load_output_data('r_controlled_wild'),
        'r_controlled_wild',
    ),
    'r_effective_wild': MeasureConfig(
        loaders.load_output_data('r_effective_wild'),
        'r_effective_wild',
    ),
    'r_controlled_variant': MeasureConfig(
        loaders.load_output_data('r_controlled_variant'),
        'r_controlled_variant',
    ),
    'r_effective_variant': MeasureConfig(
        loaders.load_output_data('r_effective_variant'),
        'r_effective_variant',
    ),
    'r_controlled': MeasureConfig(
        loaders.load_output_data('r_controlled'),
        'r_controlled',
        splice=False,
    ),
    'r_effective': MeasureConfig(
        loaders.load_output_data('r_effective'),
        'r_effective',
    ),
    'incidence_wild': MeasureConfig(
        loaders.load_output_data('incidence_wild'),
        'incidence_wild',
        splice=False,
    ),
    'incidence_variant': MeasureConfig(
        loaders.load_output_data('incidence_variant'),
        'incidence_variant',
        splice=False,
    ),
    'incidence_total': MeasureConfig(
        loaders.load_output_data('incidence_total'),
        'incidence_total',
        splice=False,
    ),
    'incidence_unvaccinated_wild': MeasureConfig(
        loaders.load_output_data('incidence_unvaccinated_wild'),
        'incidence_unvaccinated_wild',
        splice=False,
    ),
    'incidence_unvaccinated_variant': MeasureConfig(
        loaders.load_output_data('incidence_unvaccinated_variant'),
        'incidence_unvaccinated_variant',
        splice=False,
    ),
    'incidence_unvaccinated_total': MeasureConfig(
        loaders.load_output_data('incidence_unvaccinated_total'),
        'incidence_unvaccinated_total',
        splice=False,
    ),
    'force_of_infection': MeasureConfig(
        loaders.load_output_data('force_of_infection'),
        'force_of_infection',
        splice=False,
    ),
    'force_of_infection_unvaccinated': MeasureConfig(
        loaders.load_output_data('force_of_infection_unvaccinated'),
        'force_of_infection_unvaccinated',
        splice=False,
    ),
    'force_of_infection_unvaccinated_naive': MeasureConfig(
        loaders.load_output_data('force_of_infection_unvaccinated_naive'),
        'force_of_infection_unvaccinated_naive',
        splice=False,
    ),
    'force_of_infection_unvaccinated_natural_breakthrough': MeasureConfig(
        loaders.load_output_data('force_of_infection_unvaccinated_natural_breakthrough'),
        'force_of_infection_unvaccinated_natural_breakthrough',
        splice=False,
    ),

    # Betas

    'beta': MeasureConfig(
        loaders.load_ode_params('beta'),
        'betas',
    ),
    'beta_hat': MeasureConfig(
        loaders.load_ode_params('beta_hat'),
        'beta_hat',
    ),
    'beta_wild': MeasureConfig(
        loaders.load_ode_params('beta_wild'),
        'beta_wild',
    ),
    'beta_variant': MeasureConfig(
        loaders.load_ode_params('beta_variant'),
        'beta_variant',
    ),
    'empirical_beta': MeasureConfig(
        loaders.load_output_data('beta'),
        'empirical_beta',
    ),
    'empirical_beta_wild': MeasureConfig(
        loaders.load_output_data('beta_wild'),
        'empirical_beta_wild',
    ),
    'empirical_beta_variant': MeasureConfig(
        loaders.load_output_data('beta_variant'),
        'empirical_beta_variant',
    ),

    'non_escape_variant_prevalence': MeasureConfig(
        loaders.load_ode_params('rho'),
        'non_escape_variant_prevalence',
    ),
    'escape_variant_prevalence': MeasureConfig(
        loaders.load_ode_params('rho_variant'),
        'escape_variant_prevalence',
    ),

    # Beta calculation inputs

    'beta_residuals': MeasureConfig(
        loaders.load_beta_residuals,
        'log_beta_residuals',
    ),
    'scaled_beta_residuals': MeasureConfig(
        loaders.load_scaled_beta_residuals,
        'scaled_log_beta_residuals',
    ),
    'coefficients': MeasureConfig(
        loaders.load_coefficients,
        'coefficients',
        write_draws=True,
    ),
    'scaling_parameters': MeasureConfig(
        loaders.load_scaling_parameters,
        'beta_scaling_parameters',
        write_draws=True,
    ),
    'infection_fatality_ratio_es': MeasureConfig(
        loaders.load_ifr_es,
        'infection_fatality_ratio_es',
    ),
    'infection_fatality_ratio_high_risk_es': MeasureConfig(
        loaders.load_ifr_high_risk_es,
        'infection_fatality_ratio_high_risk_es',
    ),
    'infection_fatality_ratio_low_risk_es': MeasureConfig(
        loaders.load_ifr_low_risk_es,
        'infection_fatality_ratio_low_risk_es',
    ),
    'infection_to_death': MeasureConfig(
        loaders.load_infection_to_death,
        'infection_to_death',
    ),
    'infection_detection_ratio_es': MeasureConfig(
        loaders.load_idr_es,
        'infection_detection_ratio_es',
    ),
    'infection_to_case': MeasureConfig(
        loaders.load_infection_to_case,
        'infection_to_case',
    ),
    'infection_hospitalization_ratio_es': MeasureConfig(
        loaders.load_ihr_es,
        'infection_hospitalization_ratio_es',
    ),
    'infection_to_admission': MeasureConfig(
        loaders.load_infection_to_admission,
        'infection_to_admission',
    ),
}


COMPOSITE_MEASURES = {
    'infection_fatality_ratio': CompositeMeasureConfig(
        base_measures={'numerator': MEASURES['deaths'],
                       'denominator': MEASURES['infections'],
                       'duration': MEASURES['infection_to_death']},
        label='infection_fatality_ratio',
        combiner=combiners.make_ratio,
    ),
    'infection_fatality_ratio_modeled': CompositeMeasureConfig(
        base_measures={'numerator': MEASURES['deaths_modeled'],
                       'denominator': MEASURES['infections_modeled'],
                       'duration': MEASURES['infection_to_death']},
        label='infection_fatality_ratio_modeled',
        combiner=combiners.make_ratio,
    ),
    'infection_fatality_ratio_high_risk': CompositeMeasureConfig(
        base_measures={'numerator': MEASURES['deaths_hr'],
                       'denominator': MEASURES['infections_hr'],
                       'duration': MEASURES['infection_to_death']},
        label='infection_fatality_ratio_high_risk',
        combiner=combiners.make_ratio,
    ),
    'infection_fatality_ratio_low_risk': CompositeMeasureConfig(
        base_measures={'numerator': MEASURES['deaths_lr'],
                       'denominator': MEASURES['infections_lr'],
                       'duration': MEASURES['infection_to_death']},
        label='infection_fatality_ratio_low_risk',
        combiner=combiners.make_ratio,
    ),

    'infection_hospitalization_ratio': CompositeMeasureConfig(
        base_measures={'numerator': MEASURES['hospital_admissions'],
                       'denominator': MEASURES['infections'],
                       'duration': MEASURES['infection_to_admission']},
        label='infection_hospitalization_ratio',
        combiner=combiners.make_ratio,
    ),
    'infection_detection_ratio': CompositeMeasureConfig(
        base_measures={'numerator': MEASURES['cases'],
                       'denominator': MEASURES['infections'],
                       'duration': MEASURES['infection_to_case']},
        label='infection_detection_ratio',
        combiner=combiners.make_ratio,
    ),
    'empirical_escape_variant_prevalence': CompositeMeasureConfig(
        base_measures={'numerator': MEASURES['infections_variant'],
                       'denominator': MEASURES['infections_modeled']},
        label='empirical_escape_variant_prevalence',
        combiner=combiners.make_ratio,
    ),
}


COVARIATES = {
    'mobility': CovariateConfig(
        loaders.load_covariate,
        'mobility',
        time_varying=True,
        aggregator=aggregators.mean_aggregator,
    ),
    'testing': CovariateConfig(
        loaders.load_covariate,
        'testing',
        time_varying=True,
        aggregator=aggregators.mean_aggregator,
    ),
    'pneumonia': CovariateConfig(
        loaders.load_covariate,
        'pneumonia',
        time_varying=True,
        aggregator=aggregators.mean_aggregator,
    ),
    'mask_use': CovariateConfig(
        loaders.load_covariate,
        'mask_use',
        time_varying=True,
        aggregator=aggregators.mean_aggregator,
    ),
    'air_pollution_pm_2_5': CovariateConfig(
        loaders.load_covariate,
        'air_pollution_pm_2_5',
        aggregator=aggregators.mean_aggregator,
    ),
    'lri_mortality': CovariateConfig(
        loaders.load_covariate,
        'lri_mortality',
        aggregator=aggregators.mean_aggregator,
    ),
    'proportion_over_2_5k': CovariateConfig(
        loaders.load_covariate,
        'proportion_over_2_5k',
        aggregator=aggregators.mean_aggregator,
    ),
    'proportion_under_100m': CovariateConfig(
        loaders.load_covariate,
        'proportion_under_100m',
        aggregator=aggregators.mean_aggregator,
    ),
    'smoking_prevalence': CovariateConfig(
        loaders.load_covariate,
        'smoking_prevalence',
        aggregator=aggregators.mean_aggregator,
    ),
}

MISCELLANEOUS = {
    'unscaled_full_data': MiscellaneousConfig(
        loaders.load_full_data_unscaled,
        'unscaled_full_data',
        aggregator=aggregators.sum_aggregator,
    ),
    'age_specific_deaths': MiscellaneousConfig(
        loaders.load_age_specific_deaths,
        'age_specific_deaths',
        aggregator=aggregators.sum_aggregator,
        soft_fail=True,
    ),
    'variant_prevalence': MiscellaneousConfig(
        loaders.load_variant_prevalence,
        'variant_prevalence',
        aggregator=aggregators.mean_aggregator,
    ),
    'excess_mortality_scalars': MiscellaneousConfig(
        loaders.load_excess_mortality_scalars,
        'excess_mortality_scalars',
    ),
    'hospital_census_data': MiscellaneousConfig(
        loaders.load_raw_census_data,
        'hospital_census_data',
    ),
    'hospital_bed_capacity': MiscellaneousConfig(
        loaders.load_hospital_bed_capacity,
        'hospital_bed_capacity',
    ),
    'vaccine_efficacy_table': MiscellaneousConfig(
        loaders.load_vaccine_efficacy_table,
        'vaccine_efficacy_table',
    ),
#    'version_map': MiscellaneousConfig(
#        loaders.build_version_map,
#        'version_map',
#    ),
    'populations': MiscellaneousConfig(
        loaders.load_populations,
        'populations',
        aggregator=aggregators.sum_aggregator,
    ),
    'hierarchy': MiscellaneousConfig(
        loaders.load_hierarchy,
        'hierarchy',
    ),
    'locations_modeled_and_missing': MiscellaneousConfig(
        loaders.get_locations_modeled_and_missing,
        'modeled_and_missing_locations',
        is_table=False,
    ),
}

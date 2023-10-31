import itertools
from typing import Dict, Tuple, TYPE_CHECKING

import pandas as pd

from covid_model_seiir_pipeline.lib import (
    ode,
)
from covid_model_seiir_pipeline.pipeline.forecasting.model.containers import (
    Indices,
    PostprocessingParameters,
    HospitalMetrics,
    SystemMetrics,
    OutputMetrics,
)
from covid_model_seiir_pipeline.pipeline.regression.model import (
    compute_hospital_usage,
)

if TYPE_CHECKING:
    # Support type checking but keep the pipeline stages as isolated as possible.
    from covid_model_seiir_pipeline.pipeline.regression.specification import (
        HospitalParameters,
    )


def compute_output_metrics(indices: Indices,
                           future_components: pd.DataFrame,
                           postprocessing_params: PostprocessingParameters,
                           model_parameters: ode.ForecastParameters,
                           hospital_parameters: 'HospitalParameters') -> Tuple[pd.DataFrame,
                                                                               SystemMetrics,
                                                                               OutputMetrics]:
    components = postprocessing_params.past_compartments
    components = (components
                  .loc[indices.past]  # Need to drop transition day.
                  .append(future_components)
                  .sort_index())

    system_metrics = variant_system_metrics(
        indices,
        model_parameters,
        postprocessing_params,
        components,
    )

    infections = postprocessing_params.past_infections.loc[indices.past].append(
        system_metrics.modeled_infections_total.loc[indices.future]
    )
    past_deaths = postprocessing_params.past_deaths
    modeled_deaths = system_metrics.modeled_deaths_total
    deaths = (past_deaths
              .append(modeled_deaths
                      .loc[modeled_deaths
                           .dropna()
                           .index
                           .difference(past_deaths.index)])
              .rename('deaths')
              .to_frame())
    deaths['observed'] = 0
    deaths.loc[past_deaths.index, 'observed'] = 1
    deaths = deaths.set_index('observed', append=True).deaths
    cases = (infections
             .groupby('location_id')
             .shift(postprocessing_params.infection_to_case)
             * postprocessing_params.idr)
    admissions = (infections
                  .groupby('location_id')
                  .shift(postprocessing_params.infection_to_admission)
                  * postprocessing_params.ihr)
    hospital_usage = compute_corrected_hospital_usage(
        admissions,
        hospital_parameters,
        postprocessing_params,
    )

    effective_r = compute_effective_r(
        model_parameters,
        system_metrics,
    )

    output_metrics = OutputMetrics(
        infections=infections,
        cases=cases,
        deaths=deaths,
        **hospital_usage.to_dict(),
        # Other stuff
        **effective_r,
    )
    return components, system_metrics, output_metrics


def variant_system_metrics(indices: Indices,
                           model_parameters: ode.ForecastParameters,
                           postprocessing_params: PostprocessingParameters,
                           components: pd.DataFrame) -> SystemMetrics:
    components_diff = components.groupby('location_id').diff()

    cols = [f'{c}_{g}' for g, c in itertools.product(['lr', 'hr'], ode.COMPARTMENT_NAMES)]
    tracking_cols = [f'{c}_{g}' for g, c in itertools.product(['lr', 'hr'], ode.TRACKING_COMPARTMENT_NAMES)]

    infections = _make_infections(components_diff)
    infected = infections['modeled_infections_total'] - infections['modeled_infections_natural_breakthrough']
    group_infections, group_deaths = _make_group_infections_and_deaths_metrics(
        components,
        components_diff,
        postprocessing_params,
    )

    susceptible = _make_susceptible(components)
    infectious = _make_infectious(components)
    immune = _make_immune(components)
    vaccinations = _make_vaccinations(components, components_diff)
    total_pop = components[cols].sum(axis=1)
    betas = _make_betas(infections, susceptible, infectious, model_parameters.alpha, total_pop)
    incidence = _make_incidence(infections, total_pop)
    force_of_infection = _make_force_of_infection(infections, susceptible)

    variant_prevalence = infections['modeled_infections_variant'] / infections['modeled_infections_total']
    new_s_variant = components_diff[[c for c in tracking_cols if 'NewS_v' in c]].sum(axis=1)
    new_r_wild = components_diff[[c for c in tracking_cols if 'NewR_w' in c]].sum(axis=1)
    proportion_cross_immune = new_r_wild / (new_s_variant + new_r_wild)

    return SystemMetrics(
        **infections,
        modeled_infected_total=infected,
        **group_infections,
        **group_deaths,
        **susceptible,
        **infectious,
        **immune,
        **vaccinations,
        total_population=total_pop,
        **betas,
        **incidence,
        **force_of_infection,
        variant_prevalence=variant_prevalence,
        proportion_cross_immune=proportion_cross_immune,
    )


def _make_infections(components_diff) -> Dict[str, pd.Series]:
    output_column_map = {
        'wild': (ode.TRACKING_COMPARTMENT_NAMES.NewE_wild,),
        'variant': (ode.TRACKING_COMPARTMENT_NAMES.NewE_variant,),
        'natural_breakthrough': (ode.TRACKING_COMPARTMENT_NAMES.NewE_nbt,),
        'vaccine_breakthrough': (ode.TRACKING_COMPARTMENT_NAMES.NewE_vbt,),
        'total': (ode.TRACKING_COMPARTMENT_NAMES.NewE_wild, ode.TRACKING_COMPARTMENT_NAMES.NewE_variant),

        'unvaccinated_wild': (ode.TRACKING_COMPARTMENT_NAMES.NewE_unvax_wild,),
        'unvaccinated_variant': (ode.TRACKING_COMPARTMENT_NAMES.NewE_unvax_variant,),
        'unvaccinated_natural_breakthrough': (ode.TRACKING_COMPARTMENT_NAMES.NewE_unvax_nbt,),
        'unvaccinated_total': (ode.TRACKING_COMPARTMENT_NAMES.NewE_unvax_wild,
                               ode.TRACKING_COMPARTMENT_NAMES.NewE_unvax_variant),
    }
    return _make_outputs(components_diff, 'modeled_infections', output_column_map)


def _make_susceptible(components) -> Dict[str, pd.Series]:
    output_column_map = {
        'wild': ode.SUSCEPTIBLE_WILD_NAMES,
        'variant': ode.SUSCEPTIBLE_WILD_NAMES + ode.SUSCEPTIBLE_VARIANT_ONLY_NAMES,
        'variant_only': ode.SUSCEPTIBLE_VARIANT_ONLY_NAMES,
        'variant_unprotected': ode.SUSCEPTIBLE_VARIANT_UNPROTECTED_NAMES,

        'unvaccinated_wild': (ode.COMPARTMENT_NAMES.S,),
        'unvaccinated_variant': (ode.COMPARTMENT_NAMES.S, ode.COMPARTMENT_NAMES.S_variant),
        'unvaccinated_variant_only': (ode.COMPARTMENT_NAMES.S_variant,),
    }
    return _make_outputs(components, 'total_susceptible', output_column_map)


def _make_infectious(components) -> Dict[str, pd.Series]:
    output_column_map = {
        'wild': ode.INFECTIOUS_WILD_NAMES,
        'variant': ode.INFECTIOUS_VARIANT_NAMES,
        '': ode.INFECTIOUS_WILD_NAMES + ode.INFECTIOUS_VARIANT_NAMES,
    }
    return _make_outputs(components, 'total_infectious', output_column_map)


def _make_immune(components) -> Dict[str, pd.Series]:
    output_column_map = {
        'wild': ode.IMMUNE_WILD_NAMES,
        'variant': ode.IMMUNE_VARIANT_NAMES,
    }
    return _make_outputs(components, 'total_immune', output_column_map)


def _make_vaccinations(components, components_diff) -> Dict[str, pd.Series]:
    output_column_map = {
        'ineffective': (ode.TRACKING_COMPARTMENT_NAMES.V_u,),
        'protected_wild': (ode.TRACKING_COMPARTMENT_NAMES.V_p,),
        'protected_all': (ode.TRACKING_COMPARTMENT_NAMES.V_pa,),
        'immune_wild': (ode.TRACKING_COMPARTMENT_NAMES.V_m,),
        'immune_all': (ode.TRACKING_COMPARTMENT_NAMES.V_ma,),
        'effective': (ode.TRACKING_COMPARTMENT_NAMES.V_p, ode.TRACKING_COMPARTMENT_NAMES.V_pa,
                      ode.TRACKING_COMPARTMENT_NAMES.V_m, ode.TRACKING_COMPARTMENT_NAMES.V_ma),
    }
    vaccinations = _make_outputs(components_diff, 'vaccinations', output_column_map)
    vaccinations.update(
        _make_outputs(components, 'vaccinations', {'n_unvaccinated': ode.UNVACCINATED_NAMES})
    )
    return vaccinations


def _make_betas(infections, susceptible, infectious, alpha, total_pop):
    def _compute_beta(new_e, s, i):
        return new_e / (s * i**alpha / total_pop)
    betas = {}
    for beta_type in ['wild', 'variant', 'total']:
        suffix = '' if beta_type == 'total' else f'_{beta_type}'
        s_type = 'variant' if beta_type == 'total' else beta_type
        betas[f'beta{suffix}'] = _compute_beta(
            infections[f'modeled_infections_{beta_type}'],
            susceptible[f'total_susceptible_{s_type}'],
            infectious[f'total_infectious{suffix}'],
        )
    return betas


def _make_incidence(infections: Dict[str, pd.Series], total_pop: pd.Series) -> Dict[str, pd.Series]:
    return {
        f'incidence_{i_type}': infections[f'modeled_infections_{i_type}'] / total_pop
        for i_type in ['wild', 'variant', 'total', 'unvaccinated_wild', 'unvaccinated_variant', 'unvaccinated_total']
    }


def _make_force_of_infection(infections: Dict[str, pd.Series],
                             susceptible: Dict[str, pd.Series]) -> Dict[str, pd.Series]:
    foi = infections['modeled_infections_total'] / susceptible['total_susceptible_variant']
    foi_novax = (
        infections['modeled_infections_unvaccinated_total']
        / susceptible['total_susceptible_unvaccinated_variant']
    )
    foi_novax_naive = (
        (infections['modeled_infections_unvaccinated_total']
         - infections['modeled_infections_unvaccinated_natural_breakthrough'])
        / susceptible['total_susceptible_unvaccinated_wild'])

    foi_novax_breakthrough = (
        infections['modeled_infections_unvaccinated_natural_breakthrough']
        / susceptible['total_susceptible_unvaccinated_variant_only']
    )
    return {
        'force_of_infection': foi,
        'force_of_infection_unvaccinated': foi_novax,
        'force_of_infection_unvaccinated_naive': foi_novax_naive,
        'force_of_infection_unvaccinated_natural_breakthrough': foi_novax_breakthrough,
    }


def _make_outputs(data, prefix, column_map):
    out = {}
    for suffix, base_cols in column_map.items():
        cols = [f'{c}_{g}' for g, c in itertools.product(['lr', 'hr'], base_cols)]
        key = f'{prefix}_{suffix}' if suffix else prefix
        out[key] = data[cols].sum(axis=1)
    return out


def _make_group_infections_and_deaths_metrics(
        components: pd.DataFrame,
        components_diff: pd.DataFrame,
        postprocessing_params: PostprocessingParameters
) -> Tuple[Dict[str, pd.Series], Dict[str, pd.Series]]:
    group_infections = {}
    group_deaths = {}
    for group in ['hr', 'lr']:
        group_compartments = [c for c in components.columns if group in c]
        group_components_diff = components_diff.loc[:, group_compartments]
        group_ifr = getattr(postprocessing_params, f'ifr_{group}').rename('ifr')

        for covid_type in ['wild', 'variant']:
            infections = group_components_diff[f'NewE_{covid_type}_{group}'].rename('infections')
            infections_p = group_components_diff[f'NewE_p_{covid_type}_{group}'].rename('infections')
            infections_not_p = infections - infections_p
            deaths = _compute_deaths(
                infections_not_p,
                postprocessing_params.infection_to_death,
                group_ifr,
            )
            group_infections[(group, covid_type)] = infections
            group_deaths[(group, covid_type)] = deaths
    modeled_infections = dict(
        modeled_infections_lr=group_infections[('lr', 'wild')] + group_infections[('lr', 'variant')],
        modeled_infections_hr=group_infections[('hr', 'wild')] + group_infections[('hr', 'variant')],
    )

    modeled_deaths = dict(
        modeled_deaths_wild=group_deaths[('lr', 'wild')] + group_deaths[('hr', 'wild')],
        modeled_deaths_variant=group_deaths[('lr', 'variant')] + group_deaths[('hr', 'variant')],
        modeled_deaths_lr=group_deaths[('lr', 'wild')] + group_deaths[('lr', 'variant')],
        modeled_deaths_hr=group_deaths[('hr', 'wild')] + group_deaths[('hr', 'variant')],
        modeled_deaths_total=sum(group_deaths.values()),
    )
    return modeled_infections, modeled_deaths


def _compute_deaths(modeled_infections: pd.Series,
                    infection_death_lag: int,
                    ifr: pd.Series) -> pd.Series:
    modeled_deaths = (modeled_infections
                      .groupby('location_id')
                      .shift(infection_death_lag) * ifr)
    modeled_deaths = modeled_deaths.rename('deaths').reset_index()
    modeled_deaths = modeled_deaths.set_index(['location_id', 'date']).deaths
    return modeled_deaths


def compute_corrected_hospital_usage(admissions: pd.Series,
                                     hospital_parameters: 'HospitalParameters',
                                     postprocessing_parameters: PostprocessingParameters) -> HospitalMetrics:
    hfr = postprocessing_parameters.ihr / postprocessing_parameters.ifr
    hfr[hfr < 1] = 1.0
    hospital_usage = compute_hospital_usage(
        admissions,
        hfr,
        hospital_parameters,
    )
    corrected_hospital_census = (hospital_usage.hospital_census
                                 * postprocessing_parameters.hospital_census).fillna(method='ffill')
    corrected_icu_census = (corrected_hospital_census
                            * postprocessing_parameters.icu_census).fillna(method='ffill')

    hospital_usage.hospital_census = corrected_hospital_census
    hospital_usage.icu_census = corrected_icu_census

    return hospital_usage


def compute_effective_r(model_params: ode.ForecastParameters,
                        system_metrics: SystemMetrics) -> Dict[str, pd.Series]:
    sigma, gamma1, gamma2 = model_params.sigma, model_params.gamma1, model_params.gamma2
    average_generation_time = int(round((1 / sigma + 1 / gamma1 + 1 / gamma2).mean()))

    system_metrics = system_metrics.to_dict()
    pop = system_metrics['total_population']
    out = {}
    for label in ['wild', 'variant', 'total']:
        suffix = '' if label == 'total' else f'_{label}'
        s_label = 'variant' if label == 'total' else label
        infections = system_metrics[f'modeled_infections_{label}']
        susceptible = system_metrics[f'total_susceptible_{s_label}']
        out[f'r_effective{suffix}'] = (infections
                                       .groupby('location_id')
                                       .apply(lambda x: x / x.shift(average_generation_time)))
        out[f'r_controlled{suffix}'] = out[f'r_effective{suffix}'] * pop / susceptible

    return out

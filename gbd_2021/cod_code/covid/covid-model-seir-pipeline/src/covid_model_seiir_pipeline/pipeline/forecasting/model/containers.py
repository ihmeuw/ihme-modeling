from dataclasses import dataclass
from typing import Dict, List, Union

import pandas as pd

from covid_model_seiir_pipeline.lib import (
    utilities,
)

# This is just exposing these containers from this namespace so we're not
# importing from the regression stage everywhere.
from covid_model_seiir_pipeline.pipeline.regression.model.containers import (
    RatioData,
    HospitalCensusData,
    HospitalMetrics,
    HospitalCorrectionFactors,
)


class Indices:
    """Abstraction for building square datasets."""

    def __init__(self,
                 past_start_dates: pd.Series,
                 forecast_start_dates: pd.Series,
                 forecast_end_dates: pd.Series):
        self._past_index = self._build_index(past_start_dates, forecast_start_dates, pd.Timedelta(days=1))
        self._future_index = self._build_index(forecast_start_dates, forecast_end_dates)
        self._initial_condition_index = (
            forecast_start_dates
            .reset_index()
            .set_index(['location_id', 'date'])
            .sort_index()
            .index
        )
        self._full_index = self._build_index(past_start_dates, forecast_end_dates)

    @property
    def past(self) -> pd.MultiIndex:
        """Location-date index for the past."""
        return self._past_index.copy()

    @property
    def future(self) -> pd.MultiIndex:
        """Location-date index for the future."""
        return self._future_index.copy()

    @property
    def initial_condition(self) -> pd.MultiIndex:
        """Location-date index for the initial condition.

        This index has one date per location.
        """
        return self._initial_condition_index.copy()

    @property
    def full(self) -> pd.MultiIndex:
        """Location-date index for the full time series, past and future."""
        return self._full_index.copy()

    @staticmethod
    def _build_index(start: pd.Series,
                     end: pd.Series,
                     end_offset: pd.Timedelta = pd.Timedelta(days=0)) -> pd.MultiIndex:
        index = (pd.concat([start.rename('start'), end.rename('end')], axis=1)
                 .groupby('location_id')
                 .apply(lambda x: pd.date_range(x.iloc[0, 0], x.iloc[0, 1] - end_offset))
                 .explode()
                 .rename('date')
                 .reset_index()
                 .set_index(['location_id', 'date'])
                 .sort_index()
                 .index)
        return index


@dataclass
class PostprocessingParameters:
    past_compartments: pd.DataFrame

    past_infections: pd.Series
    past_deaths: pd.Series

    infection_to_death: int
    infection_to_admission: int
    infection_to_case: int

    ifr_scalar: float
    ihr_scalar: float

    ifr: pd.Series
    ifr_hr: pd.Series
    ifr_lr: pd.Series
    ihr: pd.Series
    idr: pd.Series

    hospital_census: pd.Series
    icu_census: pd.Series

    def to_dict(self) -> Dict[str, Union[int, pd.Series, pd.DataFrame]]:
        return utilities.asdict(self)

    @property
    def correction_factors_df(self) -> pd.DataFrame:
        return pd.concat([
            self.hospital_census.rename('hospital_census_correction_factor'),
            self.icu_census.rename('icu_census_correction_factor'),
        ], axis=1)


@dataclass
class SystemMetrics:
    modeled_infections_wild: pd.Series
    modeled_infections_variant: pd.Series
    modeled_infections_natural_breakthrough: pd.Series
    modeled_infections_vaccine_breakthrough: pd.Series
    modeled_infections_total: pd.Series

    modeled_infections_unvaccinated_wild: pd.Series
    modeled_infections_unvaccinated_variant: pd.Series
    modeled_infections_unvaccinated_natural_breakthrough: pd.Series
    modeled_infections_unvaccinated_total: pd.Series

    modeled_infections_lr: pd.Series
    modeled_infections_hr: pd.Series
    modeled_infected_total: pd.Series

    modeled_deaths_wild: pd.Series
    modeled_deaths_variant: pd.Series
    modeled_deaths_lr: pd.Series
    modeled_deaths_hr: pd.Series
    modeled_deaths_total: pd.Series

    total_susceptible_wild: pd.Series
    total_susceptible_variant: pd.Series
    total_susceptible_variant_only: pd.Series
    total_susceptible_variant_unprotected: pd.Series

    total_susceptible_unvaccinated_wild: pd.Series
    total_susceptible_unvaccinated_variant: pd.Series
    total_susceptible_unvaccinated_variant_only: pd.Series

    total_infectious_wild: pd.Series
    total_infectious_variant: pd.Series
    total_infectious: pd.Series

    total_immune_wild: pd.Series
    total_immune_variant: pd.Series

    vaccinations_protected_wild: pd.Series
    vaccinations_protected_all: pd.Series
    vaccinations_immune_wild: pd.Series
    vaccinations_immune_all: pd.Series
    vaccinations_effective: pd.Series
    vaccinations_ineffective: pd.Series
    vaccinations_n_unvaccinated: pd.Series

    total_population: pd.Series

    beta: pd.Series
    beta_wild: pd.Series
    beta_variant: pd.Series

    incidence_wild: pd.Series
    incidence_variant: pd.Series
    incidence_total: pd.Series
    incidence_unvaccinated_wild: pd.Series
    incidence_unvaccinated_variant: pd.Series
    incidence_unvaccinated_total: pd.Series

    force_of_infection: pd.Series
    force_of_infection_unvaccinated: pd.Series
    force_of_infection_unvaccinated_naive: pd.Series
    force_of_infection_unvaccinated_natural_breakthrough: pd.Series

    variant_prevalence: pd.Series
    proportion_cross_immune: pd.Series

    def to_dict(self) -> Dict[str, pd.Series]:
        return utilities.asdict(self)

    def to_df(self) -> pd.DataFrame:
        return pd.concat([v.rename(k) for k, v in self.to_dict().items()], axis=1)


@dataclass
class OutputMetrics:
    # observed + modeled
    infections: pd.Series
    cases: pd.Series
    hospital_admissions: pd.Series
    hospital_census: pd.Series
    icu_admissions: pd.Series
    icu_census: pd.Series
    deaths: pd.Series

    # Other stuff
    r_controlled_wild: pd.Series
    r_effective_wild: pd.Series
    r_controlled_variant: pd.Series
    r_effective_variant: pd.Series
    r_controlled: pd.Series
    r_effective: pd.Series

    def to_dict(self) -> Dict[str, pd.Series]:
        return utilities.asdict(self)

    def to_df(self) -> pd.DataFrame:
        out_list = []
        for k, v in self.to_dict().items():
            v = v.rename(k)
            if k == 'deaths':
                v = v.reset_index(level='observed')
            out_list.append(v)
        return pd.concat(out_list, axis=1)

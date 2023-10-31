from dataclasses import dataclass
from typing import Dict, List, Iterator, Tuple

import pandas as pd

from covid_model_seiir_pipeline.lib import (
    utilities,
)


@dataclass(repr=False, eq=False)
class ODEParameters:
    # Core parameters
    alpha: pd.Series
    sigma: pd.Series
    gamma1: pd.Series
    gamma2: pd.Series

    # Variant prevalences
    rho: pd.Series
    rho_variant: pd.Series
    rho_b1617: pd.Series
    rho_total: pd.Series

    # Escape variant initialization
    pi: pd.Series

    # Cross-variant immunity
    chi: pd.Series

    # Vaccine parameters
    vaccinations_unprotected_lr: pd.Series
    vaccinations_non_escape_protected_lr: pd.Series
    vaccinations_escape_protected_lr: pd.Series
    vaccinations_non_escape_immune_lr: pd.Series
    vaccinations_escape_immune_lr: pd.Series

    vaccinations_unprotected_hr: pd.Series
    vaccinations_non_escape_protected_hr: pd.Series
    vaccinations_escape_protected_hr: pd.Series
    vaccinations_non_escape_immune_hr: pd.Series
    vaccinations_escape_immune_hr: pd.Series

    def to_dict(self) -> Dict[str, pd.Series]:
        return {k: v.rename(k) for k, v in utilities.asdict(self).items()}

    def to_df(self) -> pd.DataFrame:
        return pd.concat(self.to_dict().values(), axis=1)

    def reindex(self, index: pd.Index) -> 'ODEParameters':
        # noinspection PyArgumentList
        return type(self)(
            **{key: value.reindex(index) for key, value in self.to_dict().items()}
        )

    def get_vaccinations(self, vaccine_types: List[str], risk_group: str) -> pd.DataFrame:
        vaccine_type_map = {
            'u': 'vaccinations_unprotected',
            'p': 'vaccinations_non_escape_protected',
            'pa': 'vaccinations_escape_protected',
            'm': 'vaccinations_non_escape_immune',
            'ma': 'vaccinations_escape_immune',
        }
        vaccinations = []
        for vaccine_type in vaccine_types:
            attr = f'{vaccine_type_map[vaccine_type]}_{risk_group}'
            vaccinations.append(getattr(self, attr).rename(attr))
        return pd.concat(vaccinations, axis=1)

    def __iter__(self) -> Iterator[Tuple[int, 'ODEParameters']]:
        location_ids = self.alpha.reset_index().location_id.unique()
        this_dict = self.to_dict().items()
        for location_id in location_ids:
            # noinspection PyArgumentList
            loc_parameters = type(self)(
                **{key: value.loc[location_id] for key, value in this_dict},
            )
            yield location_id, loc_parameters


@dataclass(repr=False, eq=False)
class FitParameters(ODEParameters):
    # Transmission intensity
    new_e: pd.Series = None
    kappa: pd.Series = None
    phi: pd.Series = None
    psi: pd.Series = None

    # Sub-populations
    population_low_risk: pd.Series = None
    population_high_risk: pd.Series = None


@dataclass(repr=False, eq=False)
class ForecastParameters(ODEParameters):
    # Transmission intensity
    beta: pd.Series = None
    beta_wild: pd.Series = None
    beta_variant: pd.Series = None
    beta_hat: pd.Series = None

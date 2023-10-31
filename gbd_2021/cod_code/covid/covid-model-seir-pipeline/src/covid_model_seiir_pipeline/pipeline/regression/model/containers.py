"""Containers for regression data."""
from dataclasses import dataclass
from typing import Dict, List, Iterator, Tuple, Union

import pandas as pd

from covid_model_seiir_pipeline.lib import (
    utilities,
)


@dataclass
class RatioData:
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

    def to_dict(self) -> Dict[str, Union[int, pd.Series]]:
        return utilities.asdict(self)


@dataclass
class HospitalCensusData:
    hospital_census: pd.Series
    icu_census: pd.Series

    def to_dict(self) -> Dict[str, pd.Series]:
        return utilities.asdict(self)

    def to_df(self):
        return pd.concat([v.rename(k) for k, v in self.to_dict().items()], axis=1)


@dataclass
class HospitalMetrics:
    hospital_admissions: pd.Series
    hospital_census: pd.Series
    icu_admissions: pd.Series
    icu_census: pd.Series

    def to_dict(self) -> Dict[str, pd.Series]:
        return utilities.asdict(self)

    def to_df(self):
        return pd.concat([v.rename(k) for k, v in self.to_dict().items()], axis=1)


@dataclass
class HospitalCorrectionFactors:
    hospital_census: pd.Series
    icu_census: pd.Series

    def to_dict(self) -> Dict[str, pd.Series]:
        return utilities.asdict(self)

    def to_df(self):
        return pd.concat([v.rename(k) for k, v in self.to_dict().items()], axis=1)

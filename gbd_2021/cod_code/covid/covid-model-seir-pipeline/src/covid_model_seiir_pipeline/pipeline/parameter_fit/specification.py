from dataclasses import dataclass, field
from typing import Dict, List, NamedTuple, Optional, Tuple

from covid_shared import workflow

from covid_model_seiir_pipeline.lib import (
    utilities,
)


class __FitJobs(NamedTuple):
    fit: str = 'beta_fit'


FIT_JOBS = __FitJobs()


class FitTaskSpecification(workflow.TaskSpecification):
    """Specification of execution parameters for fit OOS tasks."""
    default_max_runtime_seconds = 3000
    default_m_mem_free = '25G'
    default_num_cores = 1


class FitWorkflowSpecification(workflow.WorkflowSpecification):
    """Specification of execution parameters for fit workflows."""
    tasks = {
        FIT_JOBS.fit: FitTaskSpecification,
    }


@dataclass
class FitData:
    """Specifies the inputs and outputs for the fit"""
    infection_version: str = field(default='best')
    covariate_version: str = field(default='best')
    coefficient_version: str = field(default='best')
    variant_version: str = field(default='best')
    location_set_version_id: int = field(default=0)
    location_set_file: str = field(default='')
    output_root: str = field(default='')
    output_format: str = field(default='csv')
    n_draws: int = field(default=1000)

    def to_dict(self) -> Dict:
        """Converts to a dict, coercing list-like items to lists."""
        return utilities.asdict(self)


@dataclass
class FitScenario:
    """Specifies the parameters of the beta fit."""
    name: str = field(default='')

    alpha: Tuple[float, float] = field(default=(0.9, 1.0))
    sigma: Tuple[float, float] = field(default=(0.2, 1/3))
    gamma1: Tuple[float, float] = field(default=(0.5, 0.5))
    gamma2: Tuple[float, float] = field(default=(1/3, 1.0))

    kappa: float = field(default=0.4)
    chi: float = field(default=0.6)
    phi: float = field(default=0.0)
    psi: float = field(default=0.6)
    pi: Optional[float] = field(default=0.05)

    max_date: str = field(default='2021-06-15')
    threshold: float = field(default=0.5)
    location_filter: list = field(default_factory=list)

    def to_dict(self) -> Dict:
        """Converts to a dict, coercing list-like items to lists."""
        return {k: v for k, v in utilities.asdict(self).items() if k not in ['name', 'system']}


class FitSpecification(utilities.Specification):
    """Specification for a fit."""

    def __init__(self,
                 data: FitData,
                 workflow: FitWorkflowSpecification,
                 scenarios: List[FitScenario]):
        self._data = data
        self._workflow = workflow
        self._scenarios = {s.name: s for s in scenarios}

    @classmethod
    def parse_spec_dict(cls, fit_spec_dict: Dict) -> Tuple:
        """Constructs a regression specification from a dictionary."""
        sub_specs = {
            'data': FitData,
            'workflow': FitWorkflowSpecification,
        }
        sub_specs = {key: spec_class(**fit_spec_dict.get(key, {}))
                     for key, spec_class in sub_specs.items()}

        fit_dicts = fit_spec_dict.get('scenarios', {})
        fit_specs = []
        for name, fit_spec in fit_dicts.items():
            fit_specs.append(FitScenario(name, **fit_spec))
        if not fit_specs:
            raise ValueError('No fit specified')

        sub_specs['scenarios'] = fit_specs

        return tuple(sub_specs.values())

    @property
    def data(self) -> FitData:
        """The data specification for the regression."""
        return self._data

    @property
    def workflow(self) -> FitWorkflowSpecification:
        """The workflow specification for the regression."""
        return self._workflow

    @property
    def scenarios(self) -> Dict[str, FitScenario]:
        """The covariates for the regression."""
        return self._scenarios

    def to_dict(self) -> Dict:
        """Converts the specification to a dict."""
        spec = {
            'data': self.data.to_dict(),
            'workflow': self.workflow.to_dict(),
            'scenarios': {k: v.to_dict() for k, v in self.scenarios.items()},
        }
        return spec

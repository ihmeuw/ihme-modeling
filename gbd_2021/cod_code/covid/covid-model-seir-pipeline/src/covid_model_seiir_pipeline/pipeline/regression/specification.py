from dataclasses import dataclass, field
from typing import Dict, List, NamedTuple, Tuple

from covid_shared import workflow

from covid_model_seiir_pipeline.lib import (
    utilities,
)


class __RegressionJobs(NamedTuple):
    regression: str = 'beta_regression'
    hospital_correction_factors: str = 'hospital_correction_factors'


REGRESSION_JOBS = __RegressionJobs()


class RegressionTaskSpecification(workflow.TaskSpecification):
    """Specification of execution parameters for regression tasks."""
    default_max_runtime_seconds = 6000
    default_m_mem_free = '18G'
    default_num_cores = 1


class HospitalCorrectionFactorTaskSpecification(workflow.TaskSpecification):
    """Specification of execution parameters for regression tasks."""
    default_max_runtime_seconds = 6000
    default_m_mem_free = '100G'
    default_num_cores = 26


class RegressionWorkflowSpecification(workflow.WorkflowSpecification):
    """Specification of execution parameters for regression workflows."""
    tasks = {
        REGRESSION_JOBS.regression: RegressionTaskSpecification,
        REGRESSION_JOBS.hospital_correction_factors: HospitalCorrectionFactorTaskSpecification,
    }


@dataclass
class RegressionData:
    """Specifies the inputs and outputs for a regression"""
    covariate_version: str = field(default='best')
    infection_version: str = field(default='best')
    coefficient_version: str = field(default='')
    priors_version: str = field(default='')
    location_set_version_id: int = field(default=0)
    location_set_file: str = field(default='')
    output_root: str = field(default='')
    output_format: str = field(default='csv')
    n_draws: int = field(default=100)
    run_counties: bool = field(init=False)
    drop_locations: list = field(default_factory=list)

    def __post_init__(self):
        self.run_counties = self.location_set_version_id in [841, 920]

    def to_dict(self) -> Dict:
        """Converts to a dict, coercing list-like items to lists."""
        return {k: v for k, v in utilities.asdict(self).items() if k != 'run_counties'}


@dataclass
class RegressionParameters:
    """Specifies the parameters of the beta fit and regression."""
    alpha: Tuple[float, float] = field(default=(0.9, 1.0))
    sigma: Tuple[float, float] = field(default=(0.2, 1/3))
    gamma1: Tuple[float, float] = field(default=(0.5, 0.5))
    gamma2: Tuple[float, float] = field(default=(1/3, 1.0))
    kappa: Tuple[float, float] = field(default=(0.3, 0.5))
    chi: Tuple[float, float] = field(default=(0.0, 0.6))
    phi_mean_shift: float = field(default=0.5)
    phi_sd: float = field(default=0.3)
    psi_mean_shift: float = field(default=0.9)
    psi_sd: float = field(default=0.3)
    pi: float = field(default=0.1)

    def to_dict(self) -> Dict:
        """Converts to a dict, coercing list-like items to lists."""
        return utilities.asdict(self)


@dataclass
class HospitalParameters:
    """Parameters for the hospital model calculation."""
    compute_correction_factors: bool = field(default=True)
    hospital_stay_death: int = field(default=6)
    hospital_stay_recover: int = field(default=14)
    hospital_stay_recover_icu: int = field(default=20)
    hospital_to_icu: int = field(default=3)
    icu_stay_recover: int = field(default=13)
    icu_ratio: float = field(default=0.25)
    correction_factor_smooth_window: int = field(default=14)
    hospital_correction_factor_min: float = field(default=0.5)
    hospital_correction_factor_max: float = field(default=25.0)
    icu_correction_factor_min: float = field(default=0.05)
    icu_correction_factor_max: float = field(default=0.95)
    correction_factor_average_window: int = field(default=42)
    correction_factor_application_window: int = field(default=42)

    def to_dict(self) -> Dict:
        """Converts to a dict, coercing list-like items to lists."""
        return utilities.asdict(self)


@dataclass
class CovariateSpecification:
    """Regression specification for a covariate."""

    # model params
    name: str = field(default='covariate')
    group_level: str = field(default='')
    gprior: Tuple[float, float] = field(default=(0., 1000.))
    bounds: Tuple[float, float] = field(default=(-1000., 1000.))

    def to_dict(self) -> Dict:
        """Converts to a dict, coercing list-like items to lists.

        Drops the name parameter as it's used as a key in the specification.

        """
        return {k: v for k, v in utilities.asdict(self).items() if k != 'name'}


class RegressionSpecification(utilities.Specification):
    """Specification for a regression run."""

    def __init__(self,
                 data: RegressionData,
                 workflow: RegressionWorkflowSpecification,
                 regression_parameters: RegressionParameters,
                 hospital_parameters: HospitalParameters,
                 covariates: List[CovariateSpecification]):
        self._data = data
        self._workflow = workflow
        self._regression_parameters = regression_parameters
        self._hospital_parameters = hospital_parameters
        self._covariates = {c.name: c for c in covariates}

    @classmethod
    def parse_spec_dict(cls, regression_spec_dict: Dict) -> Tuple:
        """Constructs a regression specification from a dictionary."""
        sub_specs = {
            'data': RegressionData,
            'workflow': RegressionWorkflowSpecification,
            'regression_parameters': RegressionParameters,
            'hospital_parameters': HospitalParameters,
        }
        for key, spec_class in list(sub_specs.items()):  # We're dynamically altering. Copy with list
            spec_dict = utilities.filter_to_spec_fields(
                regression_spec_dict.get(key, {}),
                spec_class(),
            )
            sub_specs[key] = spec_class(**spec_dict)

        if sub_specs['data'].run_counties:
            sub_specs['hospital_parameters'].compute_correction_factors = False

        # covariates
        cov_dicts = regression_spec_dict.get('covariates', {})
        covariates = []
        for name, cov_spec in cov_dicts.items():
            cov_spec = utilities.filter_to_spec_fields(cov_spec, CovariateSpecification())
            covariates.append(CovariateSpecification(name, **cov_spec))
        if not covariates:  # Assume we're generating for a template
            covariates.append(CovariateSpecification())
        sub_specs['covariates'] = covariates

        return tuple(sub_specs.values())

    @property
    def data(self) -> RegressionData:
        """The data specification for the regression."""
        return self._data

    @property
    def workflow(self) -> RegressionWorkflowSpecification:
        """The workflow specification for the regression."""
        return self._workflow

    @property
    def regression_parameters(self) -> RegressionParameters:
        """The parametrization of the regression."""
        return self._regression_parameters

    @property
    def hospital_parameters(self) -> HospitalParameters:
        """The parameterization of the hospital algorithm"""
        return self._hospital_parameters

    @property
    def covariates(self) -> Dict[str, CovariateSpecification]:
        """The covariates for the regression."""
        return self._covariates

    def to_dict(self) -> Dict:
        """Converts the specification to a dict."""
        spec = {
            'data': self.data.to_dict(),
            'workflow': self.workflow.to_dict(),
            'regression_parameters': self.regression_parameters.to_dict(),
            'hospital_parameters': self.hospital_parameters.to_dict(),
            'covariates': {k: v.to_dict() for k, v in self._covariates.items()},
        }
        return spec

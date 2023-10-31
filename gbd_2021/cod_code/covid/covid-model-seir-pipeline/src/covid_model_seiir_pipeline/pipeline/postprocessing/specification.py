from dataclasses import dataclass, field
from typing import Dict, List, NamedTuple, Tuple

from covid_shared import workflow

from covid_model_seiir_pipeline.lib import (
    utilities,
)


class __PostprocessingJobs(NamedTuple):
    resample: str = 'resample_map'
    postprocess: str = 'postprocess'


POSTPROCESSING_JOBS = __PostprocessingJobs()


class ResampleTaskSpecification(workflow.TaskSpecification):
    """Specification of execution parameters for draw resample mapping tasks."""
    default_max_runtime_seconds = 10000
    default_m_mem_free = '50G'
    default_num_cores = 26


class PostprocessingTaskSpecification(workflow.TaskSpecification):
    """Specification of execution parameters for postprocessing tasks."""
    default_max_runtime_seconds = 15000
    default_m_mem_free = '100G'
    default_num_cores = 10


class PostprocessingWorkflowSpecification(workflow.WorkflowSpecification):
    """Specification of execution parameters for forecasting workflows."""

    tasks = {
        POSTPROCESSING_JOBS.resample: ResampleTaskSpecification,
        POSTPROCESSING_JOBS.postprocess: PostprocessingTaskSpecification,
    }


@dataclass
class PostprocessingData:
    """Specifies the inputs and outputs for postprocessing."""
    forecast_version: str = field(default='best')
    mortality_ratio_version: str = field(default='best')
    scenarios: list = field(default_factory=lambda: ['worse', 'reference', 'best_masks'])
    output_root: str = field(default='')

    def to_dict(self) -> Dict:
        """Converts to a dict, coercing list-like items to lists."""
        return utilities.asdict(self)


@dataclass
class ResamplingSpecification:

    reference_scenario: str = field(default='worse')
    reference_date: str = field(default='2020-12-31')
    lower_quantile: float = field(default=0.025)
    upper_quantile: float = field(default=0.975)

    def to_dict(self) -> Dict:
        return utilities.asdict(self)


@dataclass
class SplicingSpecification:
    """Specifies locations and inputs for splicing."""
    locations: list = field(default_factory=list)
    output_version: str = field(default='')

    def to_dict(self) -> Dict:
        """Converts to a dict, coercing list-like items to lists."""
        return utilities.asdict(self)


@dataclass
class AggregationSpecification:
    """Specifies hierarchy and parameters for aggregation."""
    location_file: str = field(default='')
    location_set_id: None = None
    location_set_version_id: int = field(default=None)

    def to_dict(self) -> Dict:
        """Converts to a dict, coercing list-like items to lists."""
        return utilities.asdict(self)


class PostprocessingSpecification(utilities.Specification):

    def __init__(self,
                 data: PostprocessingData,
                 workflow: PostprocessingWorkflowSpecification,
                 resampling: ResamplingSpecification,
                 splicing: List[SplicingSpecification],
                 aggregation: List[AggregationSpecification]):
        self._data = data
        self._workflow = workflow
        self._resampling = resampling
        self._splicing = splicing
        self._aggregation = aggregation

    @classmethod
    def parse_spec_dict(cls, postprocessing_spec_dict: Dict) -> Tuple:
        """Construct postprocessing specification args from a dict."""
        data = PostprocessingData(**postprocessing_spec_dict.get('data', {}))
        workflow = PostprocessingWorkflowSpecification(**postprocessing_spec_dict.get('workflow', {}))
        resampling = ResamplingSpecification(**postprocessing_spec_dict.get('resampling', {}))
        splicing_configs = postprocessing_spec_dict.get('splicing', [])
        splicing = [SplicingSpecification(**splicing_config) for splicing_config in splicing_configs]
        aggregation_configs = postprocessing_spec_dict.get('aggregation', [])
        aggregation = [AggregationSpecification(**aggregation_config) for aggregation_config in aggregation_configs]
        return data, workflow, resampling, splicing, aggregation

    @property
    def data(self) -> PostprocessingData:
        """The postprocessing data specification."""
        return self._data

    @property
    def workflow(self) -> PostprocessingWorkflowSpecification:
        return self._workflow

    @property
    def resampling(self) -> ResamplingSpecification:
        return self._resampling

    @property
    def splicing(self) -> List[SplicingSpecification]:
        return self._splicing

    @property
    def aggregation(self) -> List[AggregationSpecification]:
        return self._aggregation

    def to_dict(self):
        """Convert the specification to a dict."""
        return {
            'data': self.data.to_dict(),
            'workflow': self.workflow.to_dict(),
            'resampling': self.resampling.to_dict(),
            'splicing': [splicing_config.to_dict() for splicing_config in self.splicing],
            'aggregation': [aggregation_config.to_dict() for aggregation_config in self.aggregation],
        }

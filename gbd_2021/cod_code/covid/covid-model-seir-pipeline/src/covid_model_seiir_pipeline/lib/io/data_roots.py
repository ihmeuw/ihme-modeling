"""Concrete representations of on disk data sources."""
import itertools
from pathlib import Path
from typing import List, Union

from covid_model_seiir_pipeline.lib.io.keys import (
    DatasetType,
    MetadataType,
    DatasetKey,
    LEAF_TEMPLATES,
    PREFIX_TEMPLATES,
)
from covid_model_seiir_pipeline.lib.io.marshall import (
    DATA_STRATEGIES,
    METADATA_STRATEGIES,
)


class DataRoot:
    """Representation of a version of data from a particular source or sink.

    This class provide convenience methods for the I/O tooling to inspect
    the structure of a data source or sink as defined by its subclasses.

    Subclasses are responsible for declaring their structure by assigning
    :class:`DatasetType` and :class:`MetadataType` class variables. Instances
    of subclasses then serve as factories for generating keys pointing
    to particular data sets which can be used to transfer data and metadata
    to and from disk.

    Parameters
    ----------
    root
        An existing directory on disk where data will be read from or written
        to.
    data_format
        The on disk format for data sets in the data root.
    metadata_format
        The on disk format for metadata in the data root.

    """

    def __init__(self, root: Union[str, Path], data_format: str = 'csv', metadata_format: str = 'yaml'):
        self._root = Path(root)
        if data_format not in DATA_STRATEGIES:
            raise ValueError(f'Invalid data format {data_format} for {type(self).__name__}. '
                             f'Valid data formats are {list(DATA_STRATEGIES)}.')
        self._data_format = data_format
        if metadata_format not in METADATA_STRATEGIES:
            raise ValueError(f'Invalid metadata format {metadata_format} for {type(self).__name__}. '
                             f'Valid data formats are {list(METADATA_STRATEGIES)}.')
        self._metadata_format = metadata_format

    @property
    def dataset_types(self) -> List[str]:
        """A list of all named dataset types in the data root."""
        return [dataset_name for dataset_name, attr in type(self).__dict__.items()
                if isinstance(attr, DatasetType)]

    @property
    def metadata_types(self) -> List[str]:
        """A list of all named metadata types in the data root."""
        return [metadata_name for metadata_name, attr in type(self).__dict__.items()
                if isinstance(attr, MetadataType)]

    def terminal_paths(self, **prefix_args: List[str]) -> List[Path]:
        """Resolves and returns all terminal directory and container paths.

        Parameters
        ----------
        prefix_args
            Lists of concrete prefix fill values provided as keyword arguments
            where the keyword is a format value in a prefix template.

        Returns
        -------
            A list of terminal paths without extensions to be interpreted
            by an I/O strategy.

        """
        paths = []
        dataset_types = [dataset_type for dataset_type in type(self).__dict__.values()
                         if isinstance(dataset_type, DatasetType)]
        for dataset_type in dataset_types:
            if dataset_type.prefix_template is not None:
                for arg_set in itertools.product(*prefix_args.values()):
                    prefix_template_kwargs = dict(zip(prefix_args.keys(), arg_set))
                    prefix = dataset_type.prefix_template.format(**prefix_template_kwargs)
                    if dataset_type.leaf_template is not None:
                        path = self._root / prefix / dataset_type.name
                    else:
                        path = self._root / prefix
                    paths.append(path)
            else:
                if dataset_type.leaf_template is not None:
                    path = self._root / dataset_type.name
                else:
                    path = self._root
                paths.append(path)
        paths = list(set(paths))  # Drop duplicates
        return paths


class InfectionRoot(DataRoot):
    """Data root representing infectionator outputs."""
    metadata = MetadataType('metadata')

    em_scalars = DatasetType('em_data')
    infections = DatasetType('infections_draws', LEAF_TEMPLATES.DRAW_TEMPLATE)
    ifr = DatasetType('ifr_draws', LEAF_TEMPLATES.DRAW_TEMPLATE)
    ihr = DatasetType('ihr_draws', LEAF_TEMPLATES.DRAW_TEMPLATE)
    idr = DatasetType('idr_draws', LEAF_TEMPLATES.DRAW_TEMPLATE)


class VariantRoot(DataRoot):
    """Data root representing variant model outputs."""
    metadata = MetadataType('metadata')

    prevalence = DatasetType(LEAF_TEMPLATES.VARIANT_SCENARIO)
    original_data = DatasetType('original_data', LEAF_TEMPLATES.MEASURE_TEMPLATE)


class MortalityRatioRoot(DataRoot):
    """Data root representing age pattern of mortality."""
    metadata = MetadataType('metadata')

    mortality_ratio = DatasetType('mortality_ratio_5yr')


class CovariatePriorsRoot(DataRoot):
    metadata = MetadataType('metadata')

    priors = DatasetType('priors')


class CovariateRoot(DataRoot):
    """Data root representing prepped covariates."""
    metadata = MetadataType('metadata')

    air_pollution_pm_2_5 = DatasetType('air_pollution_pm_2_5', LEAF_TEMPLATES.COV_SCENARIO_TEMPLATE)
    lri_mortality = DatasetType('lri_mortality', LEAF_TEMPLATES.COV_SCENARIO_TEMPLATE)
    mask_use = DatasetType('mask_use', LEAF_TEMPLATES.COV_SCENARIO_TEMPLATE)
    mobility = DatasetType('mobility', LEAF_TEMPLATES.COV_SCENARIO_TEMPLATE)
    pneumonia = DatasetType('pneumonia', LEAF_TEMPLATES.COV_SCENARIO_TEMPLATE)
    proportion_over_2_5k = DatasetType('proportion_over_2_5k', LEAF_TEMPLATES.COV_SCENARIO_TEMPLATE)
    proportion_under_100m = DatasetType('proportion_under_100m', LEAF_TEMPLATES.COV_SCENARIO_TEMPLATE)
    smoking_prevalence = DatasetType('smoking_prevalence', LEAF_TEMPLATES.COV_SCENARIO_TEMPLATE)
    testing = DatasetType('testing', LEAF_TEMPLATES.COV_SCENARIO_TEMPLATE)

    mobility_info = DatasetType('mobility', LEAF_TEMPLATES.COV_INFO_TEMPLATE)
    vaccine_info = DatasetType('vaccine_coverage', LEAF_TEMPLATES.COV_INFO_TEMPLATE)
    variant_info = DatasetType('variant_prevalence', LEAF_TEMPLATES.COV_INFO_TEMPLATE)

    # Getters provide dynamic keys to support experimentation with custom covariates.
    def __getattr__(self, item: str) -> DatasetType:
        setattr(type(self), item, DatasetType(item, LEAF_TEMPLATES.COV_SCENARIO_TEMPLATE))
        return getattr(self, item)

    def __getitem__(self, item: str) -> DatasetType:
        return getattr(self, item)

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__ = state


class FitRoot(DataRoot):
    metadata = MetadataType('metadata')
    specification = MetadataType('fit_specification')
    locations = MetadataType('locations')

    hierarchy = DatasetType('hierarchy')

    beta = DatasetType('beta', LEAF_TEMPLATES.DRAW_TEMPLATE, PREFIX_TEMPLATES.SCENARIO_TEMPLATE)
    compartments = DatasetType('compartments', LEAF_TEMPLATES.DRAW_TEMPLATE, PREFIX_TEMPLATES.SCENARIO_TEMPLATE)
    ode_parameters = DatasetType('ode_parameters', LEAF_TEMPLATES.DRAW_TEMPLATE, PREFIX_TEMPLATES.SCENARIO_TEMPLATE)
    regression_parameters = DatasetType('regression_parameters', LEAF_TEMPLATES.DRAW_TEMPLATE, PREFIX_TEMPLATES.SCENARIO_TEMPLATE)
    coefficients = DatasetType('coefficients', LEAF_TEMPLATES.DRAW_TEMPLATE, PREFIX_TEMPLATES.SCENARIO_TEMPLATE)


class RegressionRoot(DataRoot):
    """Data root representing regression stage outputs."""
    metadata = MetadataType('metadata')
    specification = MetadataType('regression_specification')
    locations = MetadataType('locations')

    hierarchy = DatasetType('hierarchy')

    beta = DatasetType('beta', LEAF_TEMPLATES.DRAW_TEMPLATE)
    coefficients = DatasetType('coefficients', LEAF_TEMPLATES.DRAW_TEMPLATE)
    compartments = DatasetType('compartments', LEAF_TEMPLATES.DRAW_TEMPLATE)
    ode_parameters = DatasetType('ode_parameters', LEAF_TEMPLATES.DRAW_TEMPLATE)

    infections = DatasetType('infections', LEAF_TEMPLATES.DRAW_TEMPLATE)
    deaths = DatasetType('deaths', LEAF_TEMPLATES.DRAW_TEMPLATE)
    hospitalizations = DatasetType('hospitalizations', LEAF_TEMPLATES.MEASURE_TEMPLATE)


class ForecastRoot(DataRoot):
    """Data root representing forecast stage outputs."""
    metadata = MetadataType('metadata')
    specification = MetadataType('forecast_specification')

    beta_scaling = DatasetType('beta_scaling', LEAF_TEMPLATES.DRAW_TEMPLATE, PREFIX_TEMPLATES.SCENARIO_TEMPLATE)
    beta_residual = DatasetType('beta_residual', LEAF_TEMPLATES.DRAW_TEMPLATE, PREFIX_TEMPLATES.SCENARIO_TEMPLATE)
    ode_params = DatasetType('ode_params', LEAF_TEMPLATES.DRAW_TEMPLATE, PREFIX_TEMPLATES.SCENARIO_TEMPLATE)
    component_draws = DatasetType('component_draws', LEAF_TEMPLATES.DRAW_TEMPLATE, PREFIX_TEMPLATES.SCENARIO_TEMPLATE)
    raw_covariates = DatasetType('raw_covariates', LEAF_TEMPLATES.DRAW_TEMPLATE, PREFIX_TEMPLATES.SCENARIO_TEMPLATE)
    raw_outputs = DatasetType('raw_outputs', LEAF_TEMPLATES.DRAW_TEMPLATE, PREFIX_TEMPLATES.SCENARIO_TEMPLATE)


class PostprocessingRoot(DataRoot):
    """Data root representing postprocessing stage outputs."""
    metadata = MetadataType('metadata')
    specification = MetadataType('postprocessing_specification')
    resampling_map = MetadataType('resampling_map')

    output_draws = DatasetType('output_draws',
                               LEAF_TEMPLATES.MEASURE_TEMPLATE,
                               PREFIX_TEMPLATES.SCENARIO_TEMPLATE)
    output_summaries = DatasetType('output_summaries',
                                   LEAF_TEMPLATES.MEASURE_TEMPLATE,
                                   PREFIX_TEMPLATES.SCENARIO_TEMPLATE)
    output_miscellaneous = DatasetType('output_miscellaneous',
                                       LEAF_TEMPLATES.MEASURE_TEMPLATE,
                                       PREFIX_TEMPLATES.SCENARIO_TEMPLATE)


class DiagnosticsRoot(DataRoot):
    """Data root representing postprocessing stage outputs."""
    metadata = MetadataType('metadata')
    specification = MetadataType('diagnostics_specification')

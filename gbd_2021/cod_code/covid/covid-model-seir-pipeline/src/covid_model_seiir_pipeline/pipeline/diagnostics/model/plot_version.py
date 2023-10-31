from pathlib import Path
from typing import List, NamedTuple, Optional, Tuple

from loguru import logger
import pandas as pd

from covid_model_seiir_pipeline.lib import static_vars
from covid_model_seiir_pipeline.pipeline.postprocessing import (
    PostprocessingSpecification,
    PostprocessingDataInterface,
)
from covid_model_seiir_pipeline.pipeline.postprocessing.model import (
    MEASURES,
    COVARIATES,
    MISCELLANEOUS,
)
from covid_model_seiir_pipeline.pipeline.diagnostics.specification import (
    ComparatorSpecification,
)


Color = Tuple[float, float, float, float]


class Location(NamedTuple):
    id: int
    name: str


class PlotVersion:

    def __init__(self,
                 version: Path,
                 scenario: str,
                 label: str,
                 color: Color):
        self.version = version
        self.scenario = scenario
        self.label = label
        self.color = color
        spec = PostprocessingSpecification.from_path(
            self.version / static_vars.POSTPROCESSING_SPECIFICATION_FILE
        )
        self.pdi = PostprocessingDataInterface.from_specification(spec)
        self._cache = None

    def load_output_summaries(self, measure: str, location_id: int = None):
        try:
            data = self.load_from_cache('summaries', measure)
        except FileNotFoundError:
            data = self.pdi.load_output_summaries(self.scenario, measure)
        return self.clean_data(data, location_id)

    def load_output_draws(self, measure: str, location_id: int = None):
        try:
            data = self.load_from_cache('draws', measure)
        except FileNotFoundError:
            data = self.pdi.load_output_draws(self.scenario, measure)
        return self.clean_data(data, location_id)

    def load_output_miscellaneous(self, measure: str, is_table: bool, location_id: int = None):
        try:
            data = self.load_from_cache('miscellaneous', measure)
        except FileNotFoundError:
            data = self.pdi.load_output_miscellaneous(self.scenario, measure, is_table)
        return self.clean_data(data, location_id)

    # FIXME: bad separation of concerns.
    def build_cache(self, cache_dir: Path, cache_draws: List[str]):
        """Build a cache of all data needed for the plots."""
        self._cache = cache_dir / self.version.name / self.scenario
        self._cache.mkdir(parents=True)
        (self._cache / 'summaries').mkdir()
        (self._cache / 'draws').mkdir()
        (self._cache / 'miscellaneous').mkdir()

        for measure in [*MEASURES.values(), *COVARIATES.values()]:
            try:
                summary_data = self.pdi.load_output_summaries(self.scenario, measure.label)
                summary_data.to_parquet(self._cache / 'summaries' / f'{measure.label}.parquet', compression=None)
            except FileNotFoundError:
                logger.warning(f'No {measure.label} data found for {self.version}. Skipping.')

            if hasattr(measure, 'cumulative_label') and measure.cumulative_label:
                try:
                    summary_data = self.pdi.load_output_summaries(self.scenario, measure.cumulative_label)
                    summary_data.to_parquet(self._cache / 'summaries' / f'{measure.cumulative_label}.parquet',
                                            compression=None)
                except FileNotFoundError:
                    logger.warning(f'No {measure.cumulative_label} data found for {self.version}. Skipping.')

            if measure.label in cache_draws:
                try:
                    draw_data = self.pdi.load_output_draws(self.scenario, measure.label)
                    draw_data.to_parquet(self._cache / 'draws' / f'{measure.label}.parquet', compression=None)
                except FileNotFoundError:
                    logger.warning(f'No {measure.label} draw data found for {self.version}. Skipping.')

        for measure in MISCELLANEOUS.values():
            if measure.is_table:
                try:
                    data = self.pdi.load_output_miscellaneous(self.scenario, measure.label, measure.is_table)
                    data.to_parquet(self._cache / 'miscellaneous' / f'{measure.label}.parquet', compression=None)
                except FileNotFoundError:
                    logger.warning(f'No {measure.label} data found for {self.version}. Skipping.')
            else:
                # Don't need any of these cached for now
                pass

    def load_from_cache(self, data_type: str, measure: str):
        if self._cache is None:
            raise FileNotFoundError
        return pd.read_parquet(self._cache / data_type / f'{measure}.parquet', engine='fastparquet')

    def clean_data(self, data: pd.DataFrame, location_id: Optional[int]):
        data = data.reset_index()
        if 'location_id' in data.columns and location_id is not None:
            data = data[data.location_id == location_id].drop(columns='location_id')
        if 'date' in data.columns:
            data['date'] = pd.to_datetime(data['date'])
        return data


def make_plot_versions(comparators: List[ComparatorSpecification], color_map) -> List[PlotVersion]:
    primary_versions = []
    plot_versions = []
    for comparator in comparators:
        for scenario, label in comparator.scenarios.items():
            if scenario.endswith('*'):
                primary_versions.append((Path(comparator.version), scenario[:-1], label))
            else:
                plot_versions.append((Path(comparator.version), scenario, label))
    primary_versions = [PlotVersion(*pv, color_map(i)) for i, pv in enumerate(primary_versions)]
    normal_versions = [PlotVersion(*pv, color_map(i + len(primary_versions))) for i, pv in enumerate(plot_versions)]
    return normal_versions + primary_versions

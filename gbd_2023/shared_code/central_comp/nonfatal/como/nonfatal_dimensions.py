from copy import deepcopy
from typing import Dict, List, Tuple, Union

import numpy as np

from gbd.constants import age, measures
from ihme_dimensions.dimensionality import DataFrameDimensions


class NonfatalDimensions:
    """Class to provide dimensionality of a simulation in different contexts."""

    def __init__(
        self,
        simulation_index: Dict[str, List[int]],
        draw_index: Dict[str, List[str]],
        cause_index: Dict[str, List[int]],
        sequela_index: Dict[str, List[int]],
        impairment_index: Dict[Tuple[str, str], List[Tuple[int, int]]],
        injuries_index: Dict[Tuple[str, str], List[Tuple[int, int]]],
    ):
        self.simulation_index = simulation_index
        self.draw_index = draw_index
        self.cause_index = cause_index
        self.sequela_index = sequela_index
        self.impairment_index = impairment_index
        self.injuries_index = injuries_index

    def get_dimension_by_component(
        self, component: str, measure_id: int
    ) -> DataFrameDimensions:
        """Dimensionality with index extended depending on component."""
        at_birth = True if measure_id == measures.INCIDENCE else False
        if component == "cause":
            dimensions = self.get_cause_dimensions(measure_id, at_birth)
        if component == "impairment":
            dimensions = self.get_impairment_dimensions(measure_id)
        if component == "sequela":
            dimensions = self.get_sequela_dimensions(measure_id, at_birth)
        if component == "injuries":
            dimensions = self.get_injuries_dimensions(measure_id)
        return dimensions

    def get_simulation_dimensions(
        self, measure_id: Union[int, List[int]], at_birth: bool = False
    ) -> DataFrameDimensions:
        """Dimensionality of simulation."""
        sim_idx = deepcopy(self.simulation_index)
        draw_idx = deepcopy(self.draw_index)
        dim = DataFrameDimensions(sim_idx, draw_idx)
        dim.index_dim.add_level("measure_id", np.atleast_1d(measure_id).tolist())
        if at_birth:
            dim.index_dim.extend_level("age_group_id", [age.BIRTH])
        return dim

    def get_cause_dimensions(
        self, measure_id: int, at_birth: bool = False
    ) -> DataFrameDimensions:
        """Dimensionality including cause in index."""
        base_dim = self.get_simulation_dimensions(measure_id, at_birth)
        for key, val in list(self.cause_index.items()):
            base_dim.index_dim.add_level(key, val)
        return base_dim

    def get_sequela_dimensions(
        self, measure_id: int, at_birth: bool = False
    ) -> DataFrameDimensions:
        """Dimensionality including sequela in index."""
        base_dim = self.get_simulation_dimensions(measure_id, at_birth)
        for key, val in list(self.sequela_index.items()):
            base_dim.index_dim.add_level(key, val)
        return base_dim

    def get_impairment_dimensions(
        self, measure_id: int, at_birth: bool = False
    ) -> DataFrameDimensions:
        """Dimensionality including impairment in index."""
        base_dim = self.get_simulation_dimensions(measure_id, at_birth)
        for key, val in list(self.impairment_index.items()):
            base_dim.index_dim.add_level(key, val)
        return base_dim

    def get_injuries_dimensions(
        self, measure_id: int, at_birth: bool = False
    ) -> DataFrameDimensions:
        """Dimensionality including injuries in index."""
        base_dim = self.get_simulation_dimensions(measure_id, at_birth)
        for key, val in list(self.injuries_index.items()):
            base_dim.index_dim.add_level(key, val)
        return base_dim

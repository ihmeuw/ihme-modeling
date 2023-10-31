from pathlib import Path
from typing import List, Optional

import pandas as pd

from covid_model_seiir_pipeline.lib import (
    io,
    static_vars,
)
from covid_model_seiir_pipeline.pipeline.parameter_fit.specification import (
    FitSpecification,
)
from covid_model_seiir_pipeline.pipeline.regression.data import (
    RegressionSpecification,
    RegressionDataInterface,
)


class FitDataInterface(RegressionDataInterface):

    def __init__(self,
                 infection_root: io.InfectionRoot,
                 covariate_root: io.CovariateRoot,
                 priors_root: Optional[io.CovariatePriorsRoot],
                 coefficient_root: Optional[io.RegressionRoot],
                 regression_root: Optional[io.RegressionRoot],
                 variant_root: io.VariantRoot,
                 fit_root: io.FitRoot):
        super().__init__(infection_root, covariate_root, priors_root, coefficient_root, regression_root)
        self.variant_root = variant_root
        self.fit_root = fit_root

    @classmethod
    def from_specification(cls, specification: FitSpecification) -> 'FitDataInterface':
        infection_root = io.InfectionRoot(specification.data.infection_version)
        covariate_root = io.CovariateRoot(specification.data.covariate_version)
        variant_root = io.VariantRoot(specification.data.variant_version)
        fit_root = io.FitRoot(specification.data.output_root,
                              data_format=specification.data.output_format)
        regression_spec_path = Path(specification.data.coefficient_version) / static_vars.REGRESSION_SPECIFICATION_FILE
        regression_spec = RegressionSpecification.from_path(regression_spec_path)
        coefficient_root = io.RegressionRoot(specification.data.coefficient_version,
                                             data_format=regression_spec.data.output_format)

        return cls(
            infection_root=infection_root,
            covariate_root=covariate_root,
            priors_root=None,
            coefficient_root=coefficient_root,
            regression_root=None,
            variant_root=variant_root,
            fit_root=fit_root,
        )

    def make_dirs(self, **prefix_args) -> None:
        io.touch(self.fit_root, **prefix_args)

    ####################
    # Metadata loaders #
    ####################

    def get_n_draws(self) -> int:
        fit_spec = io.load(self.fit_root.specification())
        return fit_spec['data']['n_draws']

    #######################
    # Fit data I/O #
    #######################

    def save_specification(self, specification: FitSpecification) -> None:
        io.dump(specification.to_dict(), self.fit_root.specification())

    def load_specification(self) -> FitSpecification:
        spec_dict = io.load(self.fit_root.specification())
        return FitSpecification.from_dict(spec_dict)

    def save_location_ids(self, location_ids: List[int]) -> None:
        io.dump(location_ids, self.fit_root.locations())

    def load_location_ids(self) -> List[int]:
        return io.load(self.fit_root.locations())

    def save_hierarchy(self, hierarchy: pd.DataFrame) -> None:
        io.dump(hierarchy, self.fit_root.hierarchy())

    def load_hierarchy(self) -> pd.DataFrame:
        return io.load(self.fit_root.hierarchy())

    def save_betas(self, betas: pd.DataFrame, scenario: str, draw_id: int) -> None:
        io.dump(betas, self.fit_root.beta(scenario=scenario, draw_id=draw_id))

    def load_betas(self, scenario: str, draw_id: int) -> pd.DataFrame:
        return io.load(self.fit_root.beta(scenario=scenario, draw_id=draw_id))

    def save_compartments(self, compartments: pd.DataFrame, scenario: str, draw_id: int) -> None:
        io.dump(compartments, self.fit_root.compartments(scenario=scenario, draw_id=draw_id))

    def load_compartments(self, scenario: str, draw_id: int) -> pd.DataFrame:
        return io.load(self.fit_root.compartments(scenario=scenario, draw_id=draw_id))

    def save_ode_parameters(self, df: pd.DataFrame, scenario: str, draw_id: int) -> None:
        io.dump(df, self.fit_root.ode_parameters(scenario=scenario, draw_id=draw_id))

    def load_ode_parameters(self, scenario: str, draw_id: int) -> pd.DataFrame:
        return io.load(self.fit_root.ode_parameters(scenario=scenario, draw_id=draw_id))

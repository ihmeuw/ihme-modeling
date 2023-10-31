from functools import reduce
from pathlib import Path
from typing import Dict, List, Tuple

from loguru import logger
import pandas as pd

from covid_model_seiir_pipeline.lib import (
    io,
    static_vars,
)
from covid_model_seiir_pipeline.pipeline.regression import (
    RegressionDataInterface,
    RegressionSpecification,
    HospitalParameters,
)
from covid_model_seiir_pipeline.pipeline.forecasting.specification import (
    ForecastSpecification,
    ScenarioSpecification,
)
from covid_model_seiir_pipeline.pipeline.forecasting.model import (
    RatioData,
    HospitalMetrics,
    HospitalCorrectionFactors,
    HospitalCensusData,
)


class ForecastDataInterface:

    def __init__(self,
                 regression_root: io.RegressionRoot,
                 covariate_root: io.CovariateRoot,
                 forecast_root: io.ForecastRoot):
        self.regression_root = regression_root
        self.covariate_root = covariate_root
        self.forecast_root = forecast_root

    @classmethod
    def from_specification(cls, specification: ForecastSpecification) -> 'ForecastDataInterface':
        regression_spec_path = Path(specification.data.regression_version) / static_vars.REGRESSION_SPECIFICATION_FILE
        regression_spec = RegressionSpecification.from_path(regression_spec_path)
        regression_root = io.RegressionRoot(specification.data.regression_version,
                                            data_format=regression_spec.data.output_format)
        covariate_root = io.CovariateRoot(specification.data.covariate_version)
        # TODO: specify output format from config.
        forecast_root = io.ForecastRoot(specification.data.output_root,
                                        data_format=specification.data.output_format)

        return cls(
            regression_root=regression_root,
            covariate_root=covariate_root,
            forecast_root=forecast_root,
        )

    def make_dirs(self, **prefix_args):
        io.touch(self.forecast_root, **prefix_args)

    ############################
    # Regression paths loaders #
    ############################

    def get_n_draws(self) -> int:
        return self._get_regression_data_interface().get_n_draws()

    def is_counties_run(self) -> bool:
        return self._get_regression_data_interface().is_counties_run()

    def get_infections_metadata(self):
        return self._get_regression_data_interface().get_infections_metadata()

    def get_model_inputs_metadata(self):
        return self._get_regression_data_interface().get_model_inputs_metadata()

    def load_hierarchy(self) -> pd.DataFrame:
        return self._get_regression_data_interface().load_hierarchy()

    def load_location_ids(self) -> List[int]:
        return self._get_regression_data_interface().load_location_ids()

    def load_population(self) -> pd.DataFrame:
        return self._get_regression_data_interface().load_population()

    def load_five_year_population(self) -> pd.DataFrame:
        return self._get_regression_data_interface().load_five_year_population()

    def load_total_population(self) -> pd.Series:
        return self._get_regression_data_interface().load_total_population()

    def load_full_data_unscaled(self) -> pd.DataFrame:
        return self._get_regression_data_interface().load_full_data_unscaled()

    def load_total_deaths(self) -> pd.Series:
        return self._get_regression_data_interface().load_total_deaths()

    def load_betas(self, draw_id: int):
        return self._get_regression_data_interface().load_betas(draw_id=draw_id)

    def load_covariate(self, covariate: str, covariate_version: str, with_observed: bool = False) -> pd.DataFrame:
        return self._get_regression_data_interface().load_covariate(
            covariate, covariate_version, with_observed, self.covariate_root,
        )

    def load_covariates(self, covariates: Dict[str, str]) -> pd.DataFrame:
        return self._get_regression_data_interface().load_covariates(
            covariates, self.covariate_root,
        )

    def load_vaccinations(self, vaccine_scenario: str) -> pd.DataFrame:
        return self._get_regression_data_interface().load_vaccinations(
            vaccine_scenario, self.covariate_root,
        )

    def load_vaccination_summaries(self, measure: str, scenario: str):
        spec = self.load_specification()
        vaccine_scenario = spec.scenarios[scenario].vaccine_version
        return self._get_regression_data_interface().load_vaccination_summaries(
            measure, vaccine_scenario, self.covariate_root,
        )

    def load_vaccine_efficacy(self):
        return self._get_regression_data_interface().load_vaccine_efficacy(
            self.covariate_root,
        )

    def load_mobility_info(self, info_type: str) -> pd.DataFrame:
        return self._get_regression_data_interface().load_mobility_info(
            info_type, self.covariate_root,
        )

    def load_mandate_data(self, mobility_scenario: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        return self._get_regression_data_interface().load_mandate_data(
            mobility_scenario, self.covariate_root,
        )

    def load_raw_variant_prevalence(self, variant_scenario: str) -> pd.DataFrame:
        return self._get_regression_data_interface().load_raw_variant_prevalence(
            variant_scenario, self.covariate_root,
        )

    def load_variant_prevalence(self, variant_scenario: str) -> pd.DataFrame:
        return self._get_regression_data_interface().load_variant_prevalence(
            variant_scenario, self.covariate_root,
        )

    def load_coefficients(self, draw_id: int) -> pd.DataFrame:
        return self._get_regression_data_interface().load_coefficients(draw_id=draw_id)

    def load_compartments(self, draw_id: int) -> pd.DataFrame:
        return self._get_regression_data_interface().load_compartments(draw_id=draw_id)

    def load_ode_parameters(self, draw_id: int) -> pd.DataFrame:
        return self._get_regression_data_interface().load_ode_parameters(draw_id=draw_id)

    def load_past_infections(self, draw_id: int) -> pd.Series:
        return self._get_regression_data_interface().load_infections(draw_id=draw_id)

    def load_em_scalars_draws(self) -> pd.DataFrame:
        return self._get_regression_data_interface().load_em_scalars_draws()

    def load_em_scalars(self, draw_id: int = None) -> pd.Series:
        return self._get_regression_data_interface().load_em_scalars(draw_id)

    def load_past_deaths(self, draw_id: int) -> pd.Series:
        return self._get_regression_data_interface().load_deaths(draw_id=draw_id)

    def get_hospital_parameters(self) -> HospitalParameters:
        return self._get_regression_data_interface().load_specification().hospital_parameters

    def load_hospital_usage(self) -> HospitalMetrics:
        df = self._get_regression_data_interface().load_hospitalizations(measure='usage')
        return HospitalMetrics(**{metric: df[metric] for metric in df.columns})

    def load_hospital_correction_factors(self) -> HospitalCorrectionFactors:
        df = self._get_regression_data_interface().load_hospitalizations(measure='correction_factors')
        return HospitalCorrectionFactors(**{metric: df[metric] for metric in df.columns})

    def load_hospital_census_data(self) -> HospitalCensusData:
        return self._get_regression_data_interface().load_hospital_census_data()

    def load_hospital_bed_capacity(self) -> pd.DataFrame:
        return self._get_regression_data_interface().load_hospital_bed_capacity()

    def load_ifr(self, draw_id: int) -> pd.DataFrame:
        return self._get_regression_data_interface().load_ifr(draw_id=draw_id)

    def load_ihr(self, draw_id: int) -> pd.DataFrame:
        return self._get_regression_data_interface().load_ihr(draw_id=draw_id)

    def load_idr(self, draw_id: int) -> pd.DataFrame:
        return self._get_regression_data_interface().load_idr(draw_id=draw_id)

    def load_ratio_data(self, draw_id: int) -> RatioData:
        return self._get_regression_data_interface().load_ratio_data(draw_id=draw_id)

    ##########################
    # Covariate data loaders #
    ##########################

    def check_covariates(self, scenarios: Dict[str, ScenarioSpecification]) -> List[str]:
        regression_spec = self._get_regression_data_interface().load_specification().to_dict()
        # Bit of a hack.
        forecast_version = str(self.covariate_root._root)
        regression_version = regression_spec['data']['covariate_version']
        if not forecast_version == regression_version:
            logger.warning(f'Forecast covariate version {forecast_version} does not match '
                           f'regression covariate version {regression_version}. If the two covariate '
                           f'versions have different data in the past, the regression coefficients '
                           f'used for prediction may not be valid.')

        regression_covariates = set(regression_spec['covariates'])

        for name, scenario in scenarios.items():
            if set(scenario.covariates).symmetric_difference(regression_covariates) > {'intercept'}:
                raise ValueError('Forecast covariates must match the covariates used in regression.\n'
                                 f'Forecast covariates:   {sorted(list(scenario.covariates))}.\n'
                                 f'Regression covariates: {sorted(list(regression_covariates))}.')

            if 'intercept' in scenario.covariates:
                # Shouldn't really be specified, but might be copied over from
                # regression.  No harm really in just deleting it.
                del scenario.covariates['intercept']

            for covariate, covariate_version in scenario.covariates.items():
                if not io.exists(self.covariate_root[covariate](covariate_scenario=covariate_version)):
                    raise FileNotFoundError(f'No {covariate_version} file found for covariate {covariate}.')

        return list(regression_covariates)

    #####################
    # Forecast data I/O #
    #####################

    def save_specification(self, specification: ForecastSpecification) -> None:
        io.dump(specification.to_dict(), self.forecast_root.specification())

    def load_specification(self) -> ForecastSpecification:
        spec_dict = io.load(self.forecast_root.specification())
        return ForecastSpecification.from_dict(spec_dict)

    def save_raw_covariates(self, covariates: pd.DataFrame, scenario: str, draw_id: int) -> None:
        io.dump(covariates, self.forecast_root.raw_covariates(scenario=scenario, draw_id=draw_id))

    def load_raw_covariates(self, scenario: str, draw_id: int) -> pd.DataFrame:
        return io.load(self.forecast_root.raw_covariates(scenario=scenario, draw_id=draw_id))

    def save_ode_params(self, ode_params: pd.DataFrame, scenario: str, draw_id: int) -> None:
        io.dump(ode_params, self.forecast_root.ode_params(scenario=scenario, draw_id=draw_id))

    def load_ode_params(self, scenario: str, draw_id: int) -> pd.DataFrame:
        return io.load(self.forecast_root.ode_params(scenario=scenario, draw_id=draw_id))

    def save_components(self, forecasts: pd.DataFrame, scenario: str, draw_id: int):
        io.dump(forecasts, self.forecast_root.component_draws(scenario=scenario, draw_id=draw_id))

    def load_components(self, scenario: str, draw_id: int):
        return io.load(self.forecast_root.component_draws(scenario=scenario, draw_id=draw_id))

    def save_beta_scales(self, scales: pd.DataFrame, scenario: str, draw_id: int):
        io.dump(scales, self.forecast_root.beta_scaling(scenario=scenario, draw_id=draw_id))

    def load_beta_scales(self, scenario: str, draw_id: int):
        return io.load(self.forecast_root.beta_scaling(scenario=scenario, draw_id=draw_id))

    def save_beta_residual(self, residual: pd.DataFrame, scenario: str, draw_id: int):
        io.dump(residual, self.forecast_root.beta_residual(scenario=scenario, draw_id=draw_id))

    def load_beta_residual(self, scenario: str, draw_id: int):
        return io.load(self.forecast_root.beta_residual(scenario=scenario, draw_id=draw_id))

    def save_raw_outputs(self, raw_outputs: pd.DataFrame, scenario: str, draw_id: int):
        io.dump(raw_outputs, self.forecast_root.raw_outputs(scenario=scenario, draw_id=draw_id))

    def load_raw_outputs(self, scenario: str, draw_id: int):
        return io.load(self.forecast_root.raw_outputs(scenario=scenario, draw_id=draw_id))

    #########################
    # Non-interface helpers #
    #########################

    def _get_regression_data_interface(self) -> RegressionDataInterface:
        regression_spec = RegressionSpecification.from_dict(io.load(self.regression_root.specification()))
        regression_di = RegressionDataInterface.from_specification(regression_spec)
        return regression_di

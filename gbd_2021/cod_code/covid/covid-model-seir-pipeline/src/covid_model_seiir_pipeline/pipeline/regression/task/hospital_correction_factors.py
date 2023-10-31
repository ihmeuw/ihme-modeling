from dataclasses import asdict
from pathlib import Path

import click
import pandas as pd

from covid_model_seiir_pipeline.lib import (
    cli_tools,
    static_vars,
)
from covid_model_seiir_pipeline.pipeline.regression.specification import (
    RegressionSpecification,
    REGRESSION_JOBS,
)
from covid_model_seiir_pipeline.pipeline.regression.data import (
    RegressionDataInterface,
)
from covid_model_seiir_pipeline.pipeline.regression import (
    model,
)


logger = cli_tools.task_performance_logger


def run_hospital_correction_factors(regression_version: str, with_progress_bar: bool) -> None:
    logger.info('Starting hospital correction factors.', context='setup')
    # Build helper abstractions
    regression_spec_file = Path(regression_version) / static_vars.REGRESSION_SPECIFICATION_FILE
    regression_specification = RegressionSpecification.from_path(regression_spec_file)
    hospital_parameters = regression_specification.hospital_parameters
    data_interface = RegressionDataInterface.from_specification(regression_specification)

    logger.info('Loading input data', context='read')
    hierarchy = data_interface.load_hierarchy().reset_index()
    n_draws = data_interface.get_n_draws()
    n_cores = (regression_specification
               .workflow
               .task_specifications[REGRESSION_JOBS.hospital_correction_factors]
               .num_cores)

    admissions, hfr = model.load_admissions_and_hfr(
        data_interface,
        n_draws,
        n_cores,
        with_progress_bar
    )
    hospital_census_data = data_interface.load_hospital_census_data()
    logger.info('Computing hospital usage', context='compute_usage')
    hospital_usage = model.compute_hospital_usage(
        admissions,
        hfr,
        hospital_parameters,
    )
    logger.info('Computing correction factors', context='compute_corrections')
    correction_factors = model.calculate_hospital_correction_factors(
        hospital_usage,
        hospital_census_data,
        hierarchy,
        hospital_parameters,
    )

    logger.info('Prepping outputs', context='transform')
    usage = [value.rename(key) for key, value in asdict(hospital_usage).items()]
    usage_df = pd.concat(usage, axis=1)
    corrections = [value.rename(key) for key, value in asdict(correction_factors).items()]
    corrections_df = pd.concat(corrections, axis=1)

    logger.info('Writing outputs', context='write')
    data_interface.save_hospitalizations(usage_df, 'usage')
    data_interface.save_hospitalizations(corrections_df, 'correction_factors')

    logger.report()


@click.command()
@cli_tools.with_regression_version
@cli_tools.with_progress_bar
@cli_tools.add_verbose_and_with_debugger
def hospital_correction_factors(regression_version: str, progress_bar: bool,
                                verbose: int, with_debugger: bool):
    cli_tools.configure_logging_to_terminal(verbose)

    run = cli_tools.handle_exceptions(run_hospital_correction_factors, logger, with_debugger)
    run(regression_version=regression_version, with_progress_bar=progress_bar)


if __name__ == '__main__':
    hospital_correction_factors()

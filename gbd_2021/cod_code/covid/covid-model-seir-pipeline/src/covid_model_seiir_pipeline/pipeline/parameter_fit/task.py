from pathlib import Path

import click
import numpy as np

from covid_model_seiir_pipeline.lib import (
    cli_tools,
    static_vars,
)
from covid_model_seiir_pipeline.pipeline.regression.model import (
    clean_infection_data_measure,
    prepare_ode_fit_parameters,
    run_ode_fit,
    sample_params,
)
from covid_model_seiir_pipeline.pipeline.parameter_fit.data import FitDataInterface
from covid_model_seiir_pipeline.pipeline.parameter_fit.specification import FitSpecification


logger = cli_tools.task_performance_logger


def run_parameter_fit(fit_version: str, scenario: str, draw_id: int, progress_bar: bool) -> None:
    logger.info('Starting beta fit.', context='setup')
    # Build helper abstractions
    fit_spec_file = Path(fit_version) / static_vars.FIT_SPECIFICATION_FILE
    fit_specification = FitSpecification.from_path(fit_spec_file)
    data_interface = FitDataInterface.from_specification(fit_specification)

    logger.info('Loading ODE fit input data', context='read')
    past_infection_data = data_interface.load_past_infection_data(draw_id=draw_id)
    population = data_interface.load_five_year_population()
    rhos = data_interface.load_variant_prevalence()
    vaccinations = data_interface.load_vaccinations()

    logger.info('Prepping ODE fit parameters.', context='transform')
    infections = clean_infection_data_measure(past_infection_data, 'infections')
    fit_params = fit_specification.scenarios[scenario]

    np.random.seed(draw_id)
    sampled_params = sample_params(
        infections.index, fit_params.to_dict(),
        params_to_sample=['alpha', 'sigma', 'gamma1', 'gamma2', 'kappa', 'phi', 'psi', 'pi', 'chi']
    )
    ode_parameters = prepare_ode_fit_parameters(
        infections,
        population,
        rhos,
        vaccinations,
        sampled_params,
    )

    logger.info('Running ODE fit', context='compute_ode')
    beta, compartments = run_ode_fit(
        ode_parameters=ode_parameters,
        progress_bar=progress_bar,
    )

    data_interface.save_betas(beta, scenario=scenario, draw_id=draw_id)
    data_interface.save_compartments(compartments, scenario=scenario, draw_id=draw_id)
    data_interface.save_ode_parameters(ode_parameters.to_df(), scenario=scenario, draw_id=draw_id)

    logger.report()


@click.command()
@cli_tools.with_task_fit_version
@cli_tools.with_scenario
@cli_tools.with_draw_id
@cli_tools.with_progress_bar
@cli_tools.add_verbose_and_with_debugger
def parameter_fit(fit_version: str, scenario: str, draw_id: int,
                  progress_bar: bool, verbose: int, with_debugger: bool):
    cli_tools.configure_logging_to_terminal(verbose)
    run = cli_tools.handle_exceptions(run_parameter_fit, logger, with_debugger)
    run(fit_version=fit_version,
        scenario=scenario,
        draw_id=draw_id,
        progress_bar=progress_bar)


if __name__ == '__main__':
    parameter_fit()

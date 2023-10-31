from pathlib import Path

import click
import numpy as np
import pandas as pd

from covid_model_seiir_pipeline.lib import (
    cli_tools,
    math,
    static_vars,
)
from covid_model_seiir_pipeline.pipeline.regression.data import RegressionDataInterface
from covid_model_seiir_pipeline.pipeline.regression.specification import RegressionSpecification
from covid_model_seiir_pipeline.pipeline.regression import model


logger = cli_tools.task_performance_logger


def run_beta_regression(regression_version: str, draw_id: int, progress_bar: bool) -> None:
    logger.info('Starting beta regression.', context='setup')
    # Build helper abstractions
    regression_spec_file = Path(regression_version) / static_vars.REGRESSION_SPECIFICATION_FILE
    regression_specification = RegressionSpecification.from_path(regression_spec_file)
    data_interface = RegressionDataInterface.from_specification(regression_specification)

    logger.info('Loading ODE fit input data', context='read')
    hierarchy = data_interface.load_hierarchy()
    past_infection_data = data_interface.load_past_infection_data(draw_id=draw_id)
    population = data_interface.load_five_year_population()
    rhos = data_interface.load_variant_prevalence()
    vaccinations = data_interface.load_vaccinations()

    logger.info('Prepping ODE fit parameters.', context='transform')
    infections = model.clean_infection_data_measure(past_infection_data, 'infections')
    regression_params = regression_specification.regression_parameters.to_dict()

    np.random.seed(draw_id)
    sampled_params = model.sample_params(
        infections.index, regression_params,
        params_to_sample=['alpha', 'sigma', 'gamma1', 'gamma2', 'kappa', 'chi', 'pi'],
        draw_id=draw_id,
    )

    sampled_params['phi'] = pd.Series(
        np.random.normal(loc=sampled_params['chi'] + regression_params['phi_mean_shift'],
                         scale=regression_params['phi_sd']),
        index=infections.index, name='phi',
    )
    sampled_params['psi'] = pd.Series(
        np.random.normal(loc=sampled_params['chi'] + regression_params['psi_mean_shift'],
                         scale=regression_params['psi_sd']),
        index=infections.index, name='psi',
    )

    ode_parameters = model.prepare_ode_fit_parameters(
        infections,
        population,
        rhos,
        vaccinations,
        sampled_params,
    )

    logger.info('Running ODE fit', context='compute_ode')
    beta_fit, compartments = model.run_ode_fit(
        ode_parameters=ode_parameters,
        progress_bar=progress_bar,
    )

    logger.info('Loading regression input data', context='read')
    covariates = data_interface.load_covariates(list(regression_specification.covariates))
    gaussian_priors = data_interface.load_priors(regression_specification.covariates.values())
    prior_coefficients = data_interface.load_prior_run_coefficients(draw_id=draw_id)
    if gaussian_priors and prior_coefficients:
        raise NotImplementedError

    logger.info('Fitting beta regression', context='compute_regression')
    coefficients = model.run_beta_regression(
        beta_fit['beta'],
        covariates,
        regression_specification.covariates.values(),
        gaussian_priors,
        prior_coefficients,
        hierarchy,
    )
    log_beta_hat = math.compute_beta_hat(covariates, coefficients)
    beta_hat = np.exp(log_beta_hat).rename('beta_hat')

    # Format and save data.
    logger.info('Prepping outputs', context='transform')
    betas = pd.concat([beta_fit, beta_hat], axis=1).reindex(infections.index)
    deaths = model.clean_infection_data_measure(past_infection_data, 'deaths')
    ode_parameters = ode_parameters.to_df()

    logger.info('Writing outputs', context='write')
    data_interface.save_infections(infections, draw_id=draw_id)
    data_interface.save_deaths(deaths, draw_id=draw_id)
    data_interface.save_betas(betas, draw_id=draw_id)
    data_interface.save_compartments(compartments, draw_id=draw_id)
    data_interface.save_coefficients(coefficients, draw_id=draw_id)
    data_interface.save_ode_parameters(ode_parameters, draw_id=draw_id)

    logger.report()


@click.command()
@cli_tools.with_task_regression_version
@cli_tools.with_draw_id
@cli_tools.add_verbose_and_with_debugger
@cli_tools.with_progress_bar
def beta_regression(regression_version: str, draw_id: int,
                    progress_bar: bool, verbose: int, with_debugger: bool):
    cli_tools.configure_logging_to_terminal(verbose)
    run = cli_tools.handle_exceptions(run_beta_regression, logger, with_debugger)
    run(regression_version=regression_version,
        draw_id=draw_id,
        progress_bar=progress_bar)


if __name__ == '__main__':
    beta_regression()

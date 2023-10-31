from pathlib import Path

import click
import pandas as pd

from covid_model_seiir_pipeline.lib import (
    cli_tools,
    static_vars,
)
from covid_model_seiir_pipeline.pipeline.forecasting import model
from covid_model_seiir_pipeline.pipeline.forecasting.specification import ForecastSpecification
from covid_model_seiir_pipeline.pipeline.forecasting.data import ForecastDataInterface


logger = cli_tools.task_performance_logger


def run_beta_forecast(forecast_version: str, scenario: str, draw_id: int, progress_bar: bool):
    logger.info(f"Initiating SEIIR beta forecasting for scenario {scenario}, draw {draw_id}.", context='setup')
    forecast_spec: ForecastSpecification = ForecastSpecification.from_path(
        Path(forecast_version) / static_vars.FORECAST_SPECIFICATION_FILE
    )
    scenario_spec = forecast_spec.scenarios[scenario]
    data_interface = ForecastDataInterface.from_specification(forecast_spec)
    #################
    # Build indices #
    #################
    # The hardest thing to keep consistent is data alignment. We have about 100
    # unique datasets in this model and they need to be aligned consistently
    # to do computation.
    logger.info('Loading index building data', context='read')
    past_infections = data_interface.load_past_infections(draw_id)
    past_start_dates = past_infections.reset_index().groupby('location_id').date.min()
    forecast_start_dates = past_infections.reset_index().groupby('location_id').date.max()
    # Forecast is run to the end of the covariates
    covariates = data_interface.load_covariates(scenario_spec.covariates)
    forecast_end_dates = covariates.reset_index().groupby('location_id').date.max().reindex(forecast_start_dates.index, fill_value=pd.Timestamp('2023-01-01'))
    population = data_interface.load_five_year_population().groupby('location_id').population.sum()

    logger.info('Building indices', context='transform')
    indices = model.Indices(
        past_start_dates,
        forecast_start_dates,
        forecast_end_dates,
    )

    ########################################
    # Build parameters for the SEIIR model #
    ########################################
    logger.info('Loading SEIIR parameter input data.', context='read')
    # We'll use the same params in the ODE forecast as we did in the fit.
    ode_params = data_interface.load_ode_parameters(draw_id=draw_id)
    # Contains both the fit and regression betas
    betas = data_interface.load_betas(draw_id)
    # Rescaling parameters for the beta forecast.
    beta_scales = data_interface.load_beta_scales(scenario=scenario, draw_id=draw_id)
    # Regression coefficients for forecasting beta.
    coefficients = data_interface.load_coefficients(draw_id)
    # Vaccine data, of course.
    vaccinations = data_interface.load_vaccinations(scenario_spec.vaccine_version)
    # Variant prevalences.
    rhos = data_interface.load_variant_prevalence(scenario_spec.variant_version)
    log_beta_shift = (scenario_spec.log_beta_shift,
                      pd.Timestamp(scenario_spec.log_beta_shift_date))
    beta_scale = (scenario_spec.beta_scale,
                  pd.Timestamp(scenario_spec.beta_scale_date))

    # Collate all the parameters, ensure consistent index, etc.
    logger.info('Processing inputs into model parameters.', context='transform')
    covariates = covariates.reindex(indices.full)
    model_parameters = model.build_model_parameters(
        indices,
        ode_params,
        betas,
        covariates,
        coefficients,
        rhos,
        beta_scales,
        vaccinations,
        log_beta_shift,
        beta_scale,
    )

    # Pull in compartments from the fit and subset out the initial condition.
    logger.info('Loading past compartment data.', context='read')
    past_compartments = data_interface.load_compartments(draw_id=draw_id)
    initial_condition = past_compartments.loc[indices.initial_condition].reset_index(level='date', drop=True)

    ###################################################
    # Construct parameters for postprocessing results #
    ###################################################
    logger.info('Loading results processing input data.', context='read')
    past_deaths = data_interface.load_past_deaths(draw_id=draw_id)
    ratio_data = data_interface.load_ratio_data(draw_id=draw_id)
    hospital_parameters = data_interface.get_hospital_parameters()
    correction_factors = data_interface.load_hospital_correction_factors()

    logger.info('Prepping results processing parameters.', context='transform')
    postprocessing_params = model.build_postprocessing_parameters(
        indices,
        past_compartments,
        past_infections,
        past_deaths,
        ratio_data,
        model_parameters,
        correction_factors,
        hospital_parameters,
        scenario_spec,
    )

    logger.info('Running ODE forecast.', context='compute_ode')
    future_components = model.run_ode_model(
        initial_condition,
        model_parameters.reindex(indices.future),
        progress_bar,
    )
    logger.info('Processing ODE results and computing deaths and infections.', context='compute_results')
    components, system_metrics, output_metrics = model.compute_output_metrics(
        indices,
        future_components,
        postprocessing_params,
        model_parameters,
        hospital_parameters,
    )

    if scenario_spec.algorithm == 'draw_level_mandate_reimposition':
        logger.info('Entering mandate reimposition.', context='compute_mandates')
        # Info data specific to mandate reimposition
        percent_mandates, mandate_effects = data_interface.load_mandate_data(scenario_spec.covariates['mobility'])
        em_scalars = data_interface.load_em_scalars(draw_id)
        min_wait, days_on, reimposition_threshold, max_threshold = model.unpack_parameters(
            scenario_spec.algorithm_params,
            em_scalars,
        )
        reimposition_threshold = model.compute_reimposition_threshold(
            postprocessing_params.past_deaths,
            population,
            reimposition_threshold,
            max_threshold,
        )
        reimposition_count = 0
        reimposition_dates = {}
        last_reimposition_end_date = pd.Series(pd.NaT, index=population.index)
        reimposition_date = model.compute_reimposition_date(
            output_metrics.deaths,
            population,
            reimposition_threshold,
            min_wait,
            last_reimposition_end_date
        )

        while len(reimposition_date):  # any place reimposes mandates.
            logger.info(f'On mandate reimposition {reimposition_count + 1}. {len(reimposition_date)} locations '
                        f'are reimposing mandates.')
            mobility = covariates['mobility']
            mobility_lower_bound = model.compute_mobility_lower_bound(
                mobility,
                mandate_effects,
            )

            new_mobility = model.compute_new_mobility(
                mobility,
                reimposition_date,
                mobility_lower_bound,
                percent_mandates,
                days_on,
            )

            covariates['mobility'] = new_mobility

            model_parameters = model.build_model_parameters(
                indices,
                ode_params,
                betas,
                covariates,
                coefficients,
                rhos,
                beta_scales,
                vaccinations,
                log_beta_shift,
                beta_scale,
            )

            # The ode is done as a loop over the locations in the initial condition.
            # As locations that don't reimpose mandates produce identical forecasts,
            # subset here to only the locations that reimpose mandates for speed.
            initial_condition_subset = initial_condition.loc[reimposition_date.index]
            logger.info('Running ODE forecast.', context='compute_ode')
            future_components_subset = model.run_ode_model(
                initial_condition_subset,
                model_parameters.reindex(indices.future),
                progress_bar,
            )

            logger.info('Processing ODE results and computing deaths and infections.', context='compute_results')
            future_components = (future_components
                                 .sort_index()
                                 .drop(future_components_subset.index)
                                 .append(future_components_subset)
                                 .sort_index())
            components, system_metrics, output_metrics = model.compute_output_metrics(
                indices,
                future_components,
                postprocessing_params,
                model_parameters,
                hospital_parameters,
            )

            logger.info('Recomputing reimposition dates', context='compute_mandates')
            reimposition_count += 1
            reimposition_dates[reimposition_count] = reimposition_date
            last_reimposition_end_date.loc[reimposition_date.index] = reimposition_date + days_on
            reimposition_date = model.compute_reimposition_date(
                output_metrics.deaths,
                population,
                reimposition_threshold,
                min_wait,
                last_reimposition_end_date,
            )

    logger.info('Prepping outputs.', context='transform')
    ode_params = model_parameters.to_df()
    outputs = pd.concat([system_metrics.to_df(), output_metrics.to_df(),
                         postprocessing_params.correction_factors_df], axis=1)

    logger.info('Writing outputs.', context='write')
    data_interface.save_ode_params(ode_params, scenario, draw_id)
    data_interface.save_components(components, scenario, draw_id)
    data_interface.save_raw_covariates(covariates, scenario, draw_id)
    data_interface.save_raw_outputs(outputs, scenario, draw_id)

    logger.report()


@click.command()
@cli_tools.with_task_forecast_version
@cli_tools.with_scenario
@cli_tools.with_draw_id
@cli_tools.with_progress_bar
@cli_tools.add_verbose_and_with_debugger
def beta_forecast(forecast_version: str, scenario: str, draw_id: int,
                  progress_bar: bool, verbose: int, with_debugger: bool):
    cli_tools.configure_logging_to_terminal(verbose)

    run = cli_tools.handle_exceptions(run_beta_forecast, logger, with_debugger)
    run(forecast_version=forecast_version,
        scenario=scenario,
        draw_id=draw_id,
        progress_bar=progress_bar)


if __name__ == '__main__':
    beta_forecast()

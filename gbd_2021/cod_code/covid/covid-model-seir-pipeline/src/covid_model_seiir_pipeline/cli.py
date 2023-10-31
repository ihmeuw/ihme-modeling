from typing import Optional

import click
from covid_shared import paths
from loguru import logger
import yaml

from covid_model_seiir_pipeline.lib import (
    cli_tools,
)
from covid_model_seiir_pipeline.pipeline import (
    FitSpecification,
    do_parameter_fit,
    RegressionSpecification,
    do_beta_regression,
    ForecastSpecification,
    do_beta_forecast,
    PostprocessingSpecification,
    do_postprocessing,
    DiagnosticsSpecification,
    do_diagnostics,
)


@click.group()
def seiir():
    """Top level entry point for running SEIIR pipeline stages."""
    pass


@seiir.command(name='oos_fit')
@cli_tools.pass_run_metadata()
@cli_tools.with_fit_specification
@cli_tools.with_infection_version
@cli_tools.with_covariates_version
@cli_tools.with_coefficient_version
@cli_tools.with_variant_version
@cli_tools.with_location_specification
@cli_tools.add_preprocess_only
@cli_tools.add_output_options(paths.SEIR_REGRESSION_OUTPUTS)
@cli_tools.add_verbose_and_with_debugger
def oos_fit(run_metadata,
            fit_specification,
            infection_version, covariates_version, coefficient_version, variant_version,
            location_specification,
            preprocess_only,
            output_root, mark_best, production_tag,
            verbose, with_debugger):
    cli_tools.configure_logging_to_terminal(verbose)

    _do_oos_fit(
        run_metadata=run_metadata,
        fit_specification=fit_specification,
        infection_version=infection_version,
        covariates_version=covariates_version,
        coefficient_version=coefficient_version,
        variant_version=variant_version,
        location_specification=location_specification,
        preprocess_only=preprocess_only,
        output_root=output_root,
        mark_best=mark_best,
        production_tag=production_tag,
        with_debugger=with_debugger,
    )

    logger.info('**Done**')


@seiir.command()
@cli_tools.pass_run_metadata()
@cli_tools.with_regression_specification
@cli_tools.with_infection_version
@cli_tools.with_covariates_version
@cli_tools.with_priors_version
@cli_tools.with_coefficient_version
@cli_tools.with_location_specification
@cli_tools.add_preprocess_only
@cli_tools.add_output_options(paths.SEIR_REGRESSION_OUTPUTS)
@cli_tools.add_verbose_and_with_debugger
def regress(run_metadata,
            regression_specification,
            infection_version, covariates_version,
            priors_version,
            coefficient_version,
            location_specification,
            preprocess_only,
            output_root, mark_best, production_tag,
            verbose, with_debugger):
    """Perform beta regression for a set of infections and covariates."""
    cli_tools.configure_logging_to_terminal(verbose)

    _do_regression(
        run_metadata=run_metadata,
        regression_specification=regression_specification,
        infection_version=infection_version,
        covariates_version=covariates_version,
        priors_version=priors_version,
        coefficient_version=coefficient_version,
        location_specification=location_specification,
        preprocess_only=preprocess_only,
        output_root=output_root,
        mark_best=mark_best,
        production_tag=production_tag,
        with_debugger=with_debugger,
    )

    logger.info('**Done**')


@seiir.command()
@cli_tools.pass_run_metadata()
@cli_tools.with_forecast_specification
@cli_tools.with_regression_version
@cli_tools.with_covariates_version
@cli_tools.add_preprocess_only
@cli_tools.add_output_options(paths.SEIR_FORECAST_OUTPUTS)
@cli_tools.add_verbose_and_with_debugger
def forecast(run_metadata,
             forecast_specification,
             regression_version,
             covariates_version,
             preprocess_only,
             output_root, mark_best, production_tag,
             verbose, with_debugger):
    """Perform beta forecast for a set of scenarios on a regression."""
    cli_tools.configure_logging_to_terminal(verbose)

    _do_forecast(
        run_metadata=run_metadata,
        forecast_specification=forecast_specification,
        regression_version=regression_version,
        covariates_version=covariates_version,
        preprocess_only=preprocess_only,
        output_root=output_root,
        mark_best=mark_best,
        production_tag=production_tag,
        with_debugger=with_debugger,
    )

    logger.info('**Done**')


@seiir.command()
@cli_tools.pass_run_metadata()
@cli_tools.with_postprocessing_specification
@cli_tools.with_forecast_version
@cli_tools.with_mortality_ratio_version
@cli_tools.add_preprocess_only
@cli_tools.add_output_options(paths.SEIR_FINAL_OUTPUTS)
@cli_tools.add_verbose_and_with_debugger
def postprocess(run_metadata,
                postprocessing_specification,
                forecast_version,
                mortality_ratio_version,
                preprocess_only,
                output_root, mark_best, production_tag,
                verbose, with_debugger):
    cli_tools.configure_logging_to_terminal(verbose)

    _do_postprocess(
        run_metadata=run_metadata,
        postprocessing_specification=postprocessing_specification,
        forecast_version=forecast_version,
        mortality_ratio_version=mortality_ratio_version,
        preprocess_only=preprocess_only,
        output_root=output_root,
        mark_best=mark_best,
        production_tag=production_tag,
        with_debugger=with_debugger,
    )

    logger.info('**Done**')


@seiir.command()
@cli_tools.pass_run_metadata()
@cli_tools.with_diagnostics_specification
@cli_tools.add_preprocess_only
@cli_tools.add_output_options(paths.SEIR_DIAGNOSTICS_OUTPUTS)
@cli_tools.add_verbose_and_with_debugger
def diagnostics(run_metadata,
                diagnostics_specification,
                preprocess_only,
                output_root, mark_best, production_tag,
                verbose, with_debugger):
    cli_tools.configure_logging_to_terminal(verbose)

    _do_diagnostics(
        run_metadata=run_metadata,
        diagnostics_specification=diagnostics_specification,
        preprocess_only=preprocess_only,
        output_root=output_root,
        mark_best=mark_best,
        production_tag=production_tag,
        with_debugger=with_debugger,
    )

    logger.info('**Done**')


@seiir.command(name='run_all')
@cli_tools.pass_run_metadata()
@cli_tools.with_regression_specification
@cli_tools.with_forecast_specification
@cli_tools.with_postprocessing_specification
@cli_tools.with_diagnostics_specification
@cli_tools.with_mark_best
@cli_tools.with_production_tag
@cli_tools.add_verbose_and_with_debugger
def run_all(run_metadata,
            regression_specification, forecast_specification,
            postprocessing_specification, diagnostics_specification,
            mark_best, production_tag,
            verbose, with_debugger):
    """Run all stages of the SEIIR pipeline.

    This application is expected to be run in production only and provides
    no user-level interface for specifying output paths. The `data` block
    of the regression specification can specify a specific path for infection
    data and covariate data, but all output paths and downstream input
    paths will be inferred automatically.
    """
    base_metadata = run_metadata.to_dict()
    del base_metadata['start_time']
    #####################
    # Do the regression #
    #####################
    # Build our own run metadata since the injected version is shared across the
    # three pipeline stages.
    regression_run_metadata = cli_tools.RunMetadata()
    regression_run_metadata.update(base_metadata)

    cli_tools.configure_logging_to_terminal(verbose)

    regression_spec = _do_regression(
        run_metadata=regression_run_metadata,
        regression_specification=regression_specification,
        infection_version=None,
        covariates_version=None,
        priors_version=None,
        coefficient_version=None,
        location_specification=None,
        preprocess_only=False,
        output_root=paths.SEIR_REGRESSION_OUTPUTS,
        mark_best=mark_best,
        production_tag=production_tag,
        with_debugger=with_debugger,
    )

    logger.info('Regression finished. Starting forecast.')

    ###################
    # Do the forecast #
    ###################

    forecast_run_metadata = cli_tools.RunMetadata()
    forecast_run_metadata.update(base_metadata)

    # Get rid of last stage file handlers.
    logger.remove(2)
    logger.remove(3)

    forecast_spec = _do_forecast(
        run_metadata=forecast_run_metadata,
        forecast_specification=forecast_specification,
        regression_version=regression_spec.data.output_root,
        covariates_version=regression_spec.data.covariate_version,
        preprocess_only=False,
        output_root=paths.SEIR_FORECAST_OUTPUTS,
        mark_best=mark_best,
        production_tag=production_tag,
        with_debugger=with_debugger,
    )

    logger.info('Forecast finished. Starting postprocessing.')

    #########################
    # Do the postprocessing #
    #########################

    postprocessing_run_metadata = cli_tools.RunMetadata()
    postprocessing_run_metadata.update(base_metadata)

    # Get rid of last stage file handlers so we get clean postprocessing logs.
    logger.remove(4)
    logger.remove(5)

    postprocessing_spec = _do_postprocess(
        run_metadata=postprocessing_run_metadata,
        postprocessing_specification=postprocessing_specification,
        forecast_version=forecast_spec.data.output_root,
        mortality_ratio_version=None,
        preprocess_only=False,
        output_root=paths.SEIR_FINAL_OUTPUTS,
        mark_best=mark_best,
        production_tag=production_tag,
        with_debugger=with_debugger,
    )

    logger.info('Postprocessing finished. Starting diagnostics.')

    ######################
    # Do the diagnostics #
    ######################

    diagnostics_run_metadata = cli_tools.RunMetadata()
    diagnostics_run_metadata.update(base_metadata)

    # Get rid of last stage file handlers so we get clean diagnostics logs.
    logger.remove(6)
    logger.remove(7)

    diagnostics_spec = _do_diagnostics(
        run_metadata=diagnostics_run_metadata,
        diagnostics_specification=diagnostics_specification,
        preprocess_only=False,
        output_root=paths.SEIR_DIAGNOSTICS_OUTPUTS,
        mark_best=mark_best,
        production_tag=production_tag,
        with_debugger=with_debugger
    )

    logger.info('All stages complete!')


def _do_oos_fit(run_metadata: cli_tools.RunMetadata,
                fit_specification: str,
                infection_version: Optional[str],
                covariates_version: Optional[str],
                coefficient_version: Optional[str],
                variant_version: Optional[str],
                location_specification: Optional[str],
                preprocess_only: bool,
                output_root: Optional[str], mark_best: bool, production_tag: str,
                with_debugger: bool) -> RegressionSpecification:
    fit_spec = FitSpecification.from_path(fit_specification)

    input_versions = {
        'infection_version': cli_tools.VersionInfo(
            infection_version,
            fit_spec.data.infection_version,
            paths.PAST_INFECTIONS_ROOT,
            'infections_metadata',
            True,
        ),
        'covariate_version': cli_tools.VersionInfo(
            covariates_version,
            fit_spec.data.covariate_version,
            paths.SEIR_COVARIATES_OUTPUT_ROOT,
            'covariates_metadata',
            True,
        ),
        'coefficient_version': cli_tools.VersionInfo(
            coefficient_version,
            fit_spec.data.coefficient_version,
            paths.SEIR_REGRESSION_OUTPUTS,
            'coefficient_metadata',
            False,
        ),
        'variant_version': cli_tools.VersionInfo(
            variant_version,
            fit_spec.data.variant_version,
            paths.VARIANT_OUTPUT_ROOT,
            'variant_metadata',
            True,
        ),
    }
    fit_spec, run_metadata = cli_tools.resolve_version_info(fit_spec, run_metadata, input_versions)

    locations_set_version_id, location_set_file = cli_tools.get_location_info(
        location_specification,
        fit_spec.data.location_set_version_id,
        fit_spec.data.location_set_file
    )

    output_root = cli_tools.get_output_root(None, fit_spec.data.output_root)
    cli_tools.setup_directory_structure(output_root, with_production=True)
    run_directory = cli_tools.make_run_directory(output_root)

    fit_spec.data.location_set_version_id = locations_set_version_id
    fit_spec.data.location_set_file = location_set_file
    fit_spec.data.output_root = str(run_directory)

    run_metadata['output_path'] = str(run_directory)
    run_metadata['regression_specification'] = fit_spec.to_dict()

    cli_tools.configure_logging_to_files(run_directory)
    # noinspection PyTypeChecker
    main = cli_tools.monitor_application(do_parameter_fit,
                                         logger, with_debugger)
    app_metadata, _ = main(fit_spec, preprocess_only)

    cli_tools.finish_application(run_metadata, app_metadata,
                                 run_directory, mark_best, production_tag)

    return fit_spec


def _do_regression(run_metadata: cli_tools.RunMetadata,
                   regression_specification: str,
                   infection_version: Optional[str],
                   covariates_version: Optional[str],
                   priors_version: Optional[str],
                   coefficient_version: Optional[str],
                   location_specification: Optional[str],
                   preprocess_only: bool,
                   output_root: Optional[str], mark_best: bool, production_tag: str,
                   with_debugger: bool) -> RegressionSpecification:
    regression_spec = RegressionSpecification.from_path(regression_specification)

    input_versions = {
        'infection_version': cli_tools.VersionInfo(
            infection_version,
            regression_spec.data.infection_version,
            paths.PAST_INFECTIONS_ROOT,
            'infections_metadata',
            True,
        ),
        'covariate_version': cli_tools.VersionInfo(
            covariates_version,
            regression_spec.data.covariate_version,
            paths.SEIR_COVARIATES_OUTPUT_ROOT,
            'covariates_metadata',
            True,
        ),
        'priors_version': cli_tools.VersionInfo(
            priors_version,
            regression_spec.data.priors_version,
            paths.SEIR_COVARIATE_PRIORS_ROOT,
            'covariate_priors_metadata',
            False,
        ),
        'coefficient_version': cli_tools.VersionInfo(
            coefficient_version,
            regression_spec.data.coefficient_version,
            paths.SEIR_REGRESSION_OUTPUTS,
            'coefficient_metadata',
            False,
        ),
    }
    regression_spec, run_metadata = cli_tools.resolve_version_info(regression_spec, run_metadata, input_versions)

    locations_set_version_id, location_set_file = cli_tools.get_location_info(
        location_specification,
        regression_spec.data.location_set_version_id,
        regression_spec.data.location_set_file
    )

    output_root = cli_tools.get_output_root(output_root, regression_spec.data.output_root)
    cli_tools.setup_directory_structure(output_root, with_production=True)
    run_directory = cli_tools.make_run_directory(output_root)

    regression_spec.data.location_set_version_id = locations_set_version_id
    regression_spec.data.location_set_file = location_set_file
    regression_spec.data.output_root = str(run_directory)

    run_metadata['output_path'] = str(run_directory)
    run_metadata['regression_specification'] = regression_spec.to_dict()

    cli_tools.configure_logging_to_files(run_directory)
    # noinspection PyTypeChecker
    main = cli_tools.monitor_application(do_beta_regression,
                                         logger, with_debugger)
    app_metadata, _ = main(regression_spec, preprocess_only)

    cli_tools.finish_application(run_metadata, app_metadata,
                                 run_directory, mark_best, production_tag)

    return regression_spec


def _do_forecast(run_metadata: cli_tools.RunMetadata,
                 forecast_specification: str,
                 regression_version: Optional[str],
                 covariates_version: Optional[str],
                 preprocess_only: bool,
                 output_root: Optional[str], mark_best: bool, production_tag: str,
                 with_debugger: bool) -> ForecastSpecification:
    forecast_spec = ForecastSpecification.from_path(forecast_specification)

    input_versions = {
        'regression_version': cli_tools.VersionInfo(
            regression_version,
            forecast_spec.data.regression_version,
            paths.SEIR_REGRESSION_OUTPUTS,
            'regression_metadata',
            True,
        ),
        'covariate_version': cli_tools.VersionInfo(
            covariates_version,
            forecast_spec.data.covariate_version,
            paths.SEIR_COVARIATES_OUTPUT_ROOT,
            'covariates_metadata',
            True,
        ),
    }
    forecast_spec, run_metadata = cli_tools.resolve_version_info(forecast_spec, run_metadata, input_versions)

    output_root = cli_tools.get_output_root(output_root,
                                            forecast_spec.data.output_root)
    cli_tools.setup_directory_structure(output_root, with_production=True)
    run_directory = cli_tools.make_run_directory(output_root)
    forecast_spec.data.output_root = str(run_directory)

    run_metadata['output_path'] = str(run_directory)
    run_metadata['forecast_specification'] = forecast_spec.to_dict()

    cli_tools.configure_logging_to_files(run_directory)
    # noinspection PyTypeChecker
    main = cli_tools.monitor_application(do_beta_forecast,
                                         logger, with_debugger)
    app_metadata, _ = main(forecast_spec, preprocess_only)

    cli_tools.finish_application(run_metadata, app_metadata,
                                 run_directory, mark_best, production_tag)
    return forecast_spec


def _do_postprocess(run_metadata: cli_tools.RunMetadata,
                    postprocessing_specification: str,
                    forecast_version: Optional[str],
                    mortality_ratio_version: Optional[str],
                    preprocess_only: bool,
                    output_root: Optional[str], mark_best: bool, production_tag: str,
                    with_debugger: bool) -> PostprocessingSpecification:
    postprocessing_spec = PostprocessingSpecification.from_path(postprocessing_specification)

    input_versions = {
        'forecast_version': cli_tools.VersionInfo(
            forecast_version,
            postprocessing_spec.data.forecast_version,
            paths.SEIR_FORECAST_OUTPUTS,
            'forecast_metadata',
            True,
        ),
        'mortality_ratio_version': cli_tools.VersionInfo(
            mortality_ratio_version,
            postprocessing_spec.data.mortality_ratio_version,
            paths.MORTALITY_AGE_PATTERN_ROOT,
            'mortality_ratio_metadata',
            True,
        ),
    }

    postprocessing_spec, run_metadata = cli_tools.resolve_version_info(
        postprocessing_spec,
        run_metadata,
        input_versions,
    )

    output_root = cli_tools.get_output_root(output_root,
                                            postprocessing_spec.data.output_root)
    cli_tools.setup_directory_structure(output_root, with_production=True)
    run_directory = cli_tools.make_run_directory(output_root)
    postprocessing_spec.data.output_root = str(run_directory)

    run_metadata['output_path'] = str(run_directory)
    run_metadata['postprocessing_specification'] = postprocessing_spec.to_dict()

    cli_tools.configure_logging_to_files(run_directory)
    # noinspection PyTypeChecker
    main = cli_tools.monitor_application(do_postprocessing,
                                         logger, with_debugger)
    app_metadata, _ = main(postprocessing_spec, preprocess_only)

    cli_tools.finish_application(run_metadata, app_metadata,
                                 run_directory, mark_best, production_tag)
    return postprocessing_spec


def _do_diagnostics(run_metadata: cli_tools.RunMetadata,
                    diagnostics_specification: str,
                    preprocess_only: bool,
                    output_root: Optional[str], mark_best: bool, production_tag: str,
                    with_debugger: bool) -> DiagnosticsSpecification:
    diagnostics_spec = DiagnosticsSpecification.from_path(diagnostics_specification)

    outputs_versions = set()
    for grid_plot_spec in diagnostics_spec.grid_plots:
        for comparator in grid_plot_spec.comparators:
            comparator_version_path = cli_tools.get_input_root(None,
                                                               comparator.version,
                                                               paths.SEIR_FINAL_OUTPUTS)
            outputs_versions.add(comparator_version_path)
            comparator.version = str(comparator_version_path)

    if diagnostics_spec.cumulative_deaths_compare_csv:
        for comparator in diagnostics_spec.cumulative_deaths_compare_csv.comparators:
            comparator_version_path = cli_tools.get_input_root(None,
                                                               comparator.version,
                                                               paths.SEIR_FINAL_OUTPUTS)
            outputs_versions.add(comparator_version_path)
            comparator.version = str(comparator_version_path)
    for scatters_spec in diagnostics_spec.scatters:
        x_axis_version_path = cli_tools.get_input_root(None,
                                                       scatters_spec.x_axis.version,
                                                       paths.SEIR_FINAL_OUTPUTS)
        scatters_spec.x_axis.version = str(x_axis_version_path)
        outputs_versions.add(x_axis_version_path)
        y_axis_version_path = cli_tools.get_input_root(None,
                                                       scatters_spec.y_axis.version,
                                                       paths.SEIR_FINAL_OUTPUTS)
        scatters_spec.y_axis.version = str(y_axis_version_path)
        outputs_versions.add(y_axis_version_path)

    output_root = cli_tools.get_output_root(output_root,
                                            diagnostics_spec.data.output_root)
    cli_tools.setup_directory_structure(output_root, with_production=True)
    run_directory = cli_tools.make_run_directory(output_root)

    diagnostics_spec.data.output_root = str(run_directory)

    outputs_metadata = []
    for output_version in outputs_versions:
        with (output_version / paths.METADATA_FILE_NAME).open() as metadata_file:
            outputs_metadata.append(yaml.full_load(metadata_file))

    run_metadata['seir_outputs_metadata'] = outputs_metadata
    run_metadata['output_path'] = str(run_directory)
    run_metadata['diagnostics_specification'] = diagnostics_spec.to_dict()

    cli_tools.configure_logging_to_files(run_directory)
    # noinspection PyTypeChecker
    main = cli_tools.monitor_application(do_diagnostics,
                                         logger, with_debugger)
    app_metadata, _ = main(diagnostics_spec, preprocess_only)

    cli_tools.finish_application(run_metadata, app_metadata,
                                 run_directory, mark_best, production_tag)
    return diagnostics_spec

from pathlib import Path

import click
from covid_shared import paths, cli_tools
from loguru import logger

import warnings
warnings.simplefilter('ignore')

from covid_historical_model import controller


@click.command()
@cli_tools.pass_run_metadata()
@click.option('-m', '--model-inputs-version',
              type=click.Path(file_okay=False),
              default=paths.BEST_LINK,
              help=('Which version of the inputs data to gather and format. '
                    'May be a full path or relative to the standard inputs root.'))
@click.option('-c', '--vaccine-coverage-version',
              type=click.Path(file_okay=False),
              default=paths.BEST_LINK,
              help=('Which version of vaccine coverage estimates to use. '
                    'May be a full path or relative to the standard vaccine coverage root.'))
@click.option('-s', '--variant-scaleup-version',
              type=click.Path(file_okay=False),
              default=paths.BEST_LINK,
              help=('Which version of the variant scaleup estimates to use. '
                    'May be a full path or relative to the standard variant scaleup root.'))
@click.option('-a', '--age-rates-version',
              type=click.Path(file_okay=False),
              default=paths.BEST_LINK,
              help=('Which version of the age-specific rates estimates to use. '
                    'May be a full path or relative to the standard age-specific rates root.'))
@click.option('-t', '--testing-version',
              type=click.Path(file_okay=False),
              default=paths.BEST_LINK,
              help=('Which version of the testing estimates to use. '
                    'May be a full path or relative to the standard testing root.'))
@click.option('-u', '--use-unscaled',
              is_flag=True,
              help=('Whether to use unscaled (i.e., no excess-mortality correction) data.'))
@click.option('-o', '--output-root',
              type=click.Path(file_okay=False),
              default=paths.HISTORICAL_MODEL_ROOT,
              show_default=True,
              help=('Directory containing versioned results structure (will create structure '
                    'if not already present).'))
@click.option('-n', '--n-samples',
              type=click.INT,
              default=100,
              help='Number of seroprevalence samples.')
@click.option('--gbd',
              is_flag=True,
              help='Whether to run GBD hierarchy or not.')
@click.option('-b', '--mark-best', 'mark_dir_as_best',
              is_flag=True,
              help='Marks the new outputs as best in addition to marking them as latest.')
@click.option('-p', '--production-tag',
              type=click.STRING,
              help='Tags this run as a production run.')
@cli_tools.add_verbose_and_with_debugger
def rates_pipeline(run_metadata,
                   model_inputs_version,
                   vaccine_coverage_version, variant_scaleup_version,
                   age_rates_version,
                   testing_version,
                   use_unscaled,
                   output_root,
                   n_samples,
                   gbd,
                   mark_dir_as_best, production_tag,
                   verbose, with_debugger):
    """Run rates pipeline."""
    cli_tools.configure_logging_to_terminal(verbose)
    model_inputs_root = cli_tools.get_last_stage_directory(model_inputs_version,
                                                           last_stage_root=paths.MODEL_INPUTS_ROOT)
    vaccine_coverage_root = cli_tools.get_last_stage_directory(vaccine_coverage_version,
                                                               last_stage_root=paths.VACCINE_COVERAGE_OUTPUT_ROOT)
    variant_scaleup_root = cli_tools.get_last_stage_directory(variant_scaleup_version,
                                                              last_stage_root=paths.VARIANT_OUTPUT_ROOT)
    age_rates_root = cli_tools.get_last_stage_directory(age_rates_version,
                                                        last_stage_root=paths.AGE_SPECIFIC_RATES_ROOT)
    testing_root = cli_tools.get_last_stage_directory(testing_version,
                                                      last_stage_root=paths.TESTING_OUTPUT_ROOT)
    run_metadata.update_from_path('model_inputs_metadata', model_inputs_root / paths.METADATA_FILE_NAME)
    run_metadata.update_from_path('vaccines_metadata', vaccine_coverage_root / paths.METADATA_FILE_NAME)
    run_metadata.update_from_path('variants_metadata', variant_scaleup_root / paths.METADATA_FILE_NAME)
    # run_metadata.update_from_path('age_rates_metadata', age_rates_root / paths.METADATA_FILE_NAME)
    run_metadata.update_from_path('testing_metadata', testing_root / paths.METADATA_FILE_NAME)

    output_root = Path(output_root).resolve()
    cli_tools.setup_directory_structure(output_root, with_production=True)
    run_directory = cli_tools.make_run_directory(output_root)
    run_metadata['output_path'] = str(run_directory)
    cli_tools.configure_logging_to_files(run_directory)

    main = cli_tools.monitor_application(controller.main, logger, with_debugger)
    app_metadata, _ = main(out_dir=run_directory,
                           model_inputs_root=model_inputs_root,
                           vaccine_coverage_root=vaccine_coverage_root,
                           variant_scaleup_root=variant_scaleup_root,
                           age_rates_root=age_rates_root,
                           testing_root=testing_root,
                           n_samples=n_samples,
                           excess_mortality=not use_unscaled,
                           gbd=gbd,)

    cli_tools.finish_application(run_metadata, app_metadata, run_directory,
                                 mark_dir_as_best, production_tag)

    logger.info('**Done**')

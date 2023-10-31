import click

###########################
# Specification arguments #
###########################

with_fit_specification = click.argument(
    'fit_specification',
    type=click.Path(exists=True, dir_okay=False),
)
with_regression_specification = click.argument(
    'regression_specification',
    type=click.Path(exists=True, dir_okay=False),
)
with_forecast_specification = click.argument(
    'forecast_specification',
    type=click.Path(exists=True, dir_okay=False),
)
with_postprocessing_specification = click.argument(
    'postprocessing_specification',
    type=click.Path(exists=True, dir_okay=False),
)
with_diagnostics_specification = click.argument(
    'diagnostics_specification',
    type=click.Path(exists=True, dir_okay=False)
)
with_predictive_validity_specification = click.argument(
    'predictive_validity_specification',
    type=click.Path(exists=True, dir_okay=False),
)


###########################
# Main input data options #
###########################

with_infection_version = click.option(
    '--infection-version',
    type=click.Path(exists=True, file_okay=False),
    help='Which version of infection inputs to use.',
)
with_covariates_version = click.option(
    '--covariates-version',
    type=click.Path(exists=True, file_okay=False),
    help='Which version of covariates to use.',
)
with_variant_version = click.option(
    '--variant-version',
    type=click.Path(exists=True, file_okay=False),
    help='Which version of variants to use.',
)
with_mortality_ratio_version = click.option(
    '--mortality-ratio-version',
    type=click.Path(exists=True, file_okay=False),
    help='Which version of the mortality age pattern to use.',
)
with_priors_version = click.option(
    '--priors-version',
    type=click.Path(exists=True, file_okay=False),
    help='Data based priors for the regression.',
)
with_coefficient_version = click.option(
    '--coefficient-version',
    type=click.Path(exists=True, file_okay=False),
    help='A prior regression version for pinning the regression '
         'coefficients. If provided, all fixed effects from the '
         'provided version will be used and only random effects will '
         'be fit.',
)
with_location_specification = click.option(
    '-l', '--location-specification',
    type=click.STRING,
    help='Either a location set version id used to pull a list of '
         'locations to run, or a full path to a file describing '
         'the location set.',
)
with_regression_version = click.option(
    '--regression-version',
    type=click.Path(exists=True, file_okay=False),
    help='Which version of beta regression to use.',
)
with_forecast_version = click.option(
    '--forecast-version',
    type=click.Path(exists=True, file_okay=False),
    help='Which version of forecasts to use.'
)

######################
# Other main options #
######################

add_preprocess_only = click.option(
    '--preprocess-only',
    is_flag=True,
    help='Only make the directory and set up the metadata. '
         'Useful for setting up output directories for testing '
         'tasks individually.',
)

########################
# Task version options #
########################

with_task_fit_version = click.option(
    '--fit-version', '-i',
    type=click.Path(exists=True, file_okay=False),
    required=True,
    help='Full path to an existing directory containing a '
         '"fit_specification.yaml".',
)
with_task_regression_version = click.option(
    '--regression-version', '-i',
    type=click.Path(exists=True, file_okay=False),
    required=True,
    help='Full path to an existing directory containing a '
         '"regression_specification.yaml".',
)
with_task_forecast_version = click.option(
    '--forecast-version', '-i',
    type=click.Path(exists=True, file_okay=False),
    required=True,
    help='Full path to an existing directory containing a '
         '"forecast_specification.yaml".',
)
with_task_postprocessing_version = click.option(
    '--postprocessing-version', '-i',
    type=click.Path(exists=True, file_okay=False),
    required=True,
    help='Full path to an existing directory containing a '
         '"postprocessing_specification.yaml".',
)
with_task_diagnostics_version = click.option(
    '--diagnostics-version', '-i',
    type=click.Path(exists=True, file_okay=False),
    required=True,
    help='Full path to an existing directory containing a '
         '"diagnostics_specification.yaml".',
)

#########################
# Task argument options #
#########################

with_scenario = click.option(
    '--scenario', '-s',
    type=click.STRING,
    required=True,
    help='The scenario to be run.',
)
with_measure = click.option(
    '--measure', '-m',
    type=click.STRING,
    required=True,
    help='The measure to be run.',
)
with_draw_id = click.option(
    '--draw-id', '-d',
    type=click.INT,
    required=True,
    help='The draw to be run.',
)
with_name = click.option(
    '--name', '-n',
    type=click.STRING,
    required=True,
    help='The name to be run.',
)
with_progress_bar = click.option(
    '--pb', 'progress_bar',
    is_flag=True,
    help='Whether to show progress bars.',
)

import click

from covid_model_seiir_pipeline.pipeline.parameter_fit import (
    FIT_JOBS,
    parameter_fit,
)
from covid_model_seiir_pipeline.pipeline.regression import (
    REGRESSION_JOBS,
    beta_regression,
    hospital_correction_factors,
)
from covid_model_seiir_pipeline.pipeline.forecasting import (
    FORECAST_JOBS,
    beta_residual_scaling,
    beta_forecast,
)
from covid_model_seiir_pipeline.pipeline.postprocessing import (
    POSTPROCESSING_JOBS,
    resample_map,
    postprocess,
)
from covid_model_seiir_pipeline.pipeline.diagnostics import (
    DIAGNOSTICS_JOBS,
    cumulative_deaths_compare_csv,
    grid_plots,
    scatters,
)


@click.group()
def stask():
    """Parent command for individual seiir pipeline tasks."""
    pass


stask.add_command(parameter_fit, name=FIT_JOBS.fit)
stask.add_command(beta_regression, name=REGRESSION_JOBS.regression)
stask.add_command(hospital_correction_factors, name=REGRESSION_JOBS.hospital_correction_factors)
stask.add_command(beta_residual_scaling, name=FORECAST_JOBS.scaling)
stask.add_command(beta_forecast, name=FORECAST_JOBS.forecast)
stask.add_command(resample_map, name=POSTPROCESSING_JOBS.resample)
stask.add_command(postprocess, name=POSTPROCESSING_JOBS.postprocess)
stask.add_command(cumulative_deaths_compare_csv, name=DIAGNOSTICS_JOBS.compare_csv)
stask.add_command(grid_plots, name=DIAGNOSTICS_JOBS.grid_plots)
stask.add_command(scatters, name=DIAGNOSTICS_JOBS.scatters)

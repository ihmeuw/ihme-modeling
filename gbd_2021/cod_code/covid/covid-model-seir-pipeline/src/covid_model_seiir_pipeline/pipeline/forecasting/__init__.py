from covid_model_seiir_pipeline.pipeline.forecasting.specification import (
    FORECAST_JOBS,
    ForecastSpecification,
)
from covid_model_seiir_pipeline.pipeline.forecasting.data import ForecastDataInterface
from covid_model_seiir_pipeline.pipeline.forecasting.task import (
    beta_residual_scaling,
    beta_forecast,
)
